"""
Knowledge graph business logic: upload, list, get, delete.

Uses extractor.py (google.genai + built-in zipfile) — no fitz/PyMuPDF or python-docx required.

File storage: uploads/knowledge_graphs/<user_id>/<graph_id>.<ext>
Ownership enforced: other users receive 404.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from uuid import UUID

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.models.knowledge_graph_models import DocumentKnowledgeGraphModel
from app.services.knowledge_graph.extractor import extract_knowledge_graph
from app.services.knowledge_graph import neo4j_client
from app.services.knowledge_graph import rag_store
from app.services.knowledge_graph.schemas import (
    DocumentRetrieveResponse,
    KnowledgeGraphDeleteResponse,
    KnowledgeGraphDetailResponse,
    KnowledgeGraphListItem,
    KnowledgeGraphListResponse,
    KnowledgeGraphQueryResponse,
    KnowledgeGraphQuerySubgraph,
    KnowledgeGraphUploadResponse,
)

logger = logging.getLogger(__name__)

_UPLOAD_BASE = os.getenv("KG_UPLOAD_DIR", "uploads/knowledge_graphs")
_MAX_FILE_MB = int(os.getenv("KG_MAX_FILE_MB", "20"))
_MAX_PAGES = int(os.getenv("KG_MAX_PAGES", "20"))

_MIME_TO_EXT = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
}


def _detect_mime(filename: str, content_type: str | None) -> str:
    ext = Path(filename).suffix.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if content_type in _MIME_TO_EXT:
        return content_type
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Only PDF and DOCX files are accepted.",
    )


def _user_id_str(token_payload: dict) -> str:
    sub = token_payload.get("sub")
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )
    return str(sub)


def _load_graph(row: DocumentKnowledgeGraphModel) -> dict:
    """Safely deserialise graph_json (stored as Text in DB)."""
    if not row.graph_json:
        return {"nodes": [], "edges": [], "stats": {}}
    if isinstance(row.graph_json, dict):
        return row.graph_json
    try:
        return json.loads(row.graph_json)
    except Exception:
        return {"nodes": [], "edges": [], "stats": {}}


# ── Upload ─────────────────────────────────────────────────────────────────────

async def upload_knowledge_graph(
    file: UploadFile,
    db: Session,
    token_payload: dict,
) -> KnowledgeGraphUploadResponse:
    user_id_str = _user_id_str(token_payload)
    filename = file.filename or "upload"
    mime = _detect_mime(filename, file.content_type)

    content = await file.read()
    size_mb = len(content) / (1024 * 1024)
    if size_mb > _MAX_FILE_MB:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File exceeds {_MAX_FILE_MB} MB limit ({size_mb:.1f} MB).",
        )

    # Save file first so we have a path
    upload_dir = Path(_UPLOAD_BASE) / user_id_str
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Temp filename (will rename after we have the graph_id from DB)
    import uuid as _uuid
    temp_id = str(_uuid.uuid4())
    ext = _MIME_TO_EXT[mime]
    file_path = str(upload_dir / f"{temp_id}{ext}")
    with open(file_path, "wb") as fh:
        fh.write(content)

    # Extract
    try:
        graph = await extract_knowledge_graph(file_path, mime)
    except Exception as exc:
        logger.exception("Extraction failed for %s", filename)
        try:
            os.remove(file_path)
        except OSError:
            pass
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Knowledge graph extraction failed: {exc}",
        )

    page_count = graph.get("stats", {}).get("page_count", 0)
    if page_count > _MAX_PAGES:
        try:
            os.remove(file_path)
        except OSError:
            pass
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Document exceeds {_MAX_PAGES}-page limit ({page_count} pages).",
        )

    # Persist to DB
    row = DocumentKnowledgeGraphModel(
        user_id=UUID(user_id_str),
        filename=filename,
        file_path=file_path,
        page_count=page_count,
        graph_json=json.dumps(graph),
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    # Rename file to use the actual graph_id
    new_path = str(upload_dir / f"{row.id}{ext}")
    try:
        os.rename(file_path, new_path)
        row.file_path = new_path
        db.commit()
    except OSError:
        pass  # Non-fatal — file still accessible at old path

    # Fail-closed: API succeeds only when Postgres and AuraDB both complete.
    try:
        await neo4j_client.write_document_graph(
            user_id=user_id_str,
            graph_id=str(row.id),
            graph=graph,
        )
    except Exception as exc:
        logger.exception(
            "AuraDB dual-write failed; rolling back Postgres | user_id=%s graph_id=%s",
            user_id_str,
            row.id,
        )
        stored_path = row.file_path
        db.delete(row)
        db.commit()
        if stored_path:
            try:
                os.remove(stored_path)
            except OSError:
                pass
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Knowledge graph storage failed (AuraDB): {exc}",
        )

    logger.info("KG created %s: %d nodes, %d edges", row.id,
                graph["stats"]["node_count"], graph["stats"]["edge_count"])

    # RAG index is additive: graph UI must still succeed if embed/pgvector fails.
    try:
        rag_store.index_document(
            db,
            user_id=UUID(user_id_str),
            graph_id=row.id,
            file_path=row.file_path,
            mime=mime,
        )
    except Exception:
        logger.exception(
            "RAG index failed; graph upload still succeeds | user_id=%s graph_id=%s",
            user_id_str,
            row.id,
        )

    return KnowledgeGraphUploadResponse(graph_id=row.id)


# ── List ───────────────────────────────────────────────────────────────────────

async def list_knowledge_graphs(
    db: Session,
    token_payload: dict,
) -> KnowledgeGraphListResponse:
    user_id = UUID(_user_id_str(token_payload))
    rows = (
        db.query(DocumentKnowledgeGraphModel)
        .filter(DocumentKnowledgeGraphModel.user_id == user_id)
        .order_by(DocumentKnowledgeGraphModel.created_at.desc())
        .all()
    )
    items = [
        KnowledgeGraphListItem(
            graph_id=r.id,
            filename=r.filename,
            page_count=r.page_count,
            created_at=r.created_at,
        )
        for r in rows
    ]
    return KnowledgeGraphListResponse(items=items)


# ── Get one ────────────────────────────────────────────────────────────────────

async def get_knowledge_graph(
    graph_id: UUID,
    db: Session,
    token_payload: dict,
) -> KnowledgeGraphDetailResponse:
    user_id = UUID(_user_id_str(token_payload))
    row = (
        db.query(DocumentKnowledgeGraphModel)
        .filter(
            DocumentKnowledgeGraphModel.id == graph_id,
            DocumentKnowledgeGraphModel.user_id == user_id,
        )
        .first()
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    return KnowledgeGraphDetailResponse(
        graph_id=row.id,
        filename=row.filename,
        page_count=row.page_count,
        created_at=row.created_at,
        graph=_load_graph(row),
    )


# ── Delete ─────────────────────────────────────────────────────────────────────

async def delete_knowledge_graph(
    graph_id: UUID,
    db: Session,
    token_payload: dict,
) -> KnowledgeGraphDeleteResponse:
    user_id_str = _user_id_str(token_payload)
    user_id = UUID(user_id_str)
    row = (
        db.query(DocumentKnowledgeGraphModel)
        .filter(
            DocumentKnowledgeGraphModel.id == graph_id,
            DocumentKnowledgeGraphModel.user_id == user_id,
        )
        .first()
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

    # Fail-closed: clear AuraDB first so a Neo4j outage leaves Postgres intact
    # and the client can retry delete.
    try:
        await neo4j_client.delete_document_graph(
            user_id=user_id_str,
            graph_id=str(graph_id),
        )
    except Exception as exc:
        logger.exception(
            "AuraDB cleanup failed; Postgres graph retained | user_id=%s graph_id=%s",
            user_id_str,
            graph_id,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Knowledge graph delete failed (AuraDB): {exc}",
        )

    if row.file_path:
        try:
            os.remove(row.file_path)
        except OSError:
            pass
    db.delete(row)
    db.commit()

    return KnowledgeGraphDeleteResponse(message="Knowledge graph deleted")


# ── Query (1-hop subgraph → context) ───────────────────────────────────────────

async def query_knowledge_graph(
    question: str,
    token_payload: dict,
) -> KnowledgeGraphQueryResponse:
    """Match the user's merged AuraDB graph and return nodes/edges context only."""
    user_id_str = _user_id_str(token_payload)
    cleaned = (question or "").strip()
    if not cleaned:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="question is required",
        )

    if not neo4j_client.is_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Neo4j AuraDB is not configured",
        )

    terms = neo4j_client.extract_question_terms(cleaned)
    try:
        subgraph = await neo4j_client.query_user_subgraph(
            user_id=user_id_str,
            terms=terms,
        )
    except Exception as exc:
        logger.exception(
            "AuraDB subgraph query failed | user_id=%s question=%s",
            user_id_str,
            cleaned[:200],
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Knowledge graph query failed (AuraDB): {exc}",
        )

    nodes = subgraph.get("nodes") or []
    edges = subgraph.get("edges") or []
    context = neo4j_client.subgraph_to_context(nodes, edges)
    return KnowledgeGraphQueryResponse(
        question=cleaned,
        context=context,
        subgraph=KnowledgeGraphQuerySubgraph(nodes=nodes, edges=edges),
    )


# ── Retrieve (pgvector RAG → SQL document context) ─────────────────────────────

async def retrieve_document_context(
    question: str,
    db: Session,
    token_payload: dict,
) -> DocumentRetrieveResponse:
    """Return top-k document chunks for SQL generation. Empty if none / embed fails."""
    user_id = UUID(_user_id_str(token_payload))
    result = rag_store.retrieve_for_question(
        db,
        user_id=user_id,
        question=question,
    )
    return DocumentRetrieveResponse(
        question=result.get("question") or (question or "").strip(),
        context=result.get("context") or "",
        chunks=result.get("chunks") or [],
    )
