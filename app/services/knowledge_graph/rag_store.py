"""Index and retrieve document chunks from pgvector for SQL generation."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from uuid import UUID

from sqlalchemy.orm import Session

from app.core.settings import settings
from app.models.knowledge_graph_models import DocumentChunkModel
from app.services.knowledge_graph.extractor import extract_page_texts
from app.services.knowledge_graph.rag_chunker import split_page_texts
from app.services.knowledge_graph.rag_embeddings import embed_query, embed_texts

logger = logging.getLogger(__name__)


def format_document_context(chunks: List[Dict[str, Any]]) -> str:
    """Serialize retrieved chunks for the NL2SQL product_description slot."""
    if not chunks:
        return ""
    parts = [
        "[DOCUMENT CONTEXT]",
        "Use these excerpts as hints only. Do not invent tables or columns.",
        "SQL may use ONLY tables and columns listed in Database Schema.",
        "The user question decides the chart: same grain and the same filters.",
        "A table named in an excerpt is a hint. Use it only when that name is in the schema and the question needs it.",
        "Do not build a separate chart just because an excerpt mentions another table.",
        "If an excerpt says an entity is stored in a named table, use that table only when the schema lists it.",
        "Do not drop a filter the question asked for.",
        "If the question's word is not in that column's allowed values, use an allowed value whose meaning matches,",
        "and apply any date rule in the excerpt using the real date columns.",
        "Never write the question word as a stored value unless it is in the allowed list.",
        "When an excerpt defines a metric as one amount divided by a size or area, divide using the size column on the same row as the amount.",
        "Do not join a child table only through a parent key, because that repeats each amount once per child row.",
        "When an excerpt limits a metric to records in a named state, keep that filter and use an allowed stored value whose meaning matches.",
        "If an excerpt defines a metric with an equals sign and names a schema table as the source of the amount, use that table. Do not replace it with another table an excerpt only mentions.",
        "When the question groups by an entity, select a text name column from that entity's table, not a numeric id.",
        "Keep the state filter and any date test the excerpt defines for a condition the question asked for.",
        "",
    ]
    for index, chunk in enumerate(chunks, start=1):
        page_start = chunk.get("page_start")
        page_end = chunk.get("page_end")
        if page_start and page_end and page_start != page_end:
            loc = f"pages {page_start}-{page_end}"
        else:
            loc = f"page {page_start or '?'}"
        parts.append(f"--- chunk {index} ({loc}) ---")
        parts.append((chunk.get("content") or "").strip())
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def index_document(
    db: Session,
    *,
    user_id: UUID,
    graph_id: UUID,
    file_path: str,
    mime: str,
) -> int:
    """Chunk + embed a document and insert rows. Returns number of chunks stored."""
    pages = extract_page_texts(file_path, mime)
    pieces = split_page_texts(pages)
    if not pieces:
        logger.warning(
            "RAG index skipped — no extractable text | graph_id=%s", graph_id
        )
        return 0

    vectors = embed_texts([p["content"] for p in pieces])
    if len(vectors) != len(pieces):
        raise RuntimeError(
            f"Embedding count {len(vectors)} != chunk count {len(pieces)}"
        )

    rows = [
        DocumentChunkModel(
            user_id=user_id,
            graph_id=graph_id,
            page_start=int(piece["page_start"]),
            page_end=int(piece["page_end"]),
            content=piece["content"],
            embedding=vector,
        )
        for piece, vector in zip(pieces, vectors)
    ]
    db.add_all(rows)
    db.commit()
    logger.info(
        "RAG indexed | graph_id=%s user_id=%s chunks=%d",
        graph_id,
        user_id,
        len(rows),
    )
    return len(rows)


def retrieve_for_question(
    db: Session,
    *,
    user_id: UUID,
    question: str,
    top_k: int | None = None,
) -> Dict[str, Any]:
    """
    Embed the question and return top-k chunks for that user.

    Never raises for empty indexes — returns context="" and chunks=[].
    """
    cleaned = (question or "").strip()
    empty = {"question": cleaned, "context": "", "chunks": []}
    if not cleaned:
        return empty

    limit = int(top_k if top_k is not None else settings.RAG_TOP_K or 5)
    limit = max(1, min(limit, 20))

    try:
        query_vec = embed_query(cleaned)
    except Exception:
        logger.exception("RAG retrieve: failed to embed question")
        return empty

    distance = DocumentChunkModel.embedding.cosine_distance(query_vec)
    rows = (
        db.query(
            DocumentChunkModel.page_start,
            DocumentChunkModel.page_end,
            DocumentChunkModel.content,
            distance.label("distance"),
        )
        .filter(DocumentChunkModel.user_id == user_id)
        .order_by(distance)
        .limit(limit)
        .all()
    )
    chunks = [
        {
            "page_start": row.page_start,
            "page_end": row.page_end,
            "content": row.content,
            "distance": float(row.distance) if row.distance is not None else None,
        }
        for row in rows
    ]
    context = format_document_context(chunks)
    logger.info(
        "RAG retrieve | user_id=%s hits=%d context_chars=%d | question=%s | document_context=\n%s",
        user_id,
        len(chunks),
        len(context),
        cleaned,
        context or "(empty — no matching chunks)",
    )
    return {"question": cleaned, "context": context, "chunks": chunks}
