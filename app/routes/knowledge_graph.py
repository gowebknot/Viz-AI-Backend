"""
Knowledge graph routes: PDF/DOCX upload → sync graph build → fetch/list/delete.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, File, UploadFile, status
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.services.knowledge_graph.schemas import (
    DocumentRetrieveResponse,
    KnowledgeGraphDeleteResponse,
    KnowledgeGraphDetailResponse,
    KnowledgeGraphListResponse,
    KnowledgeGraphQueryRequest,
    KnowledgeGraphQueryResponse,
    KnowledgeGraphUploadResponse,
)
from app.services.knowledge_graph import service as kg_service
from app.utils.token_parser import get_current_user

knowledge_graph_router = APIRouter(
    prefix="/api/v1/knowledge-graphs",
    tags=["knowledge-graphs"],
)


@knowledge_graph_router.post(
    "/upload",
    response_model=KnowledgeGraphUploadResponse,
    status_code=status.HTTP_200_OK,
)
async def upload_knowledge_graph(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.upload_knowledge_graph(file, db, token_payload)


@knowledge_graph_router.post(
    "/query",
    response_model=KnowledgeGraphQueryResponse,
    status_code=status.HTTP_200_OK,
)
async def query_knowledge_graph(
    body: KnowledgeGraphQueryRequest,
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.query_knowledge_graph(body.question, token_payload)


@knowledge_graph_router.post(
    "/retrieve",
    response_model=DocumentRetrieveResponse,
    status_code=status.HTTP_200_OK,
)
async def retrieve_document_context(
    body: KnowledgeGraphQueryRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.retrieve_document_context(
        body.question, db, token_payload
    )


@knowledge_graph_router.get(
    "",
    response_model=KnowledgeGraphListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_knowledge_graphs(
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.list_knowledge_graphs(db, token_payload)


@knowledge_graph_router.get(
    "/{graph_id}",
    response_model=KnowledgeGraphDetailResponse,
    status_code=status.HTTP_200_OK,
)
async def get_knowledge_graph(
    graph_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.get_knowledge_graph(graph_id, db, token_payload)


@knowledge_graph_router.delete(
    "/{graph_id}",
    response_model=KnowledgeGraphDeleteResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_knowledge_graph(
    graph_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await kg_service.delete_knowledge_graph(graph_id, db, token_payload)
