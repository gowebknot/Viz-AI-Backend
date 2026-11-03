"""
Pydantic schemas for PDF/DOCX knowledge graph APIs.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class KnowledgeGraphUploadResponse(BaseModel):
    graph_id: UUID


class KnowledgeGraphNode(BaseModel):
    id: str
    label: str
    type: str = "Other"


class KnowledgeGraphEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str


class KnowledgeGraphStats(BaseModel):
    node_count: int = 0
    edge_count: int = 0
    page_count: int = 0


class KnowledgeGraphPayload(BaseModel):
    nodes: List[KnowledgeGraphNode] = Field(default_factory=list)
    edges: List[KnowledgeGraphEdge] = Field(default_factory=list)
    stats: KnowledgeGraphStats = Field(default_factory=KnowledgeGraphStats)


class KnowledgeGraphDetailResponse(BaseModel):
    graph_id: UUID
    filename: str
    page_count: int
    created_at: Optional[datetime] = None
    graph: Dict[str, Any]


class KnowledgeGraphListItem(BaseModel):
    graph_id: UUID
    filename: str
    page_count: int
    created_at: Optional[datetime] = None


class KnowledgeGraphListResponse(BaseModel):
    items: List[KnowledgeGraphListItem] = Field(default_factory=list)


class KnowledgeGraphDeleteResponse(BaseModel):
    message: str


class KnowledgeGraphQueryRequest(BaseModel):
    question: str = Field(..., min_length=1)


class KnowledgeGraphQueryNode(BaseModel):
    label: str
    type: str = "Other"


class KnowledgeGraphQueryEdge(BaseModel):
    source: str
    label: str
    target: str


class KnowledgeGraphQuerySubgraph(BaseModel):
    nodes: List[KnowledgeGraphQueryNode] = Field(default_factory=list)
    edges: List[KnowledgeGraphQueryEdge] = Field(default_factory=list)


class KnowledgeGraphQueryResponse(BaseModel):
    question: str
    context: str
    subgraph: KnowledgeGraphQuerySubgraph


class DocumentRetrieveChunk(BaseModel):
    page_start: int = 1
    page_end: int = 1
    content: str
    distance: Optional[float] = None


class DocumentRetrieveResponse(BaseModel):
    question: str
    context: str
    chunks: List[DocumentRetrieveChunk] = Field(default_factory=list)
