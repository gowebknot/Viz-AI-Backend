"""SQLAlchemy models for PDF document knowledge graphs and RAG chunks."""

from uuid import uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.base import Base

# Must match EMBEDDING_MODEL (text-embedding-3-small = 1536). Changing this
# requires a new column/table — Postgres cannot ALTER vector dimensions.
VECTOR_DIMENSIONS = 1536


class DocumentKnowledgeGraphModel(Base):
    """Stores a user-owned knowledge graph generated from an uploaded PDF."""

    __tablename__ = "document_knowledge_graphs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    page_count = Column(Integer, nullable=False, default=0)
    graph_json = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    user = relationship("UserModel", foreign_keys=[user_id])
    chunks = relationship(
        "DocumentChunkModel",
        back_populates="graph",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DocumentChunkModel(Base):
    """RAG chunk + embedding for SQL generation (pgvector). UI graph stays in Neo4j."""

    __tablename__ = "document_chunks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    graph_id = Column(
        UUID(as_uuid=True),
        ForeignKey("document_knowledge_graphs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    page_start = Column(Integer, nullable=False, default=1)
    page_end = Column(Integer, nullable=False, default=1)
    content = Column(Text, nullable=False)
    embedding = Column(Vector(VECTOR_DIMENSIONS), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    graph = relationship("DocumentKnowledgeGraphModel", back_populates="chunks")

    __table_args__ = (
        Index("ix_document_chunks_user_graph", "user_id", "graph_id"),
    )
