"""add document_chunks pgvector table for RAG SQL context

Revision ID: f1a2b3c4d5e6
Revises: d5e6f7a8b9c0
Create Date: 2026-10-08 13:10:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, None] = "d5e6f7a8b9c0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "document_chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "graph_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("document_knowledge_graphs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("page_start", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("page_end", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_document_chunks_user_id", "document_chunks", ["user_id"])
    op.create_index("ix_document_chunks_graph_id", "document_chunks", ["graph_id"])
    op.create_index(
        "ix_document_chunks_user_graph",
        "document_chunks",
        ["user_id", "graph_id"],
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS document_chunks_embedding_hnsw
        ON document_chunks
        USING hnsw (embedding vector_cosine_ops)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS document_chunks_embedding_hnsw")
    op.drop_index("ix_document_chunks_user_graph", table_name="document_chunks")
    op.drop_index("ix_document_chunks_graph_id", table_name="document_chunks")
    op.drop_index("ix_document_chunks_user_id", table_name="document_chunks")
    op.drop_table("document_chunks")
