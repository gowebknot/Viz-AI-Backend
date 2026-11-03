"""
This module migrates the database by creating the tables defined in the schema_models module.
"""

from sqlalchemy import text

from app.core.db import engine
from app.models.schema_models import Base
from app.models.knowledge_graph_models import (  # noqa: F401 — register on Base.metadata
    DocumentChunkModel,
    DocumentKnowledgeGraphModel,
)

with engine.begin() as conn:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

Base.metadata.create_all(engine)

with engine.begin() as conn:
    conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS document_chunks_embedding_hnsw
            ON document_chunks
            USING hnsw (embedding vector_cosine_ops)
            """
        )
    )

print("Tables created successfully!")
