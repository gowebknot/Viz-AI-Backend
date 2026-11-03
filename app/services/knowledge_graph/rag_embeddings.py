"""OpenAI embeddings for RAG chunks and NLQ retrieve."""

from __future__ import annotations

import logging
from typing import List, Sequence

from openai import OpenAI

from app.core.settings import settings
from app.models.knowledge_graph_models import VECTOR_DIMENSIONS

logger = logging.getLogger(__name__)

_BATCH = 64


def _client() -> OpenAI:
    api_key = settings.OPENAI_API_KEY or settings.OPENAPI_API_KEY
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set; cannot embed RAG chunks")
    return OpenAI(api_key=api_key)


def embed_texts(texts: Sequence[str]) -> List[List[float]]:
    """Embed texts with EMBEDDING_MODEL. Empty input → empty list."""
    cleaned = [t.strip() for t in texts if t and str(t).strip()]
    if not cleaned:
        return []

    model = (settings.EMBEDDING_MODEL or "text-embedding-3-small").strip()
    client = _client()
    vectors: List[List[float]] = []
    for i in range(0, len(cleaned), _BATCH):
        batch = cleaned[i : i + _BATCH]
        response = client.embeddings.create(model=model, input=batch)
        ordered = sorted(response.data, key=lambda item: item.index)
        for item in ordered:
            vec = list(item.embedding)
            if len(vec) != VECTOR_DIMENSIONS:
                raise RuntimeError(
                    f"Embedding dim {len(vec)} != VECTOR_DIMENSIONS {VECTOR_DIMENSIONS} "
                    f"(model={model})"
                )
            vectors.append(vec)
    logger.info("RAG embed | model=%s texts=%d", model, len(vectors))
    return vectors


def embed_query(question: str) -> List[float]:
    vectors = embed_texts([question])
    if not vectors:
        raise RuntimeError("Failed to embed empty question")
    return vectors[0]
