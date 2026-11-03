"""Split extracted PDF/DOCX page text into RAG chunks (~500–800 chars)."""

from __future__ import annotations

from typing import Any, Dict, List

_CHUNK_SIZE = 700
_CHUNK_OVERLAP = 80


def split_page_texts(
    pages: List[Dict[str, Any]],
    *,
    chunk_size: int = _CHUNK_SIZE,
    overlap: int = _CHUNK_OVERLAP,
) -> List[Dict[str, Any]]:
    """
    Turn per-page text into overlapping character windows.

    Each item: page_start, page_end, content.
    """
    if chunk_size < 100:
        chunk_size = 100
    if overlap < 0 or overlap >= chunk_size:
        overlap = max(0, chunk_size // 8)

    out: List[Dict[str, Any]] = []
    for page in pages or []:
        text = " ".join((page.get("text") or "").split())
        if not text:
            continue
        page_start = int(page.get("page_start") or 1)
        page_end = int(page.get("page_end") or page_start)
        if len(text) <= chunk_size:
            out.append(
                {"page_start": page_start, "page_end": page_end, "content": text}
            )
            continue
        start = 0
        while start < len(text):
            end = min(start + chunk_size, len(text))
            piece = text[start:end].strip()
            if piece:
                out.append(
                    {
                        "page_start": page_start,
                        "page_end": page_end,
                        "content": piece,
                    }
                )
            if end >= len(text):
                break
            start = end - overlap
    return out
