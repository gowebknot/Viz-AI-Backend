"""
Knowledge Graph Extraction Service

Extracts entities and relationships by delegating to the LLM service at
KG_EXTRACT_URL (default: http://localhost:8001/api/knowledge-graph/extract).

Text is extracted locally:
  PDF  → pypdf (page-by-page chunks)
  DOCX → built-in zipfile + xml.etree.ElementTree

The LLM service uses its own model/API key (gemini-2.5-flash by default),
keeping Gemini quota separated from the backend's key.

Returns a dict matching the shared graph payload shape:
  {
    "nodes": [{"id": str, "label": str, "type": str}],
    "edges": [{"id": str, "source": str, "target": str, "label": str}],
    "stats": {"node_count": int, "edge_count": int, "page_count": int}
  }
"""

from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from typing import Any, Dict, List

import asyncio

import httpx

logger = logging.getLogger(__name__)

_KG_EXTRACT_URL = os.getenv("KG_EXTRACT_URL", "http://localhost:8001/api/knowledge-graph/extract")
_KG_EXTRACT_TIMEOUT = float(os.getenv("KG_EXTRACT_TIMEOUT", "600"))
_CHUNK_PAGES = 5  # pages per LLM chunk


# ── Page-count helpers ────────────────────────────────────────────────────────

def _count_pdf_pages_heuristic(path: str) -> int:
    """Fast heuristic page count from raw PDF bytes (no external lib needed)."""
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        counts = [int(x) for x in re.findall(rb"/Count\s+(\d+)", raw)]
        return max(counts) if counts else 1
    except Exception:
        return 1


def _count_docx_pages(word_count: int) -> int:
    return max(1, round(word_count / 500))


# ── Text extractors ───────────────────────────────────────────────────────────

def _extract_pdf_chunks(file_path: str) -> tuple[List[Dict[str, Any]], int]:
    """
    Extract text from each PDF page using pypdf, group into chunks of
    _CHUNK_PAGES pages each, and return (chunks, total_pages).
    """
    from pypdf import PdfReader  # lazy import so the module loads without pypdf

    reader = PdfReader(file_path)
    total_pages = len(reader.pages)

    page_texts: List[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        page_texts.append(text)

    chunks: List[Dict[str, Any]] = []
    for start in range(0, total_pages, _CHUNK_PAGES):
        end = min(start + _CHUNK_PAGES, total_pages)
        chunk_text = "\n\n".join(page_texts[start:end]).strip()
        if chunk_text:
            chunks.append({
                "page_start": start + 1,
                "page_end": end,
                "text": chunk_text,
            })

    if not chunks:
        # Fallback: single empty chunk so the LLM service can return a proper error
        chunks.append({"page_start": 1, "page_end": total_pages or 1, "text": ""})

    return chunks, total_pages or _count_pdf_pages_heuristic(file_path)


def _extract_docx_chunks(file_path: str) -> tuple[List[Dict[str, Any]], int]:
    """Extract DOCX text with built-ins and return as a single chunk."""
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    paragraphs: List[str] = []
    with zipfile.ZipFile(file_path, "r") as z:
        with z.open("word/document.xml") as fh:
            root = ET.parse(fh).getroot()
    for para in root.iter(f"{ns}p"):
        text = "".join(t.text or "" for t in para.iter(f"{ns}t"))
        if text.strip():
            paragraphs.append(text.strip())
    full_text = "\n".join(paragraphs)
    word_count = len(full_text.split())
    page_count = _count_docx_pages(word_count)
    chunks = [{"page_start": 1, "page_end": page_count, "text": full_text}]
    return chunks, page_count


# ── LLM service call ──────────────────────────────────────────────────────────

async def _call_llm_service(
    filename: str,
    page_count: int,
    chunks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """POST text chunks to the LLM service and return the knowledge graph."""
    payload = {
        "filename": filename,
        "page_count": page_count,
        "chunks": chunks,
    }
    logger.info(
        "Calling LLM service for KG extraction | url=%s file=%s pages=%s chunks=%s",
        _KG_EXTRACT_URL,
        filename,
        page_count,
        len(chunks),
    )
    try:
        async with httpx.AsyncClient(timeout=_KG_EXTRACT_TIMEOUT) as client:
            resp = await client.post(_KG_EXTRACT_URL, json=payload)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = ""
        try:
            detail = exc.response.json().get("detail", "")
        except Exception:
            detail = exc.response.text[:200]
        raise RuntimeError(
            f"LLM service returned {exc.response.status_code}: {detail}"
        ) from exc
    except httpx.RequestError as exc:
        raise RuntimeError(
            f"Cannot reach LLM service at {_KG_EXTRACT_URL}: {exc}"
        ) from exc

    data = resp.json()
    logger.info(
        "KG extraction complete | nodes=%s edges=%s",
        data.get("stats", {}).get("node_count"),
        data.get("stats", {}).get("edge_count"),
    )
    return data


def extract_page_texts(file_path: str, mime_type: str) -> List[Dict[str, Any]]:
    """
    Per-page (or per-doc) text for RAG chunking.

    Do not reuse the 5-page KG LLM chunks — those discard raw sentences after
    entity extraction. RAG needs the actual page text.
    """
    if mime_type == "application/pdf":
        from pypdf import PdfReader

        reader = PdfReader(file_path)
        pages: List[Dict[str, Any]] = []
        for index, page in enumerate(reader.pages, start=1):
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            if text.strip():
                pages.append({"page_start": index, "page_end": index, "text": text})
        return pages
    if mime_type in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/msword",
    ):
        chunks, _page_count = _extract_docx_chunks(file_path)
        return [c for c in chunks if (c.get("text") or "").strip()]
    raise ValueError(f"Unsupported file type: {mime_type}")


# ── Public API ────────────────────────────────────────────────────────────────

async def extract_knowledge_graph(file_path: str, mime_type: str) -> Dict[str, Any]:
    """Extract entities/relationships from a file by delegating to the LLM service."""
    filename = os.path.basename(file_path)

    if mime_type == "application/pdf":
        chunks, page_count = await asyncio.to_thread(_extract_pdf_chunks, file_path)
    elif mime_type in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/msword",
    ):
        chunks, page_count = await asyncio.to_thread(_extract_docx_chunks, file_path)
    else:
        raise ValueError(f"Unsupported file type: {mime_type}")

    return await _call_llm_service(filename, page_count, chunks)
