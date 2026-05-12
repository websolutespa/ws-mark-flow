"""
Markdown-aware chunking utilities.

Strategies
----------
- ``fixed``             : char-count windows with overlap.
- ``recursive``         : split on paragraph boundaries first, then sentences,
                           then characters until each chunk fits.
- ``markdown_headers``  : split by `#`, `##`, `###` boundaries, then run the
                           ``recursive`` chunker on each section to enforce size.

All strategies accept a `chunk_size` (max characters) and `chunk_overlap`
(characters carried over between adjacent chunks).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable


@dataclass
class ChunkingParams:
    strategy: str = "markdown_headers"
    chunk_size: int = 1200
    chunk_overlap: int = 150


def chunking_version(p: ChunkingParams) -> str:
    """Stable identifier for the chunker config (used as cache key)."""
    raw = f"{p.strategy}:{p.chunk_size}:{p.chunk_overlap}"
    return f"{p.strategy}-{hashlib.md5(raw.encode()).hexdigest()[:8]}"


# ---------- low-level splitters ----------

def _fixed_windows(text: str, size: int, overlap: int) -> list[str]:
    if size <= 0:
        return [text]
    overlap = max(0, min(overlap, size - 1))
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        out.append(text[i:i + size])
        if i + size >= n:
            break
        i += size - overlap
    return out


_PARA_RE = re.compile(r"\n\s*\n")
_SENT_RE = re.compile(r"(?<=[.!?])\s+")


def _recursive(text: str, size: int, overlap: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]

    # Split on paragraphs, then merge greedily.
    paragraphs = [p for p in _PARA_RE.split(text) if p.strip()]
    if len(paragraphs) > 1:
        return _merge(paragraphs, size, overlap, recurse_oversize=True)

    sentences = _SENT_RE.split(text)
    if len(sentences) > 1:
        return _merge(sentences, size, overlap, recurse_oversize=True)

    return _fixed_windows(text, size, overlap)


def _merge(
    parts: Iterable[str],
    size: int,
    overlap: int,
    recurse_oversize: bool = False,
) -> list[str]:
    """Greedy merge of small parts into chunks <= `size`."""
    out: list[str] = []
    cur = ""
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if len(p) > size and recurse_oversize:
            if cur:
                out.append(cur)
                cur = ""
            out.extend(_recursive(p, size, overlap))
            continue
        if not cur:
            cur = p
        elif len(cur) + 2 + len(p) <= size:
            cur = f"{cur}\n\n{p}"
        else:
            out.append(cur)
            cur = p
    if cur:
        out.append(cur)

    if overlap > 0 and len(out) > 1:
        with_overlap: list[str] = [out[0]]
        for i in range(1, len(out)):
            tail = out[i - 1][-overlap:]
            with_overlap.append(f"{tail}\n\n{out[i]}" if tail else out[i])
        return with_overlap
    return out


_HEADER_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


def _markdown_sections(text: str) -> list[tuple[list[str], str]]:
    """Split markdown into (heading_path, section_body) pairs."""
    matches = list(_HEADER_RE.finditer(text))
    if not matches:
        return [([], text)]

    sections: list[tuple[list[str], str]] = []
    # Preamble before the first header
    pre = text[: matches[0].start()].strip()
    if pre:
        sections.append(([], pre))

    path: list[tuple[int, str]] = []  # (level, title)
    for i, m in enumerate(matches):
        level = len(m.group(1))
        title = m.group(2).strip()
        # pop deeper-or-equal entries
        while path and path[-1][0] >= level:
            path.pop()
        path.append((level, title))

        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()
        heading_path = [t for _, t in path]
        if body:
            sections.append((heading_path, body))

    return sections


# ---------- public API ----------

def chunk_markdown(
    text: str,
    params: ChunkingParams,
) -> list[tuple[str, dict]]:
    """
    Chunk a markdown string. Returns a list of `(chunk_text, metadata)`.

    `metadata` carries `headings` (list of breadcrumb strings) when applicable.
    """
    text = text or ""
    if params.strategy == "fixed":
        return [
            (t, {})
            for t in _fixed_windows(text, params.chunk_size, params.chunk_overlap)
        ]
    if params.strategy == "recursive":
        return [(t, {}) for t in _recursive(text, params.chunk_size, params.chunk_overlap)]
    if params.strategy == "markdown_headers":
        out: list[tuple[str, dict]] = []
        for headings, body in _markdown_sections(text):
            for piece in _recursive(body, params.chunk_size, params.chunk_overlap):
                meta = {"headings": " > ".join(headings)} if headings else {}
                out.append((piece, meta))
        return out

    raise ValueError(f"Unknown chunking strategy: {params.strategy}")
