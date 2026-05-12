"""
Chroma (filesystem-embedded) vector store implementation.

Persists data on local disk via `chromadb.PersistentClient`. Each `namespace`
maps to a Chroma `collection`.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field

from .base import (
    Chunk,
    DocumentRecord,
    VectorStoreIntegration,
    VectorStoreType,
)

logger = logging.getLogger(__name__)


class ChromaConfig(BaseModel):
    """Configuration for Chroma persistent client."""
    path: str = Field(description="Filesystem path for persistent storage")

    class Config:
        extra = "ignore"


class ChromaVectorStore(VectorStoreIntegration):
    """Chroma-backed vector store (filesystem-embedded)."""

    def __init__(self, config: dict[str, Any]):
        self._config = ChromaConfig.model_validate(config)
        self._client: Any = None

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.CHROMA

    async def connect(self) -> bool:
        try:
            import chromadb  # type: ignore
            base = Path(self._config.path).expanduser().resolve()
            base.mkdir(parents=True, exist_ok=True)
            self._client = await asyncio.to_thread(
                chromadb.PersistentClient, str(base)
            )
            return True
        except Exception as e:  # pragma: no cover
            logger.error(f"Chroma connect failed: {e}")
            return False

    async def disconnect(self) -> None:
        self._client = None

    def _collection(self, namespace: str):
        if self._client is None:
            raise RuntimeError("Chroma not connected")
        # cosine distance; chroma keeps it persistent in metadata.
        return self._client.get_or_create_collection(
            name=namespace or "default",
            metadata={"hnsw:space": "cosine"},
        )

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        def _do() -> dict[str, DocumentRecord]:
            col = self._collection(namespace)
            res = col.get(include=["metadatas"])
            metas = res.get("metadatas") or []
            agg: dict[str, DocumentRecord] = {}
            for m in metas:
                if not m:
                    continue
                doc_id = m.get("doc_id")
                if not doc_id:
                    continue
                rec = agg.get(doc_id)
                if rec is None:
                    rec = DocumentRecord(
                        doc_id=doc_id,
                        source_hash=m.get("source_hash"),
                        embedding_model=m.get("embedding_model"),
                        chunking_version=m.get("chunking_version"),
                        chunk_count=0,
                    )
                    agg[doc_id] = rec
                rec.chunk_count += 1
            return agg

        return await asyncio.to_thread(_do)

    async def upsert_document(
        self,
        namespace: str,
        doc_id: str,
        chunks: list[Chunk],
        source_hash: str,
        embedding_model: str,
        chunking_version: str,
    ) -> None:
        if not chunks:
            return

        def _do() -> None:
            col = self._collection(namespace)
            # Atomic-ish replace: delete all chunks of doc, then add new ones.
            try:
                col.delete(where={"doc_id": doc_id})
            except Exception as e:
                logger.debug(f"Chroma delete (pre-upsert) for {doc_id}: {e}")

            ids = [c.chunk_id for c in chunks]
            docs = [c.text for c in chunks]
            embs = [c.embedding for c in chunks if c.embedding is not None]
            if len(embs) != len(chunks):
                raise ValueError(
                    f"All chunks must have embeddings (doc={doc_id})"
                )
            metas: list[dict[str, Any]] = []
            for c in chunks:
                m = dict(c.metadata or {})
                m["doc_id"] = doc_id
                m["source_hash"] = source_hash
                m["embedding_model"] = embedding_model
                m["chunking_version"] = chunking_version
                metas.append(m)

            col.add(ids=ids, embeddings=embs, metadatas=metas, documents=docs)

        await asyncio.to_thread(_do)

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        def _do() -> bool:
            col = self._collection(namespace)
            existing = col.get(where={"doc_id": doc_id}, include=[])
            ids = existing.get("ids") or []
            if not ids:
                return False
            col.delete(where={"doc_id": doc_id})
            return True

        return await asyncio.to_thread(_do)

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        def _do() -> list[tuple[Chunk, float]]:
            col = self._collection(namespace)
            res = col.query(
                query_embeddings=[embedding],
                n_results=k,
                where=filter or None,
                include=["documents", "metadatas", "distances"],
            )
            ids = (res.get("ids") or [[]])[0]
            docs = (res.get("documents") or [[]])[0]
            metas = (res.get("metadatas") or [[]])[0]
            dists = (res.get("distances") or [[]])[0]
            out: list[tuple[Chunk, float]] = []
            for i, cid in enumerate(ids):
                m = metas[i] or {}
                out.append((
                    Chunk(
                        chunk_id=cid,
                        doc_id=m.get("doc_id", ""),
                        text=docs[i],
                        metadata=m,
                    ),
                    float(dists[i]),
                ))
            return out

        return await asyncio.to_thread(_do)
