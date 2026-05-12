"""
MongoDB Atlas Vector Search vector store.

Uses Atlas Vector Search (`$vectorSearch` aggregation stage). Each chunk is
stored as one document in the configured collection. A vector search index
of type `vectorSearch` is required on the `embedding` field with `namespace`
declared as a filter field.

Document shape
--------------
    {
        _id: chunk_id,
        namespace: str,
        doc_id: str,
        source_hash: str,
        embedding_model: str,
        chunking_version: str,
        text: str,
        metadata: dict,
        embedding: [float, ...]
    }

If `auto_create_index` is true, the integration attempts to create the
vector search index on connect via the `createSearchIndexes` admin command.
This requires Atlas (cloud or Atlas Local) — it will fail silently on a
plain MongoDB server.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from pydantic import BaseModel, Field

from .base import (
    Chunk,
    DocumentRecord,
    VectorStoreIntegration,
    VectorStoreType,
)

logger = logging.getLogger(__name__)


class MongoAtlasConfig(BaseModel):
    """Configuration for MongoDB Atlas Vector Search backend."""
    uri: str = Field(description="MongoDB connection URI (mongodb+srv://... or mongodb://...)")
    database: str = Field(description="Database name")
    collection: str = Field(default="ws_mark_flow_chunks", description="Collection name")
    embedding_dim: int = Field(default=1536, description="Embedding vector dimensionality")
    index_name: str = Field(default="vector_index", description="Atlas vector search index name")
    similarity: str = Field(
        default="cosine",
        description="Similarity metric for the vector index (cosine | euclidean | dotProduct)",
    )
    auto_create_index: bool = Field(
        default=True,
        description="Attempt to create the vector search index on connect (Atlas only)",
    )

    class Config:
        extra = "ignore"


class MongoAtlasVectorStore(VectorStoreIntegration):
    """MongoDB Atlas Vector Search-backed vector store."""

    def __init__(self, config: dict[str, Any]):
        self._config = MongoAtlasConfig.model_validate(config)
        self._client: Any = None
        self._coll: Any = None

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.MONGO_ATLAS

    async def connect(self) -> bool:
        try:
            from pymongo import AsyncMongoClient  # type: ignore
        except ImportError as e:
            logger.error(f"pymongo not installed: {e}")
            return False

        try:
            self._client = AsyncMongoClient(self._config.uri)
            db = self._client[self._config.database]
            self._coll = db[self._config.collection]
            await self._ensure_indexes()
            return True
        except Exception as e:
            logger.error(f"MongoAtlas connect failed: {e}")
            return False

    async def disconnect(self) -> None:
        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                pass
        self._client = None
        self._coll = None

    async def _ensure_indexes(self) -> None:
        # Regular index for list/delete-by-doc operations.
        try:
            await self._coll.create_index([("namespace", 1), ("doc_id", 1)])
        except Exception as e:
            logger.debug(f"MongoAtlas create_index(namespace, doc_id): {e}")

        if not self._config.auto_create_index:
            return

        # Vector search index (Atlas-only). Idempotent best-effort.
        index_def = {
            "name": self._config.index_name,
            "type": "vectorSearch",
            "definition": {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": self._config.embedding_dim,
                        "similarity": self._config.similarity,
                    },
                    {"type": "filter", "path": "namespace"},
                    {"type": "filter", "path": "doc_id"},
                ],
            },
        }
        try:
            await self._coll.create_search_index(index_def)
            logger.info(
                f"MongoAtlas vector search index '{self._config.index_name}' "
                f"requested (build may take a few seconds)"
            )
        except Exception as e:
            # Most common: already exists, or running on non-Atlas MongoDB.
            logger.debug(f"MongoAtlas create_search_index: {e}")

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        pipeline = [
            {"$match": {"namespace": namespace}},
            {
                "$group": {
                    "_id": "$doc_id",
                    "source_hash": {"$first": "$source_hash"},
                    "embedding_model": {"$first": "$embedding_model"},
                    "chunking_version": {"$first": "$chunking_version"},
                    "chunk_count": {"$sum": 1},
                }
            },
        ]
        out: dict[str, DocumentRecord] = {}
        cursor = await self._coll.aggregate(pipeline)
        async for row in cursor:
            doc_id = row.get("_id")
            if not doc_id:
                continue
            out[doc_id] = DocumentRecord(
                doc_id=doc_id,
                source_hash=row.get("source_hash"),
                embedding_model=row.get("embedding_model"),
                chunking_version=row.get("chunking_version"),
                chunk_count=int(row.get("chunk_count") or 0),
            )
        return out

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

        for c in chunks:
            if c.embedding is None:
                raise ValueError(f"Missing embedding on chunk {c.chunk_id}")

        # Atomic-ish per-doc replace: delete then insert in a single round-trip
        # of bulk operations.
        await self._coll.delete_many({"namespace": namespace, "doc_id": doc_id})

        docs = [
            {
                "_id": c.chunk_id,
                "namespace": namespace,
                "doc_id": doc_id,
                "source_hash": source_hash,
                "embedding_model": embedding_model,
                "chunking_version": chunking_version,
                "text": c.text,
                "metadata": dict(c.metadata or {}),
                "embedding": list(c.embedding),  # type: ignore[arg-type]
            }
            for c in chunks
        ]
        await self._coll.insert_many(docs, ordered=False)

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        res = await self._coll.delete_many({"namespace": namespace, "doc_id": doc_id})
        return (res.deleted_count or 0) > 0

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        # Build the optional $vectorSearch filter (only fields declared as
        # `filter` in the index definition can be used here — we declared
        # `namespace` and `doc_id`).
        vs_filter: dict[str, Any] = {"namespace": {"$eq": namespace}}
        # Extra metadata filters are applied as a post-stage $match since
        # they are not declared as Atlas filter fields.
        post_match: dict[str, Any] = {}
        if filter:
            for key, val in filter.items():
                if key in ("doc_id",):
                    vs_filter[key] = {"$eq": val}
                else:
                    post_match[f"metadata.{key}"] = val

        # numCandidates: Atlas guidance — at least 10*limit, clamp sensibly.
        num_candidates = max(50, k * 10)

        pipeline: list[dict[str, Any]] = [
            {
                "$vectorSearch": {
                    "index": self._config.index_name,
                    "path": "embedding",
                    "queryVector": list(embedding),
                    "numCandidates": num_candidates,
                    "limit": k,
                    "filter": vs_filter,
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "doc_id": 1,
                    "text": 1,
                    "metadata": 1,
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]
        if post_match:
            pipeline.insert(1, {"$match": post_match})

        out: list[tuple[Chunk, float]] = []
        cursor = await self._coll.aggregate(pipeline)
        async for row in cursor:
            score = float(row.get("score") or 0.0)
            # Convert similarity (higher=better) to distance (lower=better)
            # to match the convention used by Chroma / pgvector cosine.
            distance = 1.0 - score if self._config.similarity == "cosine" else -score
            out.append((
                Chunk(
                    chunk_id=row.get("_id"),
                    doc_id=row.get("doc_id", ""),
                    text=row.get("text", ""),
                    metadata=row.get("metadata") or {},
                ),
                distance,
            ))
        return out
