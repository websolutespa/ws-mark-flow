"""
Redis Stack vector store (RediSearch FT.* with HNSW vector index).

Storage layout
--------------
Each chunk is one Redis hash: `{prefix}:{namespace}:{chunk_id}` with fields:
    namespace, doc_id, source_hash, embedding_model, chunking_version,
    text, metadata (JSON string), embedding (binary float32 vector)

A single FT index covers all namespaces; `namespace` is indexed as a TAG so
queries filter by `@namespace:{<ns>}` before the KNN stage.

Requires Redis Stack (or Redis 8 with the RediSearch module) — plain Redis
does NOT support FT.* commands.
"""
from __future__ import annotations

import json
import logging
import struct
from typing import Any, Optional

from pydantic import BaseModel, Field

from .base import (
    Chunk,
    DocumentRecord,
    VectorStoreIntegration,
    VectorStoreType,
)

logger = logging.getLogger(__name__)


def _vec_to_bytes(vec: list[float]) -> bytes:
    """Pack a float list as little-endian float32 bytes (RediSearch convention)."""
    return struct.pack(f"<{len(vec)}f", *vec)


def _escape_tag(value: str) -> str:
    """Escape characters that are special in RediSearch TAG queries."""
    # RediSearch tag chars: , . < > { } [ ] " ' : ; ! @ # $ % ^ & * ( ) - + = ~ / \ space
    out = []
    for ch in value:
        if ch in ",.<>{}[]\"':;!@#$%^&*()-+=~/\\ ":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


class RedisConfig(BaseModel):
    """Configuration for Redis Stack vector store."""
    url: str = Field(
        description="Redis URL (redis://[:password@]host:port/db or rediss://...)",
    )
    index_name: str = Field(
        default="ws_mark_flow_chunks_idx",
        description="RediSearch index name",
    )
    key_prefix: str = Field(
        default="ws_mark_flow_chunk",
        description="Key prefix for chunk hashes",
    )
    embedding_dim: int = Field(default=1536, description="Vector dimensionality")
    similarity: str = Field(
        default="cosine",
        description="Distance metric: cosine | l2 | ip",
    )
    algorithm: str = Field(
        default="HNSW",
        description="Index algorithm: HNSW | FLAT",
    )

    class Config:
        extra = "ignore"


class RedisVectorStore(VectorStoreIntegration):
    """Redis Stack-backed vector store."""

    def __init__(self, config: dict[str, Any]):
        self._config = RedisConfig.model_validate(config)
        self._client: Any = None

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.REDIS

    # ---------- connection / schema ----------

    async def connect(self) -> bool:
        try:
            import redis.asyncio as redis  # type: ignore
        except ImportError as e:
            logger.error(f"redis-py not installed: {e}")
            return False

        try:
            self._client = redis.from_url(
                self._config.url, decode_responses=False,
            )
            await self._client.ping()
            await self._ensure_index()
            return True
        except Exception as e:
            logger.error(f"Redis connect failed: {e}")
            return False

    async def disconnect(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    def _key(self, namespace: str, chunk_id: str) -> str:
        return f"{self._config.key_prefix}:{namespace}:{chunk_id}"

    def _ns_prefix(self, namespace: str) -> str:
        return f"{self._config.key_prefix}:{namespace}:"

    async def _ensure_index(self) -> None:
        """Create the FT index if it does not exist (idempotent)."""
        idx = self._config.index_name
        try:
            await self._client.execute_command("FT.INFO", idx)
            return  # already exists
        except Exception:
            pass

        # Distance metric mapping: RediSearch uses upper-case identifiers.
        sim_map = {"cosine": "COSINE", "l2": "L2", "ip": "IP"}
        metric = sim_map.get(self._config.similarity.lower(), "COSINE")
        algo = self._config.algorithm.upper()
        if algo not in ("HNSW", "FLAT"):
            algo = "HNSW"

        # All chunks share one index across namespaces; the key prefix is
        # `{key_prefix}:` so the same index covers every namespace.
        args = [
            "FT.CREATE", idx,
            "ON", "HASH",
            "PREFIX", "1", f"{self._config.key_prefix}:",
            "SCHEMA",
            "namespace",        "TAG", "SEPARATOR", "|",
            "doc_id",           "TAG", "SEPARATOR", "|",
            "source_hash",      "TAG", "SEPARATOR", "|",
            "embedding_model",  "TAG", "SEPARATOR", "|",
            "chunking_version", "TAG", "SEPARATOR", "|",
            "text",             "TEXT",
            "embedding",        "VECTOR", algo, "6",
                                "TYPE", "FLOAT32",
                                "DIM", str(self._config.embedding_dim),
                                "DISTANCE_METRIC", metric,
        ]
        await self._client.execute_command(*args)

    # ---------- vector store ----------

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        # Aggregate over all chunks in the namespace, grouping by doc_id.
        idx = self._config.index_name
        ns_q = f"@namespace:{{{_escape_tag(namespace)}}}"
        # GROUPBY @doc_id REDUCE COUNT 0 AS chunk_count REDUCE FIRST_VALUE 1 @source_hash ...
        args = [
            "FT.AGGREGATE", idx, ns_q,
            "GROUPBY", "1", "@doc_id",
            "REDUCE", "COUNT", "0", "AS", "chunk_count",
            "REDUCE", "FIRST_VALUE", "1", "@source_hash",      "AS", "source_hash",
            "REDUCE", "FIRST_VALUE", "1", "@embedding_model",  "AS", "embedding_model",
            "REDUCE", "FIRST_VALUE", "1", "@chunking_version", "AS", "chunking_version",
            "LIMIT", "0", "10000",
        ]
        out: dict[str, DocumentRecord] = {}
        try:
            res = await self._client.execute_command(*args)
        except Exception as e:
            logger.error(f"Redis FT.AGGREGATE failed: {e}")
            return out

        # Response is [count, [k,v,k,v,...], [k,v,...], ...] — bytes by default.
        if not res or len(res) < 2:
            return out
        for row in res[1:]:
            d: dict[str, str] = {}
            for i in range(0, len(row), 2):
                k = row[i].decode() if isinstance(row[i], bytes) else str(row[i])
                v = row[i + 1]
                d[k] = v.decode() if isinstance(v, bytes) else str(v)
            doc_id = d.get("doc_id")
            if not doc_id:
                continue
            out[doc_id] = DocumentRecord(
                doc_id=doc_id,
                source_hash=d.get("source_hash") or None,
                embedding_model=d.get("embedding_model") or None,
                chunking_version=d.get("chunking_version") or None,
                chunk_count=int(d.get("chunk_count") or 0),
            )
        return out

    async def _scan_doc_keys(self, namespace: str, doc_id: str) -> list[bytes]:
        """Find all hash keys belonging to (namespace, doc_id)."""
        idx = self._config.index_name
        q = (
            f"@namespace:{{{_escape_tag(namespace)}}} "
            f"@doc_id:{{{_escape_tag(doc_id)}}}"
        )
        # FT.SEARCH ... NOCONTENT returns just the ids.
        args = [
            "FT.SEARCH", idx, q,
            "NOCONTENT",
            "LIMIT", "0", "10000",
        ]
        try:
            res = await self._client.execute_command(*args)
        except Exception as e:
            logger.error(f"Redis FT.SEARCH failed: {e}")
            return []
        if not res:
            return []
        # res = [count, key1, key2, ...]
        return [k for k in res[1:]]

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

        # Per-doc replace: delete prior chunks first.
        prior = await self._scan_doc_keys(namespace, doc_id)
        pipe = self._client.pipeline(transaction=False)
        if prior:
            pipe.delete(*prior)

        for c in chunks:
            key = self._key(namespace, c.chunk_id)
            mapping = {
                "namespace": namespace,
                "doc_id": doc_id,
                "source_hash": source_hash,
                "embedding_model": embedding_model,
                "chunking_version": chunking_version,
                "text": c.text or "",
                "metadata": json.dumps(c.metadata or {}),
                "embedding": _vec_to_bytes(list(c.embedding)),  # type: ignore[arg-type]
            }
            pipe.hset(key, mapping=mapping)
        await pipe.execute()

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        keys = await self._scan_doc_keys(namespace, doc_id)
        if not keys:
            return False
        await self._client.delete(*keys)
        return True

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        idx = self._config.index_name

        # Build pre-filter clause: namespace + first-class TAG fields.
        # Other metadata filters are applied client-side after fetching.
        pre = [f"@namespace:{{{_escape_tag(namespace)}}}"]
        post_filter: dict[str, Any] = {}
        if filter:
            for key, val in filter.items():
                if key in ("doc_id", "source_hash", "embedding_model", "chunking_version"):
                    pre.append(f"@{key}:{{{_escape_tag(str(val))}}}")
                else:
                    post_filter[key] = val

        pre_clause = " ".join(pre)
        over = max(k * 4, 20) if post_filter else k
        # Hybrid query: filter then KNN. RediSearch syntax:
        #   (@namespace:{ns})=>[KNN $K @embedding $vec AS score]
        knn = f"({pre_clause})=>[KNN {over} @embedding $vec AS score]"

        args = [
            "FT.SEARCH", idx, knn,
            "PARAMS", "2", "vec", _vec_to_bytes(list(embedding)),
            "RETURN", "5", "chunk_id_field_unused", "doc_id", "text", "metadata", "score",
            # RETURN list: we can't return the key, but we get the score + fields.
            "SORTBY", "score",
            "LIMIT", "0", str(over),
            "DIALECT", "2",
        ]
        # Fix RETURN to a sensible set (the placeholder above won't be a real field).
        args = [
            "FT.SEARCH", idx, knn,
            "PARAMS", "2", "vec", _vec_to_bytes(list(embedding)),
            "RETURN", "4", "doc_id", "text", "metadata", "score",
            "SORTBY", "score",
            "LIMIT", "0", str(over),
            "DIALECT", "2",
        ]

        try:
            res = await self._client.execute_command(*args)
        except Exception as e:
            logger.error(f"Redis KNN query failed: {e}")
            return []

        if not res or len(res) < 2:
            return []

        out: list[tuple[Chunk, float]] = []
        # res = [count, key1, [field, value, ...], key2, [...], ...]
        for i in range(1, len(res), 2):
            raw_key = res[i]
            fields = res[i + 1] if i + 1 < len(res) else []
            key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            # chunk_id = last segment of `{prefix}:{namespace}:{chunk_id}`
            chunk_id = key.split(":", 2)[-1].split(":", 1)[-1] \
                if key.count(":") >= 2 else key

            d: dict[str, Any] = {}
            for j in range(0, len(fields), 2):
                k_ = fields[j].decode() if isinstance(fields[j], bytes) else str(fields[j])
                v_ = fields[j + 1]
                d[k_] = v_.decode() if isinstance(v_, bytes) else v_

            meta_raw = d.get("metadata") or "{}"
            try:
                meta = json.loads(meta_raw) if isinstance(meta_raw, str) else {}
            except Exception:
                meta = {}

            if post_filter:
                skip = False
                for fk, fv in post_filter.items():
                    if str(meta.get(fk)) != str(fv):
                        skip = True
                        break
                if skip:
                    continue

            try:
                distance = float(d.get("score") or 0.0)
            except Exception:
                distance = 0.0

            out.append((
                Chunk(
                    chunk_id=chunk_id,
                    doc_id=str(d.get("doc_id") or ""),
                    text=str(d.get("text") or ""),
                    metadata=meta,
                ),
                distance,
            ))
            if len(out) >= k:
                break

        return out
