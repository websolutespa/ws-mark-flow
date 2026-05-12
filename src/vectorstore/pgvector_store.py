"""
pgvector (PostgreSQL) remote vector store implementation.

Schema (auto-created on connect):

    CREATE TABLE IF NOT EXISTS {table} (
        chunk_id          TEXT PRIMARY KEY,
        namespace         TEXT NOT NULL,
        doc_id            TEXT NOT NULL,
        source_hash       TEXT,
        embedding_model   TEXT,
        chunking_version  TEXT,
        text              TEXT NOT NULL,
        metadata          JSONB DEFAULT '{}'::jsonb,
        embedding         vector({dim}) NOT NULL
    );

`namespace` ~= Chroma collection name.
"""
from __future__ import annotations

import asyncio
import json
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


class PgVectorConfig(BaseModel):
    """Configuration for pgvector backend."""
    dsn: str = Field(description="PostgreSQL connection string (postgresql://user:pass@host:port/db)")
    table: str = Field(default="ws_mark_flow_chunks", description="Table name")
    embedding_dim: int = Field(default=1536, description="Embedding vector dimensionality")

    class Config:
        extra = "ignore"


class PgVectorStore(VectorStoreIntegration):
    """pgvector-backed vector store (remote)."""

    def __init__(self, config: dict[str, Any]):
        self._config = PgVectorConfig.model_validate(config)
        self._pool: Any = None

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.PGVECTOR

    async def connect(self) -> bool:
        try:
            from psycopg_pool import AsyncConnectionPool  # type: ignore
        except ImportError as e:
            logger.error(f"psycopg[pool] not installed: {e}")
            return False

        try:
            self._pool = AsyncConnectionPool(self._config.dsn, open=False, min_size=1, max_size=4)
            await self._pool.open()
            await self._ensure_schema()
            return True
        except Exception as e:
            logger.error(f"pgvector connect failed: {e}")
            return False

    async def disconnect(self) -> None:
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception:
                pass
            self._pool = None

    async def _ensure_schema(self) -> None:
        table = self._config.table
        dim = self._config.embedding_dim
        ddl = f"""
        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE IF NOT EXISTS {table} (
            chunk_id          TEXT PRIMARY KEY,
            namespace         TEXT NOT NULL,
            doc_id            TEXT NOT NULL,
            source_hash       TEXT,
            embedding_model   TEXT,
            chunking_version  TEXT,
            text              TEXT NOT NULL,
            metadata          JSONB DEFAULT '{{}}'::jsonb,
            embedding         vector({dim}) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {table}_doc_idx ON {table}(namespace, doc_id);
        """
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(ddl)
            await conn.commit()

    @staticmethod
    def _vec_literal(vec: list[float]) -> str:
        return "[" + ",".join(f"{x:.8g}" for x in vec) + "]"

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        table = self._config.table
        sql = f"""
        SELECT doc_id,
               MAX(source_hash)      AS source_hash,
               MAX(embedding_model)  AS embedding_model,
               MAX(chunking_version) AS chunking_version,
               COUNT(*)              AS chunk_count
          FROM {table}
         WHERE namespace = %s
         GROUP BY doc_id
        """
        out: dict[str, DocumentRecord] = {}
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (namespace,))
                async for row in cur:
                    doc_id, sh, em, cv, cc = row
                    out[doc_id] = DocumentRecord(
                        doc_id=doc_id,
                        source_hash=sh,
                        embedding_model=em,
                        chunking_version=cv,
                        chunk_count=int(cc),
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

        table = self._config.table
        async with self._pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"DELETE FROM {table} WHERE namespace = %s AND doc_id = %s",
                        (namespace, doc_id),
                    )
                    insert_sql = (
                        f"INSERT INTO {table} (chunk_id, namespace, doc_id, source_hash, "
                        f"embedding_model, chunking_version, text, metadata, embedding) "
                        f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::vector)"
                    )
                    rows = [
                        (
                            c.chunk_id,
                            namespace,
                            doc_id,
                            source_hash,
                            embedding_model,
                            chunking_version,
                            c.text,
                            json.dumps(c.metadata or {}),
                            self._vec_literal(c.embedding),  # type: ignore[arg-type]
                        )
                        for c in chunks
                    ]
                    await cur.executemany(insert_sql, rows)

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        table = self._config.table
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {table} WHERE namespace = %s AND doc_id = %s",
                    (namespace, doc_id),
                )
                deleted = cur.rowcount or 0
            await conn.commit()
        return deleted > 0

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        table = self._config.table
        vec_lit = self._vec_literal(embedding)

        # Build WHERE clause and matching params (in order).
        where = "namespace = %s"
        where_params: list[Any] = [namespace]
        if filter:
            for key, val in filter.items():
                where += f" AND metadata->>{json.dumps(key)} = %s"
                where_params.append(str(val))

        sql = (
            f"SELECT chunk_id, doc_id, text, metadata, "
            f"embedding <=> %s::vector AS dist "
            f"FROM {table} WHERE {where} "
            f"ORDER BY embedding <=> %s::vector LIMIT %s"
        )
        params = [vec_lit, *where_params, vec_lit, k]

        out: list[tuple[Chunk, float]] = []
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                async for row in cur:
                    cid, did, text, meta, dist = row
                    out.append((
                        Chunk(
                            chunk_id=cid,
                            doc_id=did,
                            text=text,
                            metadata=meta if isinstance(meta, dict) else json.loads(meta or "{}"),
                        ),
                        float(dist),
                    ))
        return out
