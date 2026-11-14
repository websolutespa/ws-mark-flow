"""Tests for ``src/vectorstore/pgvector_store.py``.

The psycopg pool, connection and cursor are replaced by spec'd mocks; the
tests assert on the SQL statements and parameters sent to PostgreSQL.
"""
import json
import logging
import re
import sys
from unittest.mock import MagicMock

import psycopg_pool
import pytest
from psycopg import AsyncConnection, AsyncCursor
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from src.vectorstore.base import Chunk, DocumentRecord, VectorStoreType
from src.vectorstore.pgvector_store import PgVectorConfig, PgVectorStore

DSN = "postgresql://user:secret@localhost:5432/vectors"
NS = "tenant-a"


def _sql(text: str) -> str:
    return " ".join(text.split())


class PgHarness:
    """A mocked psycopg pool recording every statement in execution order."""

    def __init__(self):
        self.events: list = []

        self.cur = MagicMock(spec=AsyncCursor)
        self.cur.__aenter__.return_value = self.cur
        self.cur.rowcount = 0
        self.rows = []
        self.cur.execute.side_effect = lambda sql, params=None: self.events.append(
            ("execute", _sql(sql), params)
        )
        self.cur.executemany.side_effect = lambda sql, rows: self.events.append(
            ("executemany", _sql(sql), rows)
        )

        self.conn = MagicMock(spec=AsyncConnection)
        self.conn.cursor.return_value = self.cur
        self.conn.commit.side_effect = lambda: self.events.append("commit")
        self.tx = self.conn.transaction.return_value
        self.tx.__aenter__.side_effect = lambda *a: self.events.append("begin")
        self.tx.__aexit__.side_effect = lambda exc_type, *a: self.events.append(
            ("end", exc_type)
        )

        self.pool = MagicMock(spec=AsyncConnectionPool)
        self.pool.connection.return_value.__aenter__.return_value = self.conn
        self.factory = MagicMock(return_value=self.pool)

    @property
    def rows(self):
        return self.cur.__aiter__.return_value

    @rows.setter
    def rows(self, rows):
        """Rows yielded by ``async for row in cursor``."""
        self.cur.__aiter__.return_value = rows

    @property
    def statements(self):
        return [e for e in self.events if isinstance(e, tuple) and e[0].startswith("execute")]


@pytest.fixture
def pg(monkeypatch):
    harness = PgHarness()
    monkeypatch.setattr(psycopg_pool, "AsyncConnectionPool", harness.factory)
    return harness


@pytest.fixture
async def store(pg):
    pgstore = PgVectorStore({"dsn": DSN, "table": "chunks", "embedding_dim": 3})
    assert await pgstore.connect() is True
    pg.events.clear()
    pg.pool.connection.reset_mock()
    return pgstore


def _chunk(index, embedding=(0.5, -1.0, 2.0), doc="a.md", **metadata):
    return Chunk(
        chunk_id=f"{doc}#{index}",
        doc_id=doc,
        text=f"text {index}",
        metadata=metadata,
        embedding=list(embedding) if embedding is not None else None,
    )


async def _upsert(store, chunks, doc="a.md", namespace=NS):
    await store.upsert_document(
        namespace=namespace,
        doc_id=doc,
        chunks=chunks,
        source_hash="h1",
        embedding_model="test-embed",
        chunking_version="v1",
    )


# --- Configuration -------------------------------------------------------------

def test_config_defaults_and_store_type():
    pgstore = PgVectorStore({"dsn": DSN, "unknown": 1})
    assert pgstore.store_type is VectorStoreType.PGVECTOR
    assert PgVectorConfig.model_validate({"dsn": DSN}).model_dump() == {
        "dsn": DSN,
        "table": "ws_mark_flow_chunks",
        "embedding_dim": 1536,
    }


def test_config_requires_dsn():
    with pytest.raises(ValidationError):
        PgVectorStore({"table": "t"})


# --- connect / disconnect ----------------------------------------------------

async def test_connect_opens_pool_and_creates_schema(pg):
    pgstore = PgVectorStore({"dsn": DSN, "table": "my_chunks", "embedding_dim": 768})
    assert await pgstore.connect() is True

    pg.factory.assert_called_once_with(DSN, open=False, min_size=1, max_size=4)
    pg.pool.open.assert_awaited_once_with()
    [(kind, ddl, params)] = pg.statements
    assert (kind, params) == ("execute", None)
    assert "CREATE EXTENSION IF NOT EXISTS vector;" in ddl
    assert "CREATE TABLE IF NOT EXISTS my_chunks (" in ddl
    assert "chunk_id TEXT PRIMARY KEY" in ddl
    assert "namespace TEXT NOT NULL" in ddl
    assert "metadata JSONB DEFAULT '{}'::jsonb" in ddl
    assert "embedding vector(768) NOT NULL" in ddl
    assert "CREATE INDEX IF NOT EXISTS my_chunks_doc_idx ON my_chunks(namespace, doc_id);" in ddl
    assert pg.events[-1] == "commit"


async def test_connect_without_psycopg_pool_returns_false(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "psycopg_pool", None)
    pgstore = PgVectorStore({"dsn": DSN})
    with caplog.at_level(logging.ERROR, logger="src.vectorstore.pgvector_store"):
        assert await pgstore.connect() is False
    assert "psycopg[pool] not installed" in caplog.text


@pytest.mark.parametrize("stage", ["construct", "open", "schema"])
async def test_connect_failure_returns_false(pg, stage, caplog):
    error = OSError("connection refused")
    if stage == "construct":
        pg.factory.side_effect = error
    elif stage == "open":
        pg.pool.open.side_effect = error
    else:
        pg.cur.execute.side_effect = error

    pgstore = PgVectorStore({"dsn": DSN})
    with caplog.at_level(logging.ERROR, logger="src.vectorstore.pgvector_store"):
        assert await pgstore.connect() is False
    assert "pgvector connect failed: connection refused" in caplog.text
    assert "secret" not in caplog.text


async def test_disconnect_closes_pool_once(store, pg):
    await store.disconnect()
    await store.disconnect()
    pg.pool.close.assert_awaited_once_with()


async def test_disconnect_swallows_close_errors(store, pg):
    pg.pool.close.side_effect = RuntimeError("already closed")
    await store.disconnect()
    await store.disconnect()
    pg.pool.close.assert_awaited_once()


async def test_disconnect_without_connect_is_noop(pg):
    await PgVectorStore({"dsn": DSN}).disconnect()
    pg.pool.close.assert_not_awaited()


# --- vector literal ------------------------------------------------------------

@pytest.mark.parametrize(
    "vec, literal",
    [
        ([], "[]"),
        ([1.0, -2.5, 0.0], "[1,-2.5,0]"),
        ([0.123456789012], "[0.12345679]"),
        ([1e-12, 3e20], "[1e-12,3e+20]"),
    ],
)
def test_vec_literal(vec, literal):
    assert PgVectorStore._vec_literal(vec) == literal


# --- list_documents ------------------------------------------------------------

async def test_list_documents_aggregates_by_doc_in_namespace(store, pg):
    pg.rows = [("a.md", "h1", "embed", "v1", 3), ("b.md", None, None, None, "1")]

    assert await store.list_documents(NS) == {
        "a.md": DocumentRecord(
            doc_id="a.md", source_hash="h1", embedding_model="embed",
            chunking_version="v1", chunk_count=3,
        ),
        "b.md": DocumentRecord(doc_id="b.md", chunk_count=1),
    }
    [(_, sql, params)] = pg.statements
    assert params == (NS,)
    assert "FROM chunks WHERE namespace = %s GROUP BY doc_id" in sql
    assert "COUNT(*) AS chunk_count" in sql


async def test_list_documents_empty(store, pg):
    assert await store.list_documents(NS) == {}


# --- upsert ----------------------------------------------------------------------

async def test_upsert_replaces_document_chunks_in_one_transaction(store, pg):
    chunks = [_chunk(0, section="Intro"), _chunk(1, embedding=(1.0, 0.0, 0.25))]
    await _upsert(store, chunks)

    assert [e if isinstance(e, str) else e[0] for e in pg.events] == [
        "begin", "execute", "executemany", "end",
    ]
    _, delete_sql, delete_params = pg.events[1]
    assert delete_sql == "DELETE FROM chunks WHERE namespace = %s AND doc_id = %s"
    assert delete_params == (NS, "a.md")

    _, insert_sql, rows = pg.events[2]
    assert insert_sql.startswith(
        "INSERT INTO chunks (chunk_id, namespace, doc_id, source_hash, embedding_model, "
        "chunking_version, text, metadata, embedding) VALUES"
    )
    assert insert_sql.endswith("(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::vector)")
    assert rows == [
        ("a.md#0", NS, "a.md", "h1", "test-embed", "v1", "text 0",
         '{"section": "Intro"}', "[0.5,-1,2]"),
        ("a.md#1", NS, "a.md", "h1", "test-embed", "v1", "text 1", "{}", "[1,0,0.25]"),
    ]
    assert pg.events[-1] == ("end", None)


async def test_upsert_failure_propagates_through_transaction(store, pg):
    pg.cur.executemany.side_effect = RuntimeError("dimension mismatch")
    with pytest.raises(RuntimeError, match="dimension mismatch"):
        await _upsert(store, [_chunk(0)])
    # The transaction block sees the error, so psycopg rolls the DELETE back.
    assert pg.events[-1] == ("end", RuntimeError)


async def test_upsert_without_chunks_runs_no_sql(store, pg):
    await _upsert(store, [])
    assert pg.events == []
    pg.pool.connection.assert_not_called()


async def test_upsert_rejects_chunks_without_embedding_before_touching_db(store, pg):
    with pytest.raises(ValueError, match=r"Missing embedding on chunk a\.md#1"):
        await _upsert(store, [_chunk(0), _chunk(1, embedding=None)])
    assert pg.events == []
    pg.pool.connection.assert_not_called()


# --- delete ----------------------------------------------------------------------

@pytest.mark.parametrize("rowcount, deleted", [(3, True), (1, True), (0, False), (None, False)])
async def test_delete_document(store, pg, rowcount, deleted):
    pg.cur.rowcount = rowcount
    assert await store.delete_document(NS, "a.md") is deleted
    assert pg.events == [
        ("execute", "DELETE FROM chunks WHERE namespace = %s AND doc_id = %s", (NS, "a.md")),
        "commit",
    ]


# --- query -------------------------------------------------------------------------

async def test_query_orders_by_cosine_distance_within_namespace(store, pg):
    pg.rows = [
        ("a.md#0", "a.md", "alpha", {"section": "Intro"}, 0.125),
        ("b.md#2", "b.md", "beta", '{"page": 2}', "0.5"),
        ("c.md#1", "c.md", "gamma", None, 1),
    ]
    hits = await store.query(NS, [0.5, 1.0, -2.0], k=3)

    [(_, sql, params)] = pg.statements
    assert sql == (
        "SELECT chunk_id, doc_id, text, metadata, embedding <=> %s::vector AS dist "
        "FROM chunks WHERE namespace = %s "
        "ORDER BY embedding <=> %s::vector LIMIT %s"
    )
    assert params == ["[0.5,1,-2]", NS, "[0.5,1,-2]", 3]

    assert [(c.chunk_id, c.doc_id, c.text, c.metadata, d) for c, d in hits] == [
        ("a.md#0", "a.md", "alpha", {"section": "Intro"}, 0.125),
        ("b.md#2", "b.md", "beta", {"page": 2}, 0.5),
        ("c.md#1", "c.md", "gamma", {}, 1.0),
    ]
    assert all(isinstance(d, float) and c.embedding is None for c, d in hits)


async def test_query_with_filter_binds_values_in_placeholder_order(store, pg):
    await store.query(NS, [1.0], k=7, filter={"section": "Intro", "page": 2})

    [(_, sql, params)] = pg.statements
    assert sql.count("%s") == len(params)
    assert "WHERE namespace = %s AND metadata->>" in sql
    assert params == ["[1]", NS, "Intro", "2", "[1]", 7]


async def test_query_with_empty_filter_matches_unfiltered_sql(store, pg):
    await store.query(NS, [1.0], k=2, filter={})
    [(_, sql, params)] = pg.statements
    assert "metadata->>" not in sql
    assert params == ["[1]", NS, "[1]", 2]


@pytest.mark.xfail(
    strict=True,
    reason='BUG: filter keys are emitted with json.dumps, i.e. as "double-quoted" '
    "SQL identifiers (column refs), so every metadata-filtered query fails in PostgreSQL",
)
async def test_query_filter_key_is_a_string_literal_or_bound_parameter(store, pg):
    await store.query(NS, [1.0], k=2, filter={"section": "Intro"})
    [(_, sql, params)] = pg.statements
    as_literal = re.search(r"metadata->>'section' = %s", sql)
    as_param = re.search(r"metadata->>%s = %s", sql) and "section" in params
    assert as_literal or as_param, sql


async def test_query_metadata_json_round_trip(store, pg):
    stored = {"section": "Intro", "tags": ["a", "b"]}
    pg.rows = [("a.md#0", "a.md", "alpha", json.dumps(stored), 0.0)]
    [(chunk, _)] = await store.query(NS, [1.0])
    assert chunk.metadata == stored
    assert pg.statements[0][2][-1] == 5, "default k is 5"
