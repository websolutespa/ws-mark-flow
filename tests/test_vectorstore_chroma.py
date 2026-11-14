"""Tests for ``src/vectorstore/chroma_store.py``.

They drive a real, in-process ``chromadb.PersistentClient`` rooted in
``tmp_path``. The store always passes explicit embeddings, so Chroma never
needs (or downloads) an embedding model and everything runs offline.
"""
import logging
import math

import pytest
from pydantic import ValidationError

from src.vectorstore.base import Chunk, DocumentRecord, VectorStoreType
from src.vectorstore.chroma_store import ChromaConfig, ChromaVectorStore

NS = "tenant-a"
OTHER_NS = "tenant-b"

X = [1.0, 0.0, 0.0]
Y = [0.0, 1.0, 0.0]
Z = [0.0, 0.0, 1.0]
XY = [1.0, 1.0, 0.0]


def _chunk(doc, index, embedding, text=None, metadata=None, **extra):
    return Chunk(
        chunk_id=f"{doc}#{index}",
        doc_id=doc,
        text=text if text is not None else f"{doc} part {index}",
        metadata={**(metadata or {}), **extra},
        embedding=embedding,
    )


async def _upsert(store, doc_id, chunks, namespace=NS, source_hash="h1"):
    await store.upsert_document(
        namespace=namespace,
        doc_id=doc_id,
        chunks=chunks,
        source_hash=source_hash,
        embedding_model="test-embed",
        chunking_version="v1",
    )


@pytest.fixture
async def store(tmp_path):
    chroma = ChromaVectorStore({"path": str(tmp_path / "chroma")})
    assert await chroma.connect() is True
    yield chroma
    await chroma.disconnect()


# --- Configuration & lifecycle -----------------------------------------------

def test_store_type_and_config():
    chroma = ChromaVectorStore({"path": "/data/chroma", "unknown": "ignored"})
    assert chroma.store_type is VectorStoreType.CHROMA
    assert ChromaConfig.model_validate({"path": "x", "dsn": "y"}).model_dump() == {"path": "x"}


def test_config_requires_a_path():
    with pytest.raises(ValidationError):
        ChromaVectorStore({})


async def test_connect_creates_missing_directories(tmp_path):
    target = tmp_path / "nested" / "deeper" / "chroma"
    chroma = ChromaVectorStore({"path": str(target)})
    assert await chroma.connect() is True
    assert target.is_dir()
    await _upsert(chroma, "a.md", [_chunk("a.md", 0, X)])
    assert any(target.iterdir()), "Chroma must persist under the configured path"
    await chroma.disconnect()


async def test_connect_expands_user_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    chroma = ChromaVectorStore({"path": "~/chroma-home"})
    assert await chroma.connect() is True
    assert (tmp_path / "chroma-home").is_dir()
    await chroma.disconnect()


@pytest.mark.parametrize(
    "call",
    [
        lambda s: s.list_documents(NS),
        lambda s: s.delete_document(NS, "a.md"),
        lambda s: s.query(NS, X),
        lambda s: _upsert(s, "a.md", [_chunk("a.md", 0, X)]),
    ],
    ids=["list_documents", "delete_document", "query", "upsert_document"],
)
async def test_operations_require_a_connection(call, tmp_path):
    chroma = ChromaVectorStore({"path": str(tmp_path / "chroma")})
    with pytest.raises(RuntimeError, match="Chroma not connected"):
        await call(chroma)

    assert await chroma.connect() is True
    await chroma.disconnect()
    with pytest.raises(RuntimeError, match="Chroma not connected"):
        await call(chroma)


async def test_data_survives_reconnect(tmp_path):
    path = str(tmp_path / "chroma")
    first = ChromaVectorStore({"path": path})
    await first.connect()
    await _upsert(first, "a.md", [_chunk("a.md", 0, X), _chunk("a.md", 1, Y)])
    await first.disconnect()

    second = ChromaVectorStore({"path": path})
    await second.connect()
    assert (await second.list_documents(NS))["a.md"].chunk_count == 2
    await second.disconnect()


# --- upsert / list -----------------------------------------------------------

async def test_upsert_then_list_documents(store):
    await _upsert(store, "a.md", [_chunk("a.md", i, v) for i, v in enumerate([X, Y, Z])])
    await _upsert(store, "b.md", [_chunk("b.md", 0, XY)], source_hash="h-b")

    assert await store.list_documents(NS) == {
        "a.md": DocumentRecord(
            doc_id="a.md",
            source_hash="h1",
            embedding_model="test-embed",
            chunking_version="v1",
            chunk_count=3,
        ),
        "b.md": DocumentRecord(
            doc_id="b.md",
            source_hash="h-b",
            embedding_model="test-embed",
            chunking_version="v1",
            chunk_count=1,
        ),
    }


async def test_list_documents_of_empty_namespace(store):
    assert await store.list_documents(NS) == {}


async def test_upsert_replaces_previous_chunks(store):
    await _upsert(store, "a.md", [_chunk("a.md", i, v) for i, v in enumerate([X, Y, Z])])
    await _upsert(store, "a.md", [_chunk("a.md", 0, Y, text="rewritten")], source_hash="h2")

    record = (await store.list_documents(NS))["a.md"]
    assert (record.chunk_count, record.source_hash) == (1, "h2")
    hits = await store.query(NS, X, k=10)
    assert [(c.chunk_id, c.text) for c, _ in hits] == [("a.md#0", "rewritten")]


async def test_upsert_does_not_touch_other_documents(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)])
    await _upsert(store, "b.md", [_chunk("b.md", 0, Y), _chunk("b.md", 1, Z)])
    await _upsert(store, "a.md", [_chunk("a.md", 0, XY)], source_hash="h2")
    counts = {d: r.chunk_count for d, r in (await store.list_documents(NS)).items()}
    assert counts == {"a.md": 1, "b.md": 2}


async def test_upsert_with_no_chunks_is_a_noop(tmp_path):
    # Returns before touching the client, so it works even when disconnected.
    chroma = ChromaVectorStore({"path": str(tmp_path / "chroma")})
    assert await _upsert(chroma, "a.md", []) is None


async def test_upsert_with_no_chunks_keeps_existing_document(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)])
    await _upsert(store, "a.md", [])
    assert (await store.list_documents(NS))["a.md"].chunk_count == 1


async def test_upsert_requires_embeddings_on_every_chunk(store):
    chunks = [_chunk("a.md", 0, X), _chunk("a.md", 1, None)]
    with pytest.raises(ValueError, match=r"All chunks must have embeddings \(doc=a\.md\)"):
        await _upsert(store, "a.md", chunks)
    assert await store.list_documents(NS) == {}


@pytest.mark.xfail(
    strict=True,
    reason="BUG: upsert deletes the old chunks before validating embeddings, "
    "so an invalid re-upsert wipes the stored document",
)
async def test_invalid_upsert_keeps_existing_document(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X), _chunk("a.md", 1, Y)])
    with pytest.raises(ValueError):
        await _upsert(store, "a.md", [_chunk("a.md", 0, None)], source_hash="h2")
    records = await store.list_documents(NS)
    assert "a.md" in records, "the previously stored document was deleted"
    assert records["a.md"].chunk_count == 2


async def test_upsert_stores_bookkeeping_metadata_over_chunk_metadata(store):
    chunk = _chunk(
        "a.md", 0, X, metadata={"doc_id": "spoofed", "source_hash": "spoofed"}, section="Intro", page=3
    )
    await _upsert(store, "a.md", [chunk])

    [(hit, _)] = await store.query(NS, X, k=1)
    assert hit.doc_id == "a.md"
    assert hit.metadata == {
        "section": "Intro",
        "page": 3,
        "doc_id": "a.md",
        "source_hash": "h1",
        "embedding_model": "test-embed",
        "chunking_version": "v1",
    }
    assert chunk.metadata["doc_id"] == "spoofed", "caller's chunk must not be mutated"


async def test_upsert_tolerates_failing_pre_delete(store, monkeypatch, caplog):
    real_collection = store._collection

    class DeleteFails:
        def __init__(self, col):
            self._col = col

        def delete(self, **kwargs):
            raise RuntimeError("delete unsupported")

        def __getattr__(self, name):
            return getattr(self._col, name)

    monkeypatch.setattr(store, "_collection", lambda ns: DeleteFails(real_collection(ns)))
    with caplog.at_level(logging.DEBUG, logger="src.vectorstore.chroma_store"):
        await _upsert(store, "a.md", [_chunk("a.md", 0, X)])

    assert (await store.list_documents(NS))["a.md"].chunk_count == 1
    assert "Chroma delete (pre-upsert) for a.md: delete unsupported" in caplog.text


# --- Namespaces ----------------------------------------------------------------

async def test_namespaces_are_isolated(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)], namespace=NS)
    await _upsert(store, "a.md", [_chunk("a.md", 0, X), _chunk("a.md", 1, Y)], namespace=OTHER_NS)

    assert (await store.list_documents(NS))["a.md"].chunk_count == 1
    assert (await store.list_documents(OTHER_NS))["a.md"].chunk_count == 2

    assert await store.delete_document(NS, "a.md") is True
    assert await store.list_documents(NS) == {}
    assert (await store.list_documents(OTHER_NS))["a.md"].chunk_count == 2
    assert len(await store.query(OTHER_NS, X, k=10)) == 2


async def test_empty_namespace_uses_default_collection(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)], namespace="")
    assert list(await store.list_documents("")) == ["a.md"]
    assert store._client.get_collection("default").count() == 1


async def test_collections_use_cosine_distance(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)])
    assert store._client.get_collection(NS).metadata["hnsw:space"] == "cosine"


# --- delete ------------------------------------------------------------------

async def test_delete_document(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X), _chunk("a.md", 1, Y)])
    await _upsert(store, "b.md", [_chunk("b.md", 0, Z)])

    assert await store.delete_document(NS, "a.md") is True
    assert list(await store.list_documents(NS)) == ["b.md"]
    assert await store.delete_document(NS, "a.md") is False


async def test_delete_unknown_document_returns_false(store):
    assert await store.delete_document(NS, "missing.md") is False


# --- query -------------------------------------------------------------------

async def test_query_ranks_by_cosine_distance(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X), _chunk("a.md", 1, Y)])
    await _upsert(store, "b.md", [_chunk("b.md", 0, XY)])

    hits = await store.query(NS, X, k=5)
    assert [c.chunk_id for c, _ in hits] == ["a.md#0", "b.md#0", "a.md#1"]
    distances = [d for _, d in hits]
    assert all(isinstance(d, float) for d in distances)
    assert distances == pytest.approx([0.0, 1 - 1 / math.sqrt(2), 1.0], abs=1e-4)
    first, _ = hits[0]
    assert (first.doc_id, first.text, first.embedding) == ("a.md", "a.md part 0", None)


@pytest.mark.parametrize("k", [1, 2])
async def test_query_returns_at_most_k_results(store, k):
    await _upsert(store, "a.md", [_chunk("a.md", i, v) for i, v in enumerate([X, Y, Z])])
    hits = await store.query(NS, X, k=k)
    assert len(hits) == k
    assert hits[0][0].chunk_id == "a.md#0"


async def test_query_applies_metadata_filter(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X, section="intro")])
    await _upsert(store, "b.md", [_chunk("b.md", 0, X, section="body")])

    hits = await store.query(NS, X, k=5, filter={"section": "body"})
    assert [c.chunk_id for c, _ in hits] == ["b.md#0"]
    hits = await store.query(NS, X, k=5, filter={"doc_id": "a.md"})
    assert [c.chunk_id for c, _ in hits] == ["a.md#0"]


async def test_query_treats_empty_filter_as_no_filter(store):
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)])
    await _upsert(store, "b.md", [_chunk("b.md", 0, Y)])
    assert len(await store.query(NS, X, k=5, filter={})) == 2


async def test_query_empty_namespace_returns_nothing(store):
    assert await store.query(NS, X, k=3) == []


# --- Records not written by this store ----------------------------------------

async def test_foreign_records_are_tolerated(store):
    """Collections may hold records added by other tools (no/partial metadata)."""
    await _upsert(store, "a.md", [_chunk("a.md", 0, X)])
    col = store._client.get_collection(NS)
    col.add(ids=["bare"], embeddings=[Y], documents=["no metadata"])
    col.add(ids=["partial"], embeddings=[Z], documents=["no doc id"], metadatas=[{"k": "v"}])

    assert list(await store.list_documents(NS)) == ["a.md"]

    hits = {c.chunk_id: c for c, _ in await store.query(NS, Y, k=5)}
    assert set(hits) == {"a.md#0", "bare", "partial"}
    assert (hits["bare"].doc_id, hits["bare"].metadata) == ("", {})
    assert (hits["partial"].doc_id, hits["partial"].metadata) == ("", {"k": "v"})
