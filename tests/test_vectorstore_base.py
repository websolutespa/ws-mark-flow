"""Tests for ``src/vectorstore/base.py``: shared models, helpers and ABC contracts."""
import hashlib
import inspect

import pytest
from pydantic import TypeAdapter, ValidationError

import src.vectorstore as vectorstore_pkg
from src.vectorstore.base import (
    Chunk,
    ChunkMention,
    DocumentRecord,
    Entity,
    GraphCapableVectorStore,
    Relation,
    VectorStoreIntegration,
    VectorStoreType,
    hash_text,
    make_doc_id,
)
from src.vectorstore.chroma_store import ChromaVectorStore
from src.vectorstore.mongo_atlas_store import MongoAtlasVectorStore
from src.vectorstore.neo4j_store import Neo4jVectorStore
from src.vectorstore.pgvector_store import PgVectorStore
from src.vectorstore.redis_store import RedisVectorStore

ALL_STORES = [
    ChromaVectorStore,
    PgVectorStore,
    MongoAtlasVectorStore,
    Neo4jVectorStore,
    RedisVectorStore,
]


# --- VectorStoreType ---------------------------------------------------------

def test_store_type_values_are_the_persisted_identifiers():
    # These strings are stored in job documents and sent by the UI: renaming
    # one silently orphans existing ingestion jobs.
    assert {member.name: member.value for member in VectorStoreType} == {
        "CHROMA": "chroma",
        "PGVECTOR": "pgvector",
        "MONGO_ATLAS": "mongo_atlas",
        "NEO4J": "neo4j",
        "REDIS": "redis",
    }


def test_store_type_is_a_str_enum_looked_up_by_value():
    assert VectorStoreType("pgvector") is VectorStoreType.PGVECTOR
    assert isinstance(VectorStoreType.REDIS, str)
    assert VectorStoreType.REDIS == "redis"
    assert VectorStoreType.MONGO_ATLAS.value == "mongo_atlas"


@pytest.mark.parametrize("bad", ["qdrant", "CHROMA", "", "Chroma"])
def test_store_type_rejects_unknown_backend(bad):
    with pytest.raises(ValueError):
        VectorStoreType(bad)


def test_store_type_serialises_to_its_plain_value():
    adapter = TypeAdapter(VectorStoreType)
    assert adapter.dump_json(VectorStoreType.NEO4J) == b'"neo4j"'
    assert adapter.validate_python("chroma") is VectorStoreType.CHROMA
    assert adapter.validate_json(b'"mongo_atlas"') is VectorStoreType.MONGO_ATLAS


# --- Models ------------------------------------------------------------------

def test_chunk_defaults():
    chunk = Chunk(chunk_id="a#0", doc_id="a.md", text="hello")
    assert chunk.metadata == {}
    assert chunk.embedding is None
    assert chunk.model_dump() == {
        "chunk_id": "a#0",
        "doc_id": "a.md",
        "text": "hello",
        "metadata": {},
        "embedding": None,
    }


def test_chunk_metadata_default_is_not_shared_between_instances():
    first = Chunk(chunk_id="a#0", doc_id="a.md", text="x")
    second = Chunk(chunk_id="a#1", doc_id="a.md", text="y")
    first.metadata["k"] = "v"
    assert second.metadata == {}


def test_chunk_keeps_embedding_and_metadata():
    chunk = Chunk(
        chunk_id="a#0",
        doc_id="a.md",
        text="hello",
        metadata={"section": "intro", "chunk_index": 0},
        embedding=[0.25, -1, 3],
    )
    assert chunk.embedding == [0.25, -1.0, 3.0]
    assert all(isinstance(x, float) for x in chunk.embedding)
    assert chunk.metadata == {"section": "intro", "chunk_index": 0}


@pytest.mark.parametrize("missing", ["chunk_id", "doc_id", "text"])
def test_chunk_requires_identity_and_text(missing):
    data = {"chunk_id": "a#0", "doc_id": "a.md", "text": "hello"}
    del data[missing]
    with pytest.raises(ValidationError) as exc:
        Chunk(**data)
    assert [err["loc"] for err in exc.value.errors()] == [(missing,)]


def test_chunk_rejects_non_numeric_embedding():
    with pytest.raises(ValidationError):
        Chunk(chunk_id="a#0", doc_id="a.md", text="x", embedding=["not-a-number"])


def test_document_record_defaults():
    record = DocumentRecord(doc_id="a.md")
    assert record.model_dump() == {
        "doc_id": "a.md",
        "source_hash": None,
        "embedding_model": None,
        "chunking_version": None,
        "chunk_count": 0,
    }
    with pytest.raises(ValidationError):
        DocumentRecord()


def test_document_record_equality_is_by_value():
    kwargs = dict(
        doc_id="a.md", source_hash="h", embedding_model="m", chunking_version="v", chunk_count=2
    )
    assert DocumentRecord(**kwargs) == DocumentRecord(**kwargs)
    assert DocumentRecord(**kwargs) != DocumentRecord(**{**kwargs, "chunk_count": 3})


def test_graph_models_defaults_and_required_fields():
    entity = Entity(id="person:ada", label="Person", name="Ada")
    assert entity.properties == {}
    relation = Relation(source_id="person:ada", target_id="org:acme", type="WORKS_AT")
    assert relation.properties == {}
    mention = ChunkMention(chunk_id="a#0", entity_id="person:ada")
    assert mention.span is None

    with pytest.raises(ValidationError):
        Entity(id="x", label="Person")
    with pytest.raises(ValidationError):
        Relation(source_id="a", target_id="b")
    with pytest.raises(ValidationError):
        ChunkMention(chunk_id="a#0")


def test_graph_model_property_defaults_are_not_shared():
    first = Entity(id="a", label="Person", name="A")
    second = Entity(id="b", label="Person", name="B")
    first.properties["role"] = "cto"
    assert second.properties == {}


# --- Helpers -----------------------------------------------------------------

@pytest.mark.parametrize(
    "path, expected",
    [
        ("/Docs/Report.MD", "docs/report.md"),
        ("docs/report.md", "docs/report.md"),
        ("///Nested/Deep/File.md", "nested/deep/file.md"),
        ("/A/B/", "a/b/"),
        ("/Schöne/ÜBERSICHT.md", "schöne/übersicht.md"),
        ("/with space/My File.md", "with space/my file.md"),
        ("/", ""),
        ("", ""),
    ],
)
def test_make_doc_id(path, expected):
    assert make_doc_id(path) == expected


def test_make_doc_id_is_idempotent_and_case_insensitive():
    once = make_doc_id("/Docs/Report.md")
    assert make_doc_id(once) == once
    assert make_doc_id("/DOCS/REPORT.MD") == once


@pytest.mark.parametrize(
    "text, digest",
    [
        ("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
    ],
)
def test_hash_text_known_sha256_vectors(text, digest):
    assert hash_text(text) == digest


def test_hash_text_hashes_utf8_bytes():
    text = "perché → 日本語\n"
    assert hash_text(text) == hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert len(hash_text(text)) == 64


def test_hash_text_is_sensitive_to_any_change():
    assert hash_text("# Title\nbody") == hash_text("# Title\nbody")
    assert hash_text("# Title\nbody") != hash_text("# Title\nbody ")
    assert hash_text("a\nb") != hash_text("a\r\nb")


# --- Async context manager ---------------------------------------------------

class _CountingStoreMixin:
    """Counts connect/disconnect calls on top of the shared in-memory fake."""

    connect_calls = 0
    disconnect_calls = 0

    async def connect(self):
        self.connect_calls += 1
        return await super().connect()

    async def disconnect(self):
        self.disconnect_calls += 1
        await super().disconnect()


@pytest.fixture
def counting_store(fake_vector_store):
    cls = type("CountingStore", (_CountingStoreMixin, fake_vector_store), {})
    return cls


async def test_context_manager_connects_yields_self_and_disconnects(counting_store):
    store = counting_store()
    async with store as entered:
        assert entered is store
        assert store.connected is True
        assert (store.connect_calls, store.disconnect_calls) == (1, 0)
    assert store.connected is False
    assert (store.connect_calls, store.disconnect_calls) == (1, 1)


async def test_context_manager_raises_when_connect_returns_false(counting_store):
    store = counting_store(connect_ok=False)
    body_ran = False
    # The message must carry the enum *value*: on Python >= 3.12 formatting a
    # ``(str, Enum)`` member directly yields "VectorStoreType.CHROMA".
    with pytest.raises(RuntimeError, match=r"^Failed to connect to vector store chroma$"):
        async with store:
            body_ran = True
    assert body_ran is False
    assert store.connect_calls == 1


async def test_context_manager_disconnects_and_propagates_body_errors(counting_store):
    store = counting_store()
    with pytest.raises(KeyError, match="boom"):
        async with store:
            raise KeyError("boom")
    assert store.connected is False
    assert store.disconnect_calls == 1


async def test_aexit_does_not_swallow_exceptions(fake_vector_store):
    store = fake_vector_store()
    await store.connect()
    assert await store.__aexit__(ValueError, ValueError("x"), None) is None
    assert store.connected is False


async def test_context_manager_propagates_connect_exceptions(fake_vector_store):
    class Exploding(fake_vector_store):
        async def connect(self):
            raise ConnectionError("backend down")

    with pytest.raises(ConnectionError, match="backend down"):
        async with Exploding():
            pytest.fail("body must not run")  # pragma: no cover


# --- Abstract contracts ------------------------------------------------------

def test_vector_store_abstract_surface():
    assert VectorStoreIntegration.__abstractmethods__ == frozenset(
        {
            "store_type",
            "connect",
            "disconnect",
            "list_documents",
            "upsert_document",
            "delete_document",
            "query",
        }
    )
    with pytest.raises(TypeError):
        VectorStoreIntegration()


def test_graph_capability_abstract_surface():
    assert GraphCapableVectorStore.__abstractmethods__ == frozenset(
        {"upsert_graph", "delete_graph", "expand_chunks"}
    )
    with pytest.raises(TypeError):
        GraphCapableVectorStore()


def test_partial_implementation_cannot_be_instantiated(fake_vector_store):
    class NoQuery(VectorStoreIntegration):
        store_type = VectorStoreType.CHROMA

        async def connect(self):
            return True  # pragma: no cover

        async def disconnect(self):
            return None  # pragma: no cover

    with pytest.raises(TypeError, match="abstract"):
        NoQuery()
    assert isinstance(fake_vector_store(), VectorStoreIntegration)


def test_abstract_method_defaults():
    query = inspect.signature(VectorStoreIntegration.query).parameters
    assert query["k"].default == 5
    assert query["filter"].default is None
    expand = inspect.signature(GraphCapableVectorStore.expand_chunks).parameters
    assert expand["hops"].default == 1
    assert expand["limit"].default == 25


@pytest.mark.parametrize("store_cls", ALL_STORES, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "method", ["list_documents", "upsert_document", "delete_document", "query"]
)
def test_backends_keep_the_abstract_signatures(store_cls, method):
    """Callers pass these arguments by keyword (see ingestion.py / main.py)."""
    expected = inspect.signature(getattr(VectorStoreIntegration, method)).parameters
    actual = inspect.signature(getattr(store_cls, method)).parameters
    assert list(actual) == list(expected)
    assert {n: p.default for n, p in actual.items()} == {
        n: p.default for n, p in expected.items()
    }
    assert inspect.iscoroutinefunction(getattr(store_cls, method))


@pytest.mark.parametrize("method", ["upsert_graph", "delete_graph", "expand_chunks"])
def test_neo4j_keeps_the_graph_signatures(method):
    expected = inspect.signature(getattr(GraphCapableVectorStore, method)).parameters
    actual = inspect.signature(getattr(Neo4jVectorStore, method)).parameters
    assert list(actual) == list(expected)
    assert {n: p.default for n, p in actual.items()} == {
        n: p.default for n, p in expected.items()
    }


# --- Package surface ---------------------------------------------------------

def test_package_exports_resolve():
    assert set(vectorstore_pkg.__all__) >= {
        "VectorStoreIntegration",
        "VectorStoreType",
        "Chunk",
        "DocumentRecord",
        "Entity",
        "Relation",
        "ChunkMention",
        "GraphCapableVectorStore",
        "ChromaVectorStore",
        "PgVectorStore",
        "MongoAtlasVectorStore",
        "Neo4jVectorStore",
        "RedisVectorStore",
    }
    for name in vectorstore_pkg.__all__:
        assert getattr(vectorstore_pkg, name) is not None


def test_only_neo4j_advertises_the_graph_capability():
    for store_cls in ALL_STORES:
        assert issubclass(store_cls, VectorStoreIntegration)
    assert [c for c in ALL_STORES if issubclass(c, GraphCapableVectorStore)] == [
        Neo4jVectorStore
    ]
