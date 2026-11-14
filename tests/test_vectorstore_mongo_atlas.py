"""Tests for ``src/vectorstore/mongo_atlas_store.py``.

``pymongo.AsyncMongoClient`` and the collection are replaced by spec'd mocks;
the tests assert on the filters, documents and aggregation pipelines sent to
MongoDB Atlas.
"""
import logging
import sys
from unittest.mock import MagicMock

import pymongo
import pytest
from pydantic import ValidationError
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.results import DeleteResult

from src.vectorstore.base import Chunk, DocumentRecord, VectorStoreType
from src.vectorstore.mongo_atlas_store import MongoAtlasConfig, MongoAtlasVectorStore

URI = "mongodb://user:secret@localhost:27017"
NS = "tenant-a"
BASE_CONFIG = {"uri": URI, "database": "vectors", "collection": "chunks", "embedding_dim": 3}


class MongoHarness:
    """Mocked AsyncMongoClient -> database -> collection chain."""

    def __init__(self):
        self.coll = MagicMock(spec=AsyncCollection)
        self.db = MagicMock(spec=AsyncDatabase)
        self.db.__getitem__.return_value = self.coll
        self.client = MagicMock(spec=pymongo.AsyncMongoClient)
        self.client.__getitem__.return_value = self.db
        self.factory = MagicMock(return_value=self.client)
        self.rows: list = []
        self.coll.aggregate.side_effect = self._aggregate

    async def _aggregate(self, pipeline):
        cursor = MagicMock(spec=AsyncCommandCursor)
        cursor.__aiter__.return_value = list(self.rows)
        return cursor

    @property
    def pipeline(self):
        [call] = self.coll.aggregate.await_args_list
        return call.args[0]


@pytest.fixture
def mongo(monkeypatch):
    harness = MongoHarness()
    monkeypatch.setattr(pymongo, "AsyncMongoClient", harness.factory)
    return harness


async def _connected(mongo, **overrides):
    store = MongoAtlasVectorStore({**BASE_CONFIG, **overrides})
    assert await store.connect() is True
    mongo.coll.reset_mock()
    return store


@pytest.fixture
async def store(mongo):
    return await _connected(mongo)


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
    store = MongoAtlasVectorStore({"uri": URI, "database": "db", "extra": "ignored"})
    assert store.store_type is VectorStoreType.MONGO_ATLAS
    assert MongoAtlasConfig.model_validate({"uri": URI, "database": "db"}).model_dump() == {
        "uri": URI,
        "database": "db",
        "collection": "ws_mark_flow_chunks",
        "embedding_dim": 1536,
        "index_name": "vector_index",
        "similarity": "cosine",
        "auto_create_index": True,
    }


@pytest.mark.parametrize("missing", ["uri", "database"])
def test_config_requires_uri_and_database(missing):
    config = dict(BASE_CONFIG)
    del config[missing]
    with pytest.raises(ValidationError):
        MongoAtlasVectorStore(config)


# --- connect / disconnect ----------------------------------------------------

async def test_connect_selects_collection_and_creates_indexes(mongo):
    store = MongoAtlasVectorStore(
        {**BASE_CONFIG, "embedding_dim": 768, "index_name": "vs_idx", "similarity": "dotProduct"}
    )
    assert await store.connect() is True

    mongo.factory.assert_called_once_with(URI)
    mongo.client.__getitem__.assert_called_once_with("vectors")
    mongo.db.__getitem__.assert_called_once_with("chunks")
    mongo.coll.create_index.assert_awaited_once_with([("namespace", 1), ("doc_id", 1)])
    mongo.coll.create_search_index.assert_awaited_once_with(
        {
            "name": "vs_idx",
            "type": "vectorSearch",
            "definition": {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 768,
                        "similarity": "dotProduct",
                    },
                    {"type": "filter", "path": "namespace"},
                    {"type": "filter", "path": "doc_id"},
                ],
            },
        }
    )


async def test_connect_can_skip_vector_index_creation(mongo):
    await _connected(mongo, auto_create_index=False)
    mongo.coll.create_search_index.assert_not_awaited()


async def test_connect_tolerates_index_creation_errors(mongo, caplog):
    mongo.coll.create_index.side_effect = OperationFailure("not authorized")
    mongo.coll.create_search_index.side_effect = OperationFailure("index already exists")

    store = MongoAtlasVectorStore(BASE_CONFIG)
    with caplog.at_level(logging.DEBUG, logger="src.vectorstore.mongo_atlas_store"):
        assert await store.connect() is True
    assert "create_index(namespace, doc_id): not authorized" in caplog.text
    assert "create_search_index: index already exists" in caplog.text


async def test_connect_without_pymongo_returns_false(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "pymongo", None)
    store = MongoAtlasVectorStore(BASE_CONFIG)
    with caplog.at_level(logging.ERROR, logger="src.vectorstore.mongo_atlas_store"):
        assert await store.connect() is False
    assert "pymongo not installed" in caplog.text


async def test_connect_failure_returns_false(mongo, caplog):
    mongo.factory.side_effect = ConfigurationError("bad URI")
    store = MongoAtlasVectorStore(BASE_CONFIG)
    with caplog.at_level(logging.ERROR, logger="src.vectorstore.mongo_atlas_store"):
        assert await store.connect() is False
    assert "MongoAtlas connect failed: bad URI" in caplog.text


async def test_disconnect_closes_client_once(store, mongo):
    await store.disconnect()
    await store.disconnect()
    mongo.client.close.assert_awaited_once_with()


async def test_disconnect_swallows_close_errors(store, mongo):
    mongo.client.close.side_effect = RuntimeError("already closed")
    await store.disconnect()
    mongo.client.close.assert_awaited_once()
    with pytest.raises(AttributeError):
        await store.list_documents(NS)  # collection handle was released


async def test_disconnect_without_connect_is_noop(mongo):
    await MongoAtlasVectorStore(BASE_CONFIG).disconnect()
    mongo.client.close.assert_not_awaited()


# --- list_documents ------------------------------------------------------------

async def test_list_documents_groups_chunks_of_namespace(store, mongo):
    mongo.rows = [
        {"_id": "a.md", "source_hash": "h1", "embedding_model": "m",
         "chunking_version": "v1", "chunk_count": 3},
        {"_id": "b.md", "chunk_count": None},
        {"_id": None, "chunk_count": 4},
        {"_id": "", "chunk_count": 1},
    ]
    assert await store.list_documents(NS) == {
        "a.md": DocumentRecord(
            doc_id="a.md", source_hash="h1", embedding_model="m",
            chunking_version="v1", chunk_count=3,
        ),
        "b.md": DocumentRecord(doc_id="b.md", chunk_count=0),
    }
    assert mongo.pipeline == [
        {"$match": {"namespace": NS}},
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


async def test_list_documents_empty(store, mongo):
    assert await store.list_documents(NS) == {}


# --- upsert ----------------------------------------------------------------------

async def test_upsert_deletes_old_chunks_then_inserts_new_ones(store, mongo):
    chunks = [_chunk(0, section="Intro"), _chunk(1, embedding=(1.0, 0.0, 0.25))]
    await _upsert(store, chunks)

    assert [c[0] for c in mongo.coll.mock_calls] == ["delete_many", "insert_many"]
    mongo.coll.delete_many.assert_awaited_once_with({"namespace": NS, "doc_id": "a.md"})
    mongo.coll.insert_many.assert_awaited_once_with(
        [
            {
                "_id": "a.md#0", "namespace": NS, "doc_id": "a.md", "source_hash": "h1",
                "embedding_model": "test-embed", "chunking_version": "v1", "text": "text 0",
                "metadata": {"section": "Intro"}, "embedding": [0.5, -1.0, 2.0],
            },
            {
                "_id": "a.md#1", "namespace": NS, "doc_id": "a.md", "source_hash": "h1",
                "embedding_model": "test-embed", "chunking_version": "v1", "text": "text 1",
                "metadata": {}, "embedding": [1.0, 0.0, 0.25],
            },
        ],
        ordered=False,
    )


async def test_upsert_stores_copies_of_chunk_data(store, mongo):
    chunk = _chunk(0, section="Intro")
    await _upsert(store, [chunk])
    [doc] = mongo.coll.insert_many.await_args.args[0]
    assert doc["metadata"] is not chunk.metadata
    assert doc["embedding"] is not chunk.embedding


async def test_upsert_without_chunks_is_a_noop(store, mongo):
    await _upsert(store, [])
    assert mongo.coll.mock_calls == []


async def test_upsert_rejects_chunks_without_embedding_before_deleting(store, mongo):
    with pytest.raises(ValueError, match=r"Missing embedding on chunk a\.md#1"):
        await _upsert(store, [_chunk(0), _chunk(1, embedding=None)])
    assert mongo.coll.mock_calls == []


# --- delete ----------------------------------------------------------------------

@pytest.mark.parametrize("deleted_count, deleted", [(2, True), (1, True), (0, False), (None, False)])
async def test_delete_document(store, mongo, deleted_count, deleted):
    mongo.coll.delete_many.return_value = MagicMock(spec=DeleteResult, deleted_count=deleted_count)
    assert await store.delete_document(NS, "a.md") is deleted
    mongo.coll.delete_many.assert_awaited_once_with({"namespace": NS, "doc_id": "a.md"})


# --- query -------------------------------------------------------------------------

def _vector_search(pipeline):
    return pipeline[0]["$vectorSearch"]


async def test_query_pipeline_without_filter(store, mongo):
    await store.query(NS, [0.5, 1.0, -2.0], k=4)
    assert mongo.pipeline == [
        {
            "$vectorSearch": {
                "index": "vector_index",
                "path": "embedding",
                "queryVector": [0.5, 1.0, -2.0],
                "numCandidates": 50,
                "limit": 4,
                "filter": {"namespace": {"$eq": NS}},
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


@pytest.mark.parametrize("k, candidates", [(1, 50), (5, 50), (6, 60), (20, 200)])
async def test_query_num_candidates_is_ten_times_k_with_floor(store, mongo, k, candidates):
    await store.query(NS, [1.0], k=k)
    vs = _vector_search(mongo.pipeline)
    assert (vs["numCandidates"], vs["limit"]) == (candidates, k)


async def test_query_default_k_is_five(store, mongo):
    await store.query(NS, [1.0])
    assert _vector_search(mongo.pipeline)["limit"] == 5


async def test_query_doc_id_filter_is_pushed_into_vector_search(store, mongo):
    await store.query(NS, [1.0], k=3, filter={"doc_id": "a.md"})
    pipeline = mongo.pipeline
    assert _vector_search(pipeline)["filter"] == {
        "namespace": {"$eq": NS},
        "doc_id": {"$eq": "a.md"},
    }
    assert [next(iter(stage)) for stage in pipeline] == ["$vectorSearch", "$project"]


async def test_query_metadata_filter_becomes_post_match(store, mongo):
    await store.query(NS, [1.0], k=3, filter={"section": "Intro", "page": 2, "doc_id": "a.md"})
    pipeline = mongo.pipeline
    assert [next(iter(stage)) for stage in pipeline] == ["$vectorSearch", "$match", "$project"]
    assert pipeline[1] == {"$match": {"metadata.section": "Intro", "metadata.page": 2}}
    assert _vector_search(pipeline)["filter"] == {
        "namespace": {"$eq": NS},
        "doc_id": {"$eq": "a.md"},
    }


async def test_query_with_empty_filter_has_no_post_match(store, mongo):
    await store.query(NS, [1.0], filter={})
    assert [next(iter(stage)) for stage in mongo.pipeline] == ["$vectorSearch", "$project"]


async def test_query_converts_cosine_similarity_to_distance(store, mongo):
    mongo.rows = [
        {"_id": "a.md#0", "doc_id": "a.md", "text": "alpha",
         "metadata": {"section": "Intro"}, "score": 0.75},
        {"_id": "b.md#1", "score": None, "metadata": None},
    ]
    hits = await store.query(NS, [1.0], k=2)
    assert [(c.chunk_id, c.doc_id, c.text, c.metadata, d) for c, d in hits] == [
        ("a.md#0", "a.md", "alpha", {"section": "Intro"}, 0.25),
        ("b.md#1", "", "", {}, 1.0),
    ]
    assert all(c.embedding is None for c, _ in hits)


@pytest.mark.parametrize("similarity", ["euclidean", "dotProduct"])
async def test_query_negates_non_cosine_scores(mongo, similarity):
    store = await _connected(mongo, similarity=similarity, index_name="idx2")
    mongo.rows = [
        {"_id": "a.md#0", "doc_id": "a.md", "text": "near", "metadata": {}, "score": 0.9},
        {"_id": "a.md#1", "doc_id": "a.md", "text": "far", "metadata": {}, "score": 0.2},
    ]
    hits = await store.query(NS, [1.0], k=2)
    assert [d for _, d in hits] == [-0.9, -0.2]
    assert hits[0][1] < hits[1][1], "lower distance must still mean a better match"
    assert _vector_search(mongo.pipeline)["index"] == "idx2"


async def test_query_with_no_hits(store, mongo):
    assert await store.query(NS, [1.0]) == []
