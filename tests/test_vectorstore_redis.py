"""Tests for ``src/vectorstore/redis_store.py`` with the redis asyncio client mocked out."""
import json
import logging
import struct
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio
from pydantic import ValidationError
from redis.asyncio.client import Pipeline
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from src.vectorstore.base import Chunk, DocumentRecord, VectorStoreType
from src.vectorstore.redis_store import (
    RedisConfig,
    RedisVectorStore,
    _escape_tag,
    _vec_to_bytes,
)

CONFIG = {
    "url": "redis://127.0.0.1:6379/0",
    "index_name": "test_idx",
    "key_prefix": "pfx",
    "embedding_dim": 3,
}


# --- Fakes -------------------------------------------------------------------

class RedisFakes:
    """A spec'd ``redis.asyncio.Redis`` client plus its non-transactional pipeline."""

    def __init__(self):
        self.pipe = MagicMock(spec=Pipeline)
        self.client = MagicMock(spec=redis.asyncio.Redis)
        # Sync-looking in redis-py but awaitable at runtime on the asyncio client.
        self.client.ping = AsyncMock(return_value=True)
        self.client.delete = AsyncMock(return_value=1)
        self.client.pipeline.return_value = self.pipe
        self.factory = MagicMock(return_value=self.client)

    def commands(self) -> list[tuple]:
        return [c.args for c in self.client.execute_command.call_args_list]


@pytest.fixture
def fakes(monkeypatch) -> RedisFakes:
    f = RedisFakes()
    monkeypatch.setattr(redis.asyncio, "from_url", f.factory)
    return f


@pytest.fixture
async def store(fakes) -> RedisVectorStore:
    s = RedisVectorStore(CONFIG)
    assert await s.connect() is True  # FT.INFO succeeds: index already there
    fakes.client.execute_command.reset_mock()
    return s


def _chunk(cid, text="t", embedding=(0.5, -1.0, 2.0), **meta) -> Chunk:
    return Chunk(
        chunk_id=cid,
        doc_id="docs/a.md",
        text=text,
        metadata=meta,
        embedding=list(embedding) if embedding is not None else None,
    )


def _unpack(blob: bytes) -> list[float]:
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


# --- Helpers & config --------------------------------------------------------

@pytest.mark.parametrize("vec", [[0.5, -1.0, 2.0], [0.0], []])
def test_vec_to_bytes_packs_little_endian_float32(vec):
    blob = _vec_to_bytes(vec)
    assert len(blob) == 4 * len(vec)
    assert _unpack(blob) == vec


def test_vec_to_bytes_is_little_endian():
    assert _vec_to_bytes([1.0]) == b"\x00\x00\x80\x3f"


@pytest.mark.parametrize(
    "value, expected",
    [
        ("plain_ns", "plain_ns"),
        ("docs/a.md", "docs\\/a\\.md"),
        ("my ns", "my\\ ns"),
        ("a-b:c", "a\\-b\\:c"),
        ("{x}@y", "\\{x\\}\\@y"),
        ("back\\slash", "back\\\\slash"),
        ("perché", "perché"),
        ("", ""),
    ],
)
def test_escape_tag(value, expected):
    assert _escape_tag(value) == expected


def test_config_defaults_and_unknown_keys_ignored():
    cfg = RedisConfig.model_validate({"url": "redis://h", "bogus": 1})
    assert cfg.index_name == "ws_mark_flow_chunks_idx"
    assert cfg.key_prefix == "ws_mark_flow_chunk"
    assert cfg.embedding_dim == 1536
    assert (cfg.similarity, cfg.algorithm) == ("cosine", "HNSW")
    assert not hasattr(cfg, "bogus")


def test_config_requires_url():
    with pytest.raises(ValidationError):
        RedisVectorStore({})


def test_store_type_is_redis():
    assert RedisVectorStore(CONFIG).store_type is VectorStoreType.REDIS


# --- Connection / index ------------------------------------------------------

async def test_connect_reuses_existing_index(fakes):
    assert await RedisVectorStore(CONFIG).connect() is True

    fakes.factory.assert_called_once_with("redis://127.0.0.1:6379/0", decode_responses=False)
    fakes.client.ping.assert_awaited_once()
    assert fakes.commands() == [("FT.INFO", "test_idx")]


def _ft_create(fakes) -> list:
    create = [args for args in fakes.commands() if args[0] == "FT.CREATE"]
    assert len(create) == 1
    return list(create[0])


async def test_connect_creates_index_when_missing(fakes):
    fakes.client.execute_command.side_effect = [ResponseError("Unknown index name"), b"OK"]

    assert await RedisVectorStore(CONFIG).connect() is True

    args = _ft_create(fakes)
    assert args[:7] == ["FT.CREATE", "test_idx", "ON", "HASH", "PREFIX", "1", "pfx:"]
    schema = args[args.index("SCHEMA") + 1:]
    for tag in ("namespace", "doc_id", "source_hash", "embedding_model", "chunking_version"):
        i = schema.index(tag)
        assert schema[i + 1:i + 4] == ["TAG", "SEPARATOR", "|"]
    assert schema[schema.index("text") + 1] == "TEXT"
    vec = schema[schema.index("embedding"):]
    assert vec == ["embedding", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                   "DIM", "3", "DISTANCE_METRIC", "COSINE"]


@pytest.mark.parametrize(
    "similarity, algorithm, metric, algo",
    [
        ("cosine", "HNSW", "COSINE", "HNSW"),
        ("L2", "flat", "L2", "FLAT"),
        ("ip", "Flat", "IP", "FLAT"),
        ("manhattan", "ivf", "COSINE", "HNSW"),
    ],
)
async def test_index_metric_and_algorithm_mapping(fakes, similarity, algorithm, metric, algo):
    fakes.client.execute_command.side_effect = [ResponseError("Unknown index name"), b"OK"]
    store = RedisVectorStore({**CONFIG, "similarity": similarity, "algorithm": algorithm})

    assert await store.connect() is True

    args = _ft_create(fakes)
    assert args[args.index("VECTOR") + 1] == algo
    assert args[args.index("DISTANCE_METRIC") + 1] == metric


async def test_connect_returns_false_without_redis_package(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)
    with caplog.at_level(logging.ERROR):
        assert await RedisVectorStore(CONFIG).connect() is False
    assert "redis-py not installed" in caplog.text


async def test_connect_returns_false_when_ping_fails(fakes, caplog):
    fakes.client.ping.side_effect = RedisConnectionError("refused")
    with caplog.at_level(logging.ERROR):
        assert await RedisVectorStore(CONFIG).connect() is False
    assert "Redis connect failed" in caplog.text
    fakes.client.execute_command.assert_not_awaited()


async def test_connect_returns_false_when_index_cannot_be_created(fakes):
    # e.g. plain Redis without the RediSearch module.
    fakes.client.execute_command.side_effect = ResponseError("unknown command 'FT.INFO'")
    assert await RedisVectorStore(CONFIG).connect() is False


async def test_context_manager_raises_on_failed_connect(fakes):
    fakes.factory.side_effect = ValueError("bad url")
    with pytest.raises(RuntimeError, match="redis"):
        async with RedisVectorStore(CONFIG):
            pytest.fail("body must not run")  # pragma: no cover


async def test_disconnect_closes_client_once(store, fakes):
    await store.disconnect()
    await store.disconnect()
    fakes.client.aclose.assert_awaited_once()


async def test_disconnect_swallows_close_errors(store, fakes):
    fakes.client.aclose.side_effect = RedisConnectionError("gone")
    await store.disconnect()
    await store.disconnect()
    fakes.client.aclose.assert_awaited_once()


async def test_disconnect_without_connect_is_a_noop():
    await RedisVectorStore(CONFIG).disconnect()


# --- list_documents ----------------------------------------------------------

async def test_list_documents_aggregates_by_doc_within_namespace(store, fakes):
    fakes.client.execute_command.return_value = [
        3,
        [b"doc_id", b"a.md", b"chunk_count", b"2", b"source_hash", b"h1",
         b"embedding_model", b"m", b"chunking_version", b"v1"],
        ["doc_id", "b.md", "chunk_count", 5, "source_hash", "",
         "embedding_model", "", "chunking_version", ""],
        [b"doc_id", b"", b"chunk_count", b"7"],
        [b"chunk_count", b""],
    ]

    docs = await store.list_documents("docs/team a")

    args = fakes.commands()[0]
    assert args[:3] == ("FT.AGGREGATE", "test_idx", "@namespace:{docs\\/team\\ a}")
    assert args[3:6] == ("GROUPBY", "1", "@doc_id")
    for field in ("source_hash", "embedding_model", "chunking_version"):
        assert f"@{field}" in args
    assert docs == {
        "a.md": DocumentRecord(doc_id="a.md", source_hash="h1", embedding_model="m",
                               chunking_version="v1", chunk_count=2),
        "b.md": DocumentRecord(doc_id="b.md", chunk_count=5),
    }


@pytest.mark.parametrize("response", [None, [], [0]])
async def test_list_documents_empty_responses(store, fakes, response):
    fakes.client.execute_command.return_value = response
    assert await store.list_documents("ns") == {}


async def test_list_documents_returns_empty_on_search_error(store, fakes, caplog):
    fakes.client.execute_command.side_effect = ResponseError("no such index")
    with caplog.at_level(logging.ERROR):
        assert await store.list_documents("ns") == {}
    assert "FT.AGGREGATE failed" in caplog.text


# --- upsert_document ---------------------------------------------------------

async def test_upsert_without_chunks_is_a_noop(store, fakes):
    await store.upsert_document("ns", "a.md", [], "h", "m", "v")
    fakes.client.execute_command.assert_not_awaited()
    fakes.client.pipeline.assert_not_called()


async def test_upsert_rejects_chunks_without_embedding_before_writing(store, fakes):
    with pytest.raises(ValueError, match="a#1"):
        await store.upsert_document(
            "ns", "a.md", [_chunk("a#0"), _chunk("a#1", embedding=None)], "h", "m", "v")
    fakes.client.execute_command.assert_not_awaited()
    fakes.client.pipeline.assert_not_called()


async def test_upsert_replaces_previous_chunks_of_the_document(store, fakes):
    fakes.client.execute_command.return_value = [2, b"pfx:ns:a#old0", b"pfx:ns:a#old1"]
    chunks = [_chunk("a#0", "first", (1.0, 0.0, 0.5), section="intro"), _chunk("a#1", "")]

    await store.upsert_document("ns", "docs/a.md", chunks, "hash1", "emb", "v2")

    scan = fakes.commands()[0]
    assert scan[:3] == ("FT.SEARCH", "test_idx", "@namespace:{ns} @doc_id:{docs\\/a\\.md}")
    assert "NOCONTENT" in scan

    # Delete of the old keys is queued before the new hashes are written.
    names = [c[0] for c in fakes.pipe.method_calls]
    assert names == ["delete", "hset", "hset", "execute"]
    fakes.pipe.delete.assert_called_once_with(b"pfx:ns:a#old0", b"pfx:ns:a#old1")
    first, second = fakes.pipe.hset.call_args_list
    assert first.args == ("pfx:ns:a#0",)
    mapping = first.kwargs["mapping"]
    assert {k: v for k, v in mapping.items() if k != "embedding"} == {
        "namespace": "ns",
        "doc_id": "docs/a.md",
        "source_hash": "hash1",
        "embedding_model": "emb",
        "chunking_version": "v2",
        "text": "first",
        "metadata": json.dumps({"section": "intro"}),
    }
    assert _unpack(mapping["embedding"]) == [1.0, 0.0, 0.5]
    assert second.args == ("pfx:ns:a#1",)
    assert second.kwargs["mapping"]["text"] == ""
    assert json.loads(second.kwargs["mapping"]["metadata"]) == {}
    fakes.pipe.execute.assert_awaited_once()


@pytest.mark.parametrize(
    "scan", [[0], None, ResponseError("index missing")], ids=["no-hits", "none", "error"]
)
async def test_upsert_of_new_document_only_writes(store, fakes, scan):
    if isinstance(scan, Exception):
        fakes.client.execute_command.side_effect = scan
    else:
        fakes.client.execute_command.return_value = scan

    await store.upsert_document("ns", "a.md", [_chunk("a#0")], "h", "m", "v")

    fakes.pipe.delete.assert_not_called()
    fakes.pipe.hset.assert_called_once()
    fakes.pipe.execute.assert_awaited_once()


async def test_upsert_keys_are_namespaced(store, fakes):
    fakes.client.execute_command.return_value = [0]
    await store.upsert_document("tenant-a", "a.md", [_chunk("a#0")], "h", "m", "v")
    await store.upsert_document("tenant-b", "a.md", [_chunk("a#0")], "h", "m", "v")
    keys = [c.args[0] for c in fakes.pipe.hset.call_args_list]
    assert keys == ["pfx:tenant-a:a#0", "pfx:tenant-b:a#0"]
    namespaces = [c.kwargs["mapping"]["namespace"] for c in fakes.pipe.hset.call_args_list]
    assert namespaces == ["tenant-a", "tenant-b"]


# --- delete_document ---------------------------------------------------------

async def test_delete_document_removes_all_doc_keys(store, fakes):
    fakes.client.execute_command.return_value = [2, b"pfx:ns:a#0", b"pfx:ns:a#1"]

    assert await store.delete_document("ns", "a.md") is True

    assert fakes.commands()[0][2] == "@namespace:{ns} @doc_id:{a\\.md}"
    fakes.client.delete.assert_awaited_once_with(b"pfx:ns:a#0", b"pfx:ns:a#1")


@pytest.mark.parametrize("scan", [[0], None, ResponseError("boom")], ids=["no-hits", "none", "error"])
async def test_delete_document_of_unknown_doc_returns_false(store, fakes, scan):
    if isinstance(scan, Exception):
        fakes.client.execute_command.side_effect = scan
    else:
        fakes.client.execute_command.return_value = scan

    assert await store.delete_document("ns", "a.md") is False
    fakes.client.delete.assert_not_awaited()


# --- query -------------------------------------------------------------------

def _hit(key, doc_id=b"a.md", text=b"txt", metadata=b"{}", score=b"0.25"):
    fields = []
    for name, value in (("doc_id", doc_id), ("text", text), ("metadata", metadata), ("score", score)):
        if value is not None:
            fields += [name.encode(), value]
    return [key, fields]


def _response(*hits):
    return [len(hits)] + [item for hit in hits for item in hit]


async def test_query_builds_namespace_scoped_knn_search(store, fakes):
    await store.query("docs/team a", [0.5, -1.0, 2.0], k=3)

    args = list(fakes.commands()[0])
    assert args[:3] == [
        "FT.SEARCH", "test_idx", "(@namespace:{docs\\/team\\ a})=>[KNN 3 @embedding $vec AS score]"]
    params = args[args.index("PARAMS"):args.index("PARAMS") + 4]
    assert params[:3] == ["PARAMS", "2", "vec"]
    assert _unpack(params[3]) == [0.5, -1.0, 2.0]
    assert args[args.index("SORTBY") + 1] == "score"
    assert args[args.index("LIMIT") + 1:args.index("LIMIT") + 3] == ["0", "3"]
    assert args[-2:] == ["DIALECT", "2"]
    ret = args.index("RETURN")
    n = int(args[ret + 1])
    assert set(args[ret + 2:ret + 2 + n]) == {"doc_id", "text", "metadata", "score"}


async def test_query_pushes_tag_filters_into_the_prefilter(store, fakes):
    await store.query("ns", [0.0, 0.0, 1.0], k=2, filter={"doc_id": "a.md", "source_hash": "h"})
    args = fakes.commands()[0]
    assert args[2] == "(@namespace:{ns} @doc_id:{a\\.md} @source_hash:{h})=>[KNN 2 @embedding $vec AS score]"
    assert args[args.index("LIMIT") + 2] == "2"


@pytest.mark.parametrize("k, over", [(1, 20), (5, 20), (6, 24)])
async def test_query_overfetches_when_post_filtering_metadata(store, fakes, k, over):
    await store.query("ns", [0.0, 0.0, 1.0], k=k, filter={"lang": "it"})
    args = fakes.commands()[0]
    assert args[2] == f"(@namespace:{{ns}})=>[KNN {over} @embedding $vec AS score]"
    assert args[args.index("LIMIT") + 2] == str(over)


async def test_query_parses_hits(store, fakes):
    fakes.client.execute_command.return_value = _response(
        _hit(b"pfx:ns:a#0", metadata=json.dumps({"section": "intro"}).encode(), score=b"0.125"),
        _hit("pfx:ns:b#0", doc_id=None, text=None, metadata=None, score=None),
        _hit(b"pfx:ns:c#0", metadata=b"{not json", score=b"nan-ish"),
        _hit(b"pfx:ns:d#0", metadata=7, score=0.5),
    )

    hits = await store.query("ns", [0.1, 0.2, 0.3])

    assert hits == [
        (Chunk(chunk_id="a#0", doc_id="a.md", text="txt", metadata={"section": "intro"}), 0.125),
        (Chunk(chunk_id="b#0", doc_id="", text="", metadata={}), 0.0),
        (Chunk(chunk_id="c#0", doc_id="a.md", text="txt", metadata={}), 0.0),
        (Chunk(chunk_id="d#0", doc_id="a.md", text="txt", metadata={}), 0.5),
    ]


async def test_query_handles_unprefixed_keys_and_missing_fields(store, fakes):
    fakes.client.execute_command.return_value = [1, b"orphan"]
    hits = await store.query("ns", [0.1, 0.2, 0.3])
    assert hits == [(Chunk(chunk_id="orphan", doc_id="", text="", metadata={}), 0.0)]


async def test_query_post_filters_metadata_and_stops_at_k(store, fakes):
    fakes.client.execute_command.return_value = _response(
        _hit(b"pfx:ns:a#0", metadata=b'{"lang": "en"}'),
        _hit(b"pfx:ns:a#1", metadata=b'{"lang": "it", "page": 1}'),
        _hit(b"pfx:ns:a#2", metadata=b'{"page": 1}'),
        _hit(b"pfx:ns:a#3", metadata=b'{"lang": "it", "page": "1"}'),
        _hit(b"pfx:ns:a#4", metadata=b'{"lang": "it", "page": 1}'),
    )

    hits = await store.query("ns", [0.1, 0.2, 0.3], k=2, filter={"lang": "it", "page": 1})

    assert [c.chunk_id for c, _ in hits] == ["a#1", "a#3"]


async def test_query_stops_at_k_without_filter(store, fakes):
    fakes.client.execute_command.return_value = _response(
        *[_hit(f"pfx:ns:a#{i}".encode()) for i in range(4)])
    hits = await store.query("ns", [0.1, 0.2, 0.3], k=1)
    assert [c.chunk_id for c, _ in hits] == ["a#0"]


@pytest.mark.parametrize("response", [None, [], [0]])
async def test_query_empty_responses(store, fakes, response):
    fakes.client.execute_command.return_value = response
    assert await store.query("ns", [0.1, 0.2, 0.3]) == []


async def test_query_returns_empty_on_search_error(store, fakes, caplog):
    fakes.client.execute_command.side_effect = ResponseError("Syntax error")
    with caplog.at_level(logging.ERROR):
        assert await store.query("ns", [0.1, 0.2, 0.3]) == []
    assert "KNN query failed" in caplog.text


@pytest.mark.xfail(
    strict=True,
    reason="BUG: query() truncates chunk ids containing ':' (key parsed by splitting on ':')",
)
async def test_query_returns_chunk_ids_containing_colons_intact(store, fakes):
    chunk_id = "notes/meeting 10:30.md#0000#abcdef012345"
    fakes.client.execute_command.side_effect = [
        [0],  # upsert: no previous chunks
        _response(_hit(f"pfx:ns:{chunk_id}".encode())),
    ]
    await store.upsert_document("ns", "notes/meeting 10:30.md", [_chunk(chunk_id)], "h", "m", "v")
    assert fakes.pipe.hset.call_args.args == (f"pfx:ns:{chunk_id}",)

    hits = await store.query("ns", [0.1, 0.2, 0.3])

    assert [c.chunk_id for c, _ in hits] == [chunk_id]
