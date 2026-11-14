"""Tests for ``src/vectorstore/neo4j_store.py`` with the async Neo4j driver mocked out."""
import logging
import sys
from unittest.mock import MagicMock

import neo4j
import pytest
from neo4j import AsyncDriver, AsyncResult, AsyncSession, AsyncTransaction, Record
from neo4j.exceptions import ClientError, ServiceUnavailable, TransactionError
from pydantic import ValidationError

from src.vectorstore.base import (
    Chunk,
    ChunkMention,
    DocumentRecord,
    Entity,
    Relation,
    VectorStoreType,
)
from src.vectorstore.neo4j_store import Neo4jConfig, Neo4jVectorStore, _safe_ident

CONFIG = {
    "uri": "bolt://127.0.0.1:7687",
    "username": "svc",
    "password": "s3cret",
    "database": "graphdb",
    "embedding_dim": 3,
    "index_name": "test_idx",
}


# --- Fakes -------------------------------------------------------------------

def _result(rows=(), single=None) -> MagicMock:
    """An ``AsyncResult`` yielding ``rows`` (dicts turned into Records)."""
    res = MagicMock(spec=AsyncResult)
    res.__aiter__.return_value = [Record(r) for r in rows]
    res.single.return_value = Record(single) if single is not None else None
    return res


class Neo4jFakes:
    """Driver -> session -> transaction chain, all spec'd on the real classes."""

    def __init__(self):
        self.tx = MagicMock(spec=AsyncTransaction)
        self.tx.__aenter__.return_value = self.tx
        self.session = MagicMock(spec=AsyncSession)
        self.session.__aenter__.return_value = self.session
        self.session.begin_transaction.return_value = self.tx
        self.session.run.return_value = _result()
        self.driver = MagicMock(spec=AsyncDriver)
        self.driver.session.return_value = self.session
        self.factory = MagicMock(return_value=self.driver)


def _cypher(call) -> str:
    """Whitespace-normalised Cypher text of a recorded ``run`` call."""
    return " ".join(call.args[0].split())


def _tx_queries(fakes) -> list[str]:
    return [_cypher(c) for c in fakes.tx.run.call_args_list]


def _neo4j_storable(value) -> bool:
    """Neo4j node/relationship properties: primitives or homogeneous lists of them."""
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return True
    if isinstance(value, list):
        return all(
            v is not None and not isinstance(v, (list, dict)) and _neo4j_storable(v)
            for v in value
        )
    return False


@pytest.fixture
def fakes(monkeypatch) -> Neo4jFakes:
    f = Neo4jFakes()
    monkeypatch.setattr(neo4j.AsyncGraphDatabase, "driver", f.factory)
    return f


@pytest.fixture
async def store(fakes) -> Neo4jVectorStore:
    s = Neo4jVectorStore(CONFIG)
    assert await s.connect() is True
    fakes.session.run.reset_mock()
    fakes.driver.session.reset_mock()
    return s


def _chunk(cid, text="t", embedding=(0.1, 0.2, 0.3), **meta) -> Chunk:
    return Chunk(
        chunk_id=cid,
        doc_id="docs/a.md",
        text=text,
        metadata=meta,
        embedding=list(embedding) if embedding is not None else None,
    )


# --- Helpers & config --------------------------------------------------------

@pytest.mark.parametrize(
    "name, expected",
    [
        ("WORKS_AT", "WORKS_AT"),
        ("Person", "Person"),
        ("a1_b2", "a1_b2"),
        ("1ABC", "FALLBACK"),
        ("_private", "FALLBACK"),
        ("HAS-DASH", "FALLBACK"),
        ("X]->(y) DETACH DELETE y //", "FALLBACK"),
        ("", "FALLBACK"),
        (None, "FALLBACK"),
    ],
)
def test_safe_ident_only_lets_plain_identifiers_through(name, expected):
    assert _safe_ident(name, "FALLBACK") == expected


def test_config_defaults_and_unknown_keys_ignored():
    cfg = Neo4jConfig.model_validate({"uri": "bolt://h", "password": "p", "bogus": 1})
    assert cfg.username == "neo4j"
    assert cfg.database == "neo4j"
    assert cfg.embedding_dim == 1536
    assert cfg.index_name == "ws_mark_flow_chunk_embeddings"
    assert cfg.similarity == "cosine"
    assert not hasattr(cfg, "bogus")


@pytest.mark.parametrize("missing", ["uri", "password"])
def test_config_requires_uri_and_password(missing):
    data = {"uri": "bolt://h", "password": "p"}
    del data[missing]
    with pytest.raises(ValidationError):
        Neo4jVectorStore(data)


def test_store_type_is_neo4j():
    assert Neo4jVectorStore(CONFIG).store_type is VectorStoreType.NEO4J


# --- Connection / schema -----------------------------------------------------

async def test_connect_opens_driver_and_creates_schema(fakes):
    store = Neo4jVectorStore(CONFIG)

    assert await store.connect() is True

    fakes.factory.assert_called_once_with("bolt://127.0.0.1:7687", auth=("svc", "s3cret"))
    fakes.driver.verify_connectivity.assert_awaited_once()
    fakes.driver.session.assert_called_with(database="graphdb")
    queries = [_cypher(c) for c in fakes.session.run.call_args_list]
    assert len(queries) == 6
    assert all("IF NOT EXISTS" in q for q in queries)
    assert sum(q.startswith("CREATE CONSTRAINT") for q in queries) == 3
    assert "FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE" in queries[0]
    assert "REQUIRE (d.namespace, d.doc_id) IS UNIQUE" in queries[1]
    assert "REQUIRE (e.namespace, e.id) IS UNIQUE" in queries[2]
    assert sum(q.startswith("CREATE INDEX") for q in queries) == 2
    vector = queries[-1]
    assert vector.startswith("CREATE VECTOR INDEX test_idx IF NOT EXISTS FOR (c:Chunk) ON (c.embedding)")
    assert fakes.session.run.call_args_list[-1].kwargs == {"dim": 3, "sim": "cosine"}


async def test_connect_returns_false_when_driver_missing(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "neo4j", None)
    with caplog.at_level(logging.ERROR):
        assert await Neo4jVectorStore(CONFIG).connect() is False
    assert "not installed" in caplog.text


async def test_connect_returns_false_when_server_unreachable(fakes, caplog):
    fakes.driver.verify_connectivity.side_effect = ServiceUnavailable("down")
    store = Neo4jVectorStore(CONFIG)
    with caplog.at_level(logging.ERROR):
        assert await store.connect() is False
    assert "Neo4j connect failed" in caplog.text
    fakes.session.run.assert_not_called()


async def test_connect_returns_false_when_schema_creation_fails(fakes):
    fakes.session.run.side_effect = ClientError("no permission")
    assert await Neo4jVectorStore(CONFIG).connect() is False


async def test_context_manager_raises_on_failed_connect(fakes):
    fakes.factory.side_effect = ValueError("bad uri")
    with pytest.raises(RuntimeError, match="neo4j"):
        async with Neo4jVectorStore(CONFIG):
            pytest.fail("body must not run")  # pragma: no cover


async def test_disconnect_closes_driver_once(store, fakes):
    await store.disconnect()
    await store.disconnect()
    fakes.driver.close.assert_awaited_once()


async def test_disconnect_swallows_close_errors(store, fakes):
    fakes.driver.close.side_effect = ServiceUnavailable("gone")
    await store.disconnect()
    await store.disconnect()
    fakes.driver.close.assert_awaited_once()


async def test_disconnect_without_connect_is_a_noop():
    await Neo4jVectorStore(CONFIG).disconnect()


# --- list_documents ----------------------------------------------------------

async def test_list_documents_is_scoped_by_namespace_and_maps_rows(store, fakes):
    fakes.session.run.return_value = _result([
        {"doc_id": "a.md", "source_hash": "h1", "embedding_model": "m",
         "chunking_version": "v1", "chunk_count": 3},
        {"doc_id": None, "source_hash": "x", "embedding_model": "x",
         "chunking_version": "x", "chunk_count": 9},
        {"doc_id": "b.md", "source_hash": None, "embedding_model": None,
         "chunking_version": None, "chunk_count": None},
    ])

    docs = await store.list_documents("tenant-a")

    call = fakes.session.run.call_args
    assert call.kwargs == {"ns": "tenant-a"}
    assert "MATCH (c:Chunk {namespace: $ns})" in _cypher(call)
    fakes.driver.session.assert_called_with(database="graphdb")
    assert docs == {
        "a.md": DocumentRecord(doc_id="a.md", source_hash="h1", embedding_model="m",
                               chunking_version="v1", chunk_count=3),
        "b.md": DocumentRecord(doc_id="b.md", chunk_count=0),
    }


async def test_list_documents_empty_namespace(store, fakes):
    assert await store.list_documents("empty") == {}


# --- upsert_document ---------------------------------------------------------

async def test_upsert_without_chunks_does_not_touch_the_database(store, fakes):
    await store.upsert_document("ns", "a.md", [], "h", "m", "v")
    fakes.driver.session.assert_not_called()


async def test_upsert_rejects_chunks_without_embedding_before_writing(store, fakes):
    chunks = [_chunk("a#0"), _chunk("a#1", embedding=None)]
    with pytest.raises(ValueError, match="a#1"):
        await store.upsert_document("ns", "a.md", chunks, "h", "m", "v")
    fakes.session.begin_transaction.assert_not_called()


async def test_upsert_replaces_old_chunks_in_one_transaction(store, fakes):
    chunks = [
        _chunk("a#0", "first", (1.0, 0.0, 0.0), section="intro"),
        _chunk("a#1", "second", (0.0, 1.0, 0.0)),
        _chunk("a#2", "third", (0.0, 0.0, 1.0)),
    ]

    await store.upsert_document("ns", "a.md", chunks, "hash1", "emb-model", "v2")

    fakes.session.begin_transaction.assert_awaited_once()
    calls = fakes.tx.run.call_args_list
    queries = _tx_queries(fakes)
    assert len(calls) == 4

    # 1. Old chunks of *this* doc in *this* namespace are dropped first.
    assert "MATCH (c:Chunk {namespace: $ns, doc_id: $doc_id}) DETACH DELETE c" in queries[0]
    assert calls[0].kwargs == {"ns": "ns", "doc_id": "a.md"}
    # 2. Document node merged with the idempotency fields.
    assert queries[1].startswith("MERGE (d:Document {namespace: $ns, doc_id: $doc_id})")
    assert calls[1].kwargs == {
        "ns": "ns", "doc_id": "a.md", "source_hash": "hash1",
        "embedding_model": "emb-model", "chunking_version": "v2",
    }
    # 3. Chunks created, linked, and vectors stored with the index helper.
    assert "CREATE (c:Chunk {" in queries[2]
    assert "MERGE (d)-[:HAS_CHUNK {order: row.order}]->(c)" in queries[2]
    assert "db.create.setNodeVectorProperty(c, 'embedding', row.embedding)" in queries[2]
    create = calls[2].kwargs
    assert (create["ns"], create["doc_id"]) == ("ns", "a.md")
    assert (create["source_hash"], create["embedding_model"], create["chunking_version"]) == (
        "hash1", "emb-model", "v2")
    assert [(r["chunk_id"], r["text"], r["order"], r["embedding"]) for r in create["rows"]] == [
        ("a#0", "first", 0, [1.0, 0.0, 0.0]),
        ("a#1", "second", 1, [0.0, 1.0, 0.0]),
        ("a#2", "third", 2, [0.0, 0.0, 1.0]),
    ]
    # 4. NEXT adjacency chain.
    assert "MERGE (a)-[:NEXT]->(b)" in queries[3]
    assert calls[3].kwargs == {"ns": "ns", "doc_id": "a.md"}
    fakes.tx.commit.assert_awaited_once()


async def test_upsert_single_chunk_skips_next_chain(store, fakes):
    await store.upsert_document("ns", "a.md", [_chunk("a#0")], "h", "m", "v")
    queries = _tx_queries(fakes)
    assert len(queries) == 3
    assert not any("NEXT" in q for q in queries)
    fakes.tx.commit.assert_awaited_once()


async def test_upsert_does_not_commit_when_a_statement_fails(store, fakes):
    fakes.tx.run.side_effect = [None, ClientError("constraint violated")]
    with pytest.raises(ClientError):
        await store.upsert_document("ns", "a.md", [_chunk("a#0")], "h", "m", "v")
    fakes.tx.commit.assert_not_awaited()


@pytest.mark.xfail(
    strict=True,
    reason="BUG: chunk metadata dict is stored as a node property; Neo4j rejects map properties",
)
async def test_upsert_chunk_rows_only_hold_storable_property_values(store, fakes):
    await store.upsert_document(
        "ns", "a.md", [_chunk("a#0", section="intro", page=2)], "h", "m", "v")
    row = fakes.tx.run.call_args_list[2].kwargs["rows"][0]
    for key, value in row.items():
        assert _neo4j_storable(value), f"{key}={value!r} cannot be a Neo4j property"


# --- delete_document ---------------------------------------------------------

@pytest.mark.parametrize(
    "single, expected",
    [({"deleted": 4}, True), ({"deleted": 0}, False), ({"deleted": None}, False), (None, False)],
)
async def test_delete_document_reports_whether_nodes_were_removed(store, fakes, single, expected):
    fakes.session.run.return_value = _result(single=single)

    assert await store.delete_document("ns", "a.md") is expected

    call = fakes.session.run.call_args
    assert call.kwargs == {"ns": "ns", "doc_id": "a.md"}
    cypher = _cypher(call)
    assert "(n:Chunk OR n:Document)" in cypher
    assert "n.namespace = $ns AND n.doc_id = $doc_id" in cypher
    assert "DETACH DELETE n" in cypher


# --- query -------------------------------------------------------------------

def _hit(cid, score, doc_id="a.md", text="txt", metadata=None):
    return {"chunk_id": cid, "doc_id": doc_id, "text": text, "metadata": metadata, "score": score}


@pytest.mark.parametrize("k, over", [(1, 20), (5, 20), (6, 24), (10, 40)])
async def test_query_overfetches_and_filters_by_namespace(store, fakes, k, over):
    await store.query("tenant-a", (0.5, 0.25, 0.0), k=k)

    call = fakes.session.run.call_args
    assert call.kwargs == {"index": "test_idx", "over": over, "vec": [0.5, 0.25, 0.0], "ns": "tenant-a"}
    cypher = _cypher(call)
    assert "CALL db.index.vector.queryNodes($index, $over, $vec)" in cypher
    assert "WHERE node.namespace = $ns" in cypher


async def test_query_converts_cosine_similarity_to_distance(store, fakes):
    fakes.session.run.return_value = _result([
        _hit("a#0", 0.75, metadata={"section": "intro"}),
        _hit("b#0", None, doc_id=None, text=None, metadata=None),
    ])

    hits = await store.query("ns", [0.1, 0.2, 0.3])

    assert hits == [
        (Chunk(chunk_id="a#0", doc_id="a.md", text="txt", metadata={"section": "intro"}), 0.25),
        (Chunk(chunk_id="b#0", doc_id="", text="", metadata={}), 1.0),
    ]
    assert all(c.embedding is None for c, _ in hits)


async def test_query_negates_score_for_euclidean(fakes):
    store = Neo4jVectorStore({**CONFIG, "similarity": "euclidean"})
    await store.connect()
    fakes.session.run.return_value = _result([_hit("a#0", 0.8), _hit("a#1", 0.5)])

    hits = await store.query("ns", [0.1, 0.2, 0.3])

    assert [d for _, d in hits] == [-0.8, -0.5]
    assert hits[0][1] < hits[1][1]  # closer match still sorts first


async def test_query_stops_at_k(store, fakes):
    fakes.session.run.return_value = _result([_hit(f"a#{i}", 0.9 - i / 10) for i in range(6)])
    hits = await store.query("ns", [0.1, 0.2, 0.3], k=2)
    assert [c.chunk_id for c, _ in hits] == ["a#0", "a#1"]


async def test_query_post_filters_on_doc_id_and_metadata(store, fakes):
    fakes.session.run.return_value = _result([
        _hit("a#0", 0.9, doc_id="a.md", metadata={"lang": "it", "page": 1}),
        _hit("b#0", 0.9, doc_id="b.md", metadata={"lang": "it", "page": 1}),
        _hit("a#1", 0.8, doc_id="a.md", metadata={"lang": "en", "page": 1}),
        _hit("a#2", 0.7, doc_id="a.md", metadata={"lang": "it", "page": "1"}),
        _hit("a#3", 0.6, doc_id="a.md", metadata=None),
    ])

    hits = await store.query("ns", [0.1, 0.2, 0.3], filter={"doc_id": "a.md", "lang": "it", "page": 1})

    # Metadata values are compared as strings, so 1 and "1" both match.
    assert [c.chunk_id for c, _ in hits] == ["a#0", "a#2"]


async def test_query_ignores_non_map_metadata(store, fakes):
    fakes.session.run.return_value = _result([_hit("a#0", 1.0, metadata=["not", "a", "map"])])
    hits = await store.query("ns", [0.1, 0.2, 0.3])
    assert hits[0][0].metadata == {}


# --- Graph: upsert_graph -----------------------------------------------------

ENTITIES = [
    Entity(id="person:ada", label="Person", name="Ada", properties={"role": "cto"}),
    Entity(id="org:acme", label="Bad Label!", name="ACME"),
]
RELATIONS = [
    Relation(source_id="person:ada", target_id="org:acme", type="WORKS_AT", properties={"since": 2020}),
    Relation(source_id="org:acme", target_id="person:ada", type="EMPLOYS"),
    Relation(source_id="person:ada", target_id="org:acme", type="WORKS_AT"),
    Relation(source_id="person:ada", target_id="org:acme", type="x]->() DELETE n //"),
]
MENTIONS = [
    ChunkMention(chunk_id="a#0", entity_id="person:ada", span="Ada"),
    ChunkMention(chunk_id="a#1", entity_id="org:acme"),
]


async def test_upsert_graph_replaces_mentions_and_merges_entities(store, fakes):
    await store.upsert_graph("ns", "a.md", ENTITIES, RELATIONS, MENTIONS)

    calls = fakes.tx.run.call_args_list
    queries = _tx_queries(fakes)

    # Old MENTIONS of this doc's chunks go first; entities are kept.
    assert "MATCH (c:Chunk {namespace: $ns, doc_id: $doc_id}) -[m:MENTIONS]->(:Entity) DELETE m" in queries[0]
    assert calls[0].kwargs == {"ns": "ns", "doc_id": "a.md"}
    assert not any("DELETE e" in q for q in queries)

    assert "MERGE (e:Entity {namespace: $ns, id: row.id})" in queries[1]
    assert calls[1].kwargs == {"ns": "ns", "rows": [
        {"id": "person:ada", "label": "Person", "name": "Ada", "properties": {"role": "cto"}},
        {"id": "org:acme", "label": "Entity", "name": "ACME", "properties": {}},
    ]}

    rel_calls = {
        q.split("MERGE (a)-[r:")[1].split("]")[0]: c.kwargs
        for q, c in zip(queries, calls) if "MERGE (a)-[r:" in q
    }
    assert rel_calls == {
        "WORKS_AT": {"ns": "ns", "rows": [
            {"src": "person:ada", "tgt": "org:acme", "props": {"since": 2020}},
            {"src": "person:ada", "tgt": "org:acme", "props": {}},
        ]},
        "EMPLOYS": {"ns": "ns", "rows": [{"src": "org:acme", "tgt": "person:ada", "props": {}}]},
        "RELATED_TO": {"ns": "ns", "rows": [{"src": "person:ada", "tgt": "org:acme", "props": {}}]},
    }
    assert all("{namespace: $ns, id: row.src}" in q for q in queries if "MERGE (a)-[r:" in q)
    assert not any("DELETE n" in q for q in queries)

    assert "MERGE (c)-[m:MENTIONS]->(e)" in queries[-1]
    assert "MATCH (e:Entity {namespace: $ns, id: row.entity_id})" in queries[-1]
    assert calls[-1].kwargs == {"ns": "ns", "rows": [
        {"chunk_id": "a#0", "entity_id": "person:ada", "span": "Ada"},
        {"chunk_id": "a#1", "entity_id": "org:acme", "span": None},
    ]}
    assert len(calls) == 6
    fakes.tx.commit.assert_awaited_once()


async def test_upsert_graph_with_nothing_only_clears_mentions(store, fakes):
    await store.upsert_graph("ns", "a.md", [], [], [])
    queries = _tx_queries(fakes)
    assert len(queries) == 1
    assert "DELETE m" in queries[0]
    fakes.tx.commit.assert_awaited_once()


class _StrictTx:
    """Mimics the real driver: after one failed statement the tx is unusable."""

    def __init__(self):
        self.queries: list[str] = []
        self.committed = False
        self._failed = False

    async def run(self, query, **params):
        if self._failed:
            raise TransactionError(self, "Transaction failed")
        if "apoc." in query:
            self._failed = True
            raise ClientError("Unknown function 'apoc.map.merge'")
        self.queries.append(" ".join(query.split()))

    async def commit(self):
        if self._failed:
            raise TransactionError(self, "Transaction failed")
        self.committed = True


@pytest.mark.xfail(
    strict=True,
    reason="BUG: APOC fallback re-runs in the already failed transaction, so it can never succeed",
)
async def test_upsert_graph_works_without_apoc(store, fakes):
    strict_tx = _StrictTx()
    fakes.tx.run.side_effect = strict_tx.run
    fakes.tx.commit.side_effect = strict_tx.commit

    await store.upsert_graph("ns", "a.md", ENTITIES, [], [])

    assert any("MERGE (e:Entity" in q for q in strict_tx.queries)
    assert strict_tx.committed


@pytest.mark.xfail(
    strict=True,
    reason="BUG: entity properties dict is stored as a node property; Neo4j rejects map properties",
)
async def test_upsert_graph_entity_rows_only_hold_storable_property_values(store, fakes):
    await store.upsert_graph("ns", "a.md", ENTITIES[:1], [], [])
    for row in fakes.tx.run.call_args_list[1].kwargs["rows"]:
        for key, value in row.items():
            assert _neo4j_storable(value), f"{key}={value!r} cannot be a Neo4j property"


# --- Graph: delete_graph -----------------------------------------------------

@pytest.mark.parametrize(
    "single, expected", [({"n": 2}, True), ({"n": 0}, False), ({"n": None}, False), (None, False)]
)
async def test_delete_graph_drops_only_this_docs_mentions(store, fakes, single, expected):
    fakes.session.run.return_value = _result(single=single)

    assert await store.delete_graph("ns", "a.md") is expected

    call = fakes.session.run.call_args
    assert call.kwargs == {"ns": "ns", "doc_id": "a.md"}
    cypher = _cypher(call)
    assert "MATCH (c:Chunk {namespace: $ns, doc_id: $doc_id}) -[m:MENTIONS]->(:Entity)" in cypher
    assert "DELETE m" in cypher
    assert "DETACH DELETE" not in cypher


# --- Graph: expand_chunks ----------------------------------------------------

async def test_expand_chunks_without_seeds_skips_the_database(store, fakes):
    assert await store.expand_chunks("ns", []) == []
    fakes.driver.session.assert_not_called()


@pytest.mark.parametrize("hops, depth", [(0, 1), (1, 1), (2, 2), (3, 3), (10, 3), ("2", 2)])
async def test_expand_chunks_clamps_hops(store, fakes, hops, depth):
    await store.expand_chunks("ns", ["a#0"], hops=hops)
    assert f"-[*1..{depth}]-(nbr:Entity)" in _cypher(fakes.session.run.call_args)


async def test_expand_chunks_returns_seed_entities_with_neighbors(store, fakes):
    fakes.session.run.return_value = _result([
        {"s_id": "person:ada", "s_label": "Person", "s_name": "Ada", "s_props": {"role": "cto"},
         "neighbors": [
             {"id": "org:acme", "label": "Organization", "name": "ACME", "properties": {"x": 1}},
             {"id": None, "label": None, "name": None, "properties": None},
             None,
             {"id": "city:rome", "label": None, "name": None, "properties": None},
         ]},
        {"s_id": None, "s_label": "Ghost", "s_name": "x", "s_props": None, "neighbors": []},
        {"s_id": "org:acme", "s_label": None, "s_name": None, "s_props": None, "neighbors": None},
    ])

    out = await store.expand_chunks("tenant", ["a#0", "a#1"], hops=2, limit=7)

    call = fakes.session.run.call_args
    assert call.kwargs == {"ns": "tenant", "chunk_ids": ["a#0", "a#1"], "limit": 7}
    assert "MATCH (c:Chunk {chunk_id: cid, namespace: $ns})" in _cypher(call)
    assert out == [
        (
            Entity(id="person:ada", label="Person", name="Ada", properties={"role": "cto"}),
            [
                Entity(id="org:acme", label="Organization", name="ACME", properties={"x": 1}),
                Entity(id="city:rome", label="Entity", name="city:rome"),
            ],
        ),
        (Entity(id="org:acme", label="Entity", name="org:acme"), []),
    ]


async def test_expand_chunks_default_hops_and_limit(store, fakes):
    await store.expand_chunks("ns", ["a#0"])
    call = fakes.session.run.call_args
    assert call.kwargs["limit"] == 25
    assert "-[*1..1]-" in _cypher(call)


async def test_operations_use_the_configured_database(store, fakes):
    fakes.session.run.return_value = _result(single={"deleted": 1, "n": 1})
    await store.list_documents("ns")
    await store.delete_document("ns", "a.md")
    await store.query("ns", [0.0, 0.0, 1.0])
    await store.delete_graph("ns", "a.md")
    await store.expand_chunks("ns", ["a#0"])
    await store.upsert_document("ns", "a.md", [_chunk("a#0")], "h", "m", "v")
    await store.upsert_graph("ns", "a.md", [], [], [])
    assert fakes.driver.session.call_count == 7
    assert all(c.kwargs == {"database": "graphdb"} for c in fakes.driver.session.call_args_list)
