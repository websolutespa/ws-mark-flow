"""Tests for ``src/graph_extraction.py``: ontology loading, canonical ids and LLM-driven extraction."""
import json
import logging
import sys
import threading
import time

import pytest

from src.config import Settings
from src.graph_extraction import (
    LEXICAL_LABELS,
    GraphExtractionSettings,
    GraphExtractor,
    Ontology,
    OntologyRelation,
    canonical_id,
    extractor_version,
    load_ontology,
)
from src.vectorstore.base import Chunk, ChunkMention, Entity, Relation
from tests.helpers import FakeWire

ONTOLOGY = Ontology(
    node_labels=["Person", "Organization"],
    relations=[OntologyRelation(type="WORKS_AT", source=["Person"], target=["Organization"])],
    node_properties={"Person": ["role"]},
)

ONTOLOGY_YAML = """\
node_labels: [Person, Organization]
relations:
  - type: WORKS_AT
    source: [Person]
    target: [Organization]
node_properties:
  Person: [role]
"""


def _chunk(index: int, text: str) -> Chunk:
    return Chunk(chunk_id=f"doc.md#{index}", doc_id="doc.md", text=text)


def _chunk_text(prompt: str) -> str:
    return prompt.rsplit('TEXT:\n"""\n', 1)[1].rsplit('\n"""', 1)[0]


class ScriptedLLM:
    """Replaces ``GraphExtractor._call_llm``: answers per chunk text."""

    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    def __call__(self, prompt, provider, model, api_key, base_url):
        self.calls.append(
            {"prompt": prompt, "provider": provider, "model": model,
             "api_key": api_key, "base_url": base_url}
        )
        reply = self.replies[_chunk_text(prompt)]
        if isinstance(reply, Exception):
            raise reply
        return reply if isinstance(reply, str) else json.dumps(reply)


@pytest.fixture
def script(monkeypatch):
    def install(replies):
        llm = ScriptedLLM(replies)
        monkeypatch.setattr(GraphExtractor, "_call_llm", staticmethod(llm))
        return llm
    return install


@pytest.fixture
def extractor(settings):
    return GraphExtractor(settings)


def _schema_cfg(**overrides):
    values = {"enabled": True, "mode": "schema_guided", "ontology": ONTOLOGY, **overrides}
    return GraphExtractionSettings(**values)


# --- Ontology ----------------------------------------------------------------

def test_ontology_fingerprint_is_stable_and_content_sensitive():
    same = Ontology.model_validate(ONTOLOGY.model_dump())
    other = ONTOLOGY.model_copy(update={"node_labels": ["Person"]})
    assert ONTOLOGY.fingerprint() == same.fingerprint()
    assert ONTOLOGY.fingerprint() != other.fingerprint()
    assert ONTOLOGY.fingerprint() != Ontology().fingerprint()


@pytest.mark.parametrize("empty", [None, "", {}])
def test_load_ontology_empty_input_is_unrestricted(empty):
    assert load_ontology(empty) == Ontology()


@pytest.mark.parametrize(
    "source",
    [ONTOLOGY.model_dump(), ONTOLOGY.model_dump_json(), ONTOLOGY_YAML, "  " + ONTOLOGY_YAML],
    ids=["dict", "json", "yaml", "yaml-padded"],
)
def test_load_ontology_inline(source):
    assert load_ontology(source) == ONTOLOGY


@pytest.mark.parametrize(
    ("filename", "content"),
    [("ontology.json", ONTOLOGY.model_dump_json(indent=2)), ("ontology.yaml", ONTOLOGY_YAML)],
)
def test_load_ontology_from_file(tmp_path, filename, content):
    path = tmp_path / filename
    path.write_text(content, encoding="utf-8")
    assert load_ontology(str(path)) == ONTOLOGY


@pytest.mark.parametrize(
    "bad",
    ["node_labels: [unclosed", "just some words", '{"node_labels": 5}', "- a\n- b"],
)
def test_load_ontology_rejects_unparseable_input(bad):
    with pytest.raises(ValueError, match="Could not parse ontology"):
        load_ontology(bad)


@pytest.mark.xfail(
    sys.platform != "win32",
    strict=True,
    reason="BUG: inline ontology > 255 chars is probed as a file path; Path.exists raises ENAMETOOLONG",
)
def test_load_ontology_long_inline_yaml():
    labels = [f"Label{i}" for i in range(60)]
    source = "node_labels:\n  - " + "\n  - ".join(labels)
    assert len(source) > 255
    assert load_ontology(source).node_labels == labels


# --- canonical_id ------------------------------------------------------------

@pytest.mark.parametrize(
    ("label", "name", "expected"),
    [
        ("Person", "Ada Lovelace", "person::ada-lovelace"),
        ("Organization", "  ACME, Inc. ", "organization::acme-inc"),
        ("Product", "GPT-5.4", "product::gpt-5-4"),
        ("Concept", "!!!", "concept::unknown"),
        ("Concept", "", "concept::unknown"),
        ("", "Rome", "entity::rome"),
    ],
)
def test_canonical_id(label, name, expected):
    assert canonical_id(label, name) == expected


def test_canonical_id_ignores_case_and_punctuation_noise():
    assert canonical_id("Person", "Ada  Lovelace") == canonical_id("Person", "ada-lovelace!")


@pytest.mark.xfail(strict=True, reason="BUG: non-ASCII letters are stripped, so distinct names collide")
@pytest.mark.parametrize(("a", "b"), [("北京", "上海"), ("Müller", "Möller")])
def test_canonical_id_keeps_distinct_non_ascii_names_apart(a, b):
    assert canonical_id("Place", a) != canonical_id("Place", b)


# --- extractor_version -------------------------------------------------------

def test_extractor_version_reflects_mode_model_and_ontology():
    lexical = GraphExtractionSettings()
    assert extractor_version(lexical) == "lexical-default-none"
    guided = _schema_cfg(llm_model="gpt-x")
    assert extractor_version(guided) == f"schema_guided-gpt-x-{ONTOLOGY.fingerprint()}"
    changed = _schema_cfg(llm_model="gpt-x", ontology=Ontology(node_labels=["Person"]))
    assert extractor_version(changed) != extractor_version(guided)


@pytest.mark.xfail(strict=True, reason="BUG: extractor_version ignores ontology_source, so ontology edits never re-extract")
def test_extractor_version_changes_with_ontology_source():
    a = GraphExtractionSettings(mode="schema_guided", ontology_source='{"node_labels": ["Person"]}')
    b = GraphExtractionSettings(mode="schema_guided", ontology_source='{"node_labels": ["Place"]}')
    assert extractor_version(a) != extractor_version(b)


# --- extract: short-circuits -------------------------------------------------

@pytest.mark.parametrize(
    ("enabled", "chunks"),
    [(False, [_chunk(0, "Ada works at ACME.")]), (True, [])],
)
async def test_extract_is_a_no_op_when_disabled_or_empty(extractor, script, enabled, chunks):
    llm = script({})
    result = await extractor.extract(chunks, GraphExtractionSettings(enabled=enabled))
    assert (result.entities, result.relations, result.mentions) == ([], [], [])
    assert llm.calls == []


# --- extract: lexical mode ---------------------------------------------------

async def test_lexical_mode_extracts_entities_and_mentions_only(extractor, script):
    text = "Ada Lovelace visited London."
    llm = script({text: {
        "entities": [
            {"name": "Ada Lovelace", "label": "Person", "properties": {"born": 1815, "died": None}},
            {"name": "London", "label": "Place"},
            {"name": "Analytical Engine", "label": "Machine"},  # not a lexical label
            {"name": "", "label": "Person"},
            {"name": "Nobody"},
            "not-a-dict",
        ],
        "relations": [{"source_name": "Ada Lovelace", "target_name": "London", "type": "VISITED"}],
    }})

    result = await extractor.extract([_chunk(0, text)], GraphExtractionSettings(enabled=True))

    assert result.entities == [
        Entity(id="person::ada-lovelace", label="Person", name="Ada Lovelace", properties={"born": "1815"}),
        Entity(id="place::london", label="Place", name="London", properties={}),
    ]
    assert result.mentions == [
        ChunkMention(chunk_id="doc.md#0", entity_id="person::ada-lovelace", span="Ada Lovelace"),
        ChunkMention(chunk_id="doc.md#0", entity_id="place::london", span="London"),
    ]
    assert result.relations == []
    prompt = llm.calls[0]["prompt"]
    assert ", ".join(LEXICAL_LABELS) in prompt
    assert "Do NOT extract relations" in prompt
    assert "ONTOLOGY" not in prompt


async def test_llm_settings_fall_back_to_global_settings(tmp_path, script):
    settings = Settings(
        temp_dir=str(tmp_path), llm_provider="Anthropic", llm_model="claude-x",
        llm_api_key="global-key", llm_base_url="http://localhost:1/v1",
    )
    llm = script({"t": {}})
    await GraphExtractor(settings).extract([_chunk(0, "t")], GraphExtractionSettings(enabled=True))
    assert llm.calls[0] | {"prompt": None} == {
        "prompt": None, "provider": "anthropic", "model": "claude-x",
        "api_key": "global-key", "base_url": "http://localhost:1/v1",
    }


async def test_job_llm_settings_override_global_settings(extractor, script):
    llm = script({"t": {}})
    cfg = GraphExtractionSettings(
        enabled=True, llm_provider="GOOGLE", llm_model="gemini-x",
        llm_api_key="job-key", llm_base_url="http://localhost:2",
    )
    await extractor.extract([_chunk(0, "t")], cfg)
    assert llm.calls[0] | {"prompt": None} == {
        "prompt": None, "provider": "google", "model": "gemini-x",
        "api_key": "job-key", "base_url": "http://localhost:2",
    }


# --- extract: schema-guided mode ---------------------------------------------

async def test_schema_guided_keeps_only_ontology_labels_and_relations(extractor, script):
    text = "Ada works at ACME in London with Bob."
    llm = script({text: {
        "entities": [
            {"name": "Ada", "label": "Person", "properties": {"role": "engineer"}},
            {"name": "ACME", "label": "Organization"},
            {"name": "London", "label": "Place"},  # not in the ontology
            {"name": "Bob", "label": "Person"},
        ],
        "relations": [
            # resolved by (name, label), case-insensitively
            {"source_name": "ada", "source_label": "Person", "target_name": "ACME",
             "target_label": "Organization", "type": "WORKS_AT", "properties": {"since": 2020}},
            # resolved by name alone when the label is missing or wrong
            {"source_name": "Bob", "target_name": "ACME", "target_label": "Company", "type": "WORKS_AT"},
            {"source_name": "Ada", "target_name": "Bob", "type": "KNOWS"},       # type not allowed
            {"source_name": "Ada", "target_name": "London", "type": "WORKS_AT"},  # endpoint dropped
            {"source_name": "Ada", "target_name": "Ada", "type": "WORKS_AT"},     # self loop
            {"source_name": "Ada", "target_name": "ACME"},                        # no type
            ["not", "a", "dict"],
        ],
    }})

    result = await extractor.extract([_chunk(0, text)], _schema_cfg())

    assert [e.id for e in result.entities] == ["person::ada", "organization::acme", "person::bob"]
    assert result.relations == [
        Relation(source_id="person::ada", target_id="organization::acme", type="WORKS_AT",
                 properties={"since": "2020"}),
        Relation(source_id="person::bob", target_id="organization::acme", type="WORKS_AT"),
    ]
    prompt = llm.calls[0]["prompt"]
    assert "ONTOLOGY:" in prompt
    assert '"WORKS_AT"' in prompt
    assert "Do NOT invent new labels" in prompt


async def test_schema_guided_without_relation_types_accepts_any_relation(extractor, script):
    script({"t": {
        "entities": [{"name": "Ada", "label": "Person"}, {"name": "Bob", "label": "Person"}],
        "relations": [{"source_name": "Ada", "target_name": "Bob", "type": "KNOWS"}],
    }})
    cfg = _schema_cfg(ontology=Ontology(node_labels=["Person"]))
    result = await extractor.extract([_chunk(0, "t")], cfg)
    assert [r.type for r in result.relations] == ["KNOWS"]


async def test_per_chunk_limits_are_enforced(extractor, script):
    script({"t": {
        "entities": [{"name": f"P{i}", "label": "Person"} for i in range(5)],
        "relations": [
            {"source_name": "P0", "target_name": f"P{i}", "type": "WORKS_AT"} for i in range(1, 5)
        ],
    }})
    cfg = _schema_cfg(
        ontology=Ontology(node_labels=["Person"], relations=[OntologyRelation(type="WORKS_AT")]),
        max_entities_per_chunk=3, max_relations_per_chunk=1,
    )
    result = await extractor.extract([_chunk(0, "t")], cfg)
    assert [e.name for e in result.entities] == ["P0", "P1", "P2"]
    assert [(r.source_id, r.target_id) for r in result.relations] == [("person::p0", "person::p1")]


async def test_ontology_source_is_loaded_when_no_inline_ontology(extractor, script, tmp_path):
    path = tmp_path / "ontology.yaml"
    path.write_text(ONTOLOGY_YAML, encoding="utf-8")
    llm = script({"t": {"entities": [{"name": "Rome", "label": "Place"}, {"name": "Ada", "label": "Person"}]}})
    cfg = GraphExtractionSettings(enabled=True, mode="schema_guided", ontology_source=str(path))

    result = await extractor.extract([_chunk(0, "t")], cfg)

    assert [e.label for e in result.entities] == ["Person"]
    assert "ONTOLOGY:" in llm.calls[0]["prompt"]


async def test_bad_ontology_source_falls_back_to_lexical(extractor, script, caplog):
    llm = script({"t": {"entities": [{"name": "Rome", "label": "Place"}]}})
    cfg = GraphExtractionSettings(enabled=True, mode="schema_guided", ontology_source="node_labels: [oops")

    with caplog.at_level(logging.ERROR, logger="src.graph_extraction"):
        result = await extractor.extract([_chunk(0, "t")], cfg)

    assert "Ontology load failed" in caplog.text
    assert [e.label for e in result.entities] == ["Place"]
    assert "ONTOLOGY" not in llm.calls[0]["prompt"]


# --- extract: aggregation across chunks --------------------------------------

async def test_entities_and_relations_are_deduplicated_across_chunks(extractor, script):
    relation = {"source_name": "Ada", "target_name": "ACME", "type": "WORKS_AT"}
    script({
        "first": {
            "entities": [{"name": "Ada", "label": "Person", "properties": {"role": "dev"}},
                         {"name": "ACME", "label": "Organization"}],
            "relations": [relation],
        },
        "second": {
            "entities": [{"name": "ADA", "label": "Person", "properties": {"team": "core"}},
                         {"name": "Acme", "label": "Organization"}],
            "relations": [relation],
        },
    })

    result = await extractor.extract([_chunk(0, "first"), _chunk(1, "second")], _schema_cfg())

    assert len(result.entities) == 2
    ada = result.entities[0]
    assert (ada.id, ada.name) == ("person::ada", "Ada")
    assert ada.properties == {"role": "dev", "team": "core"}
    assert len(result.relations) == 1
    assert sorted((m.chunk_id, m.entity_id, m.span) for m in result.mentions) == [
        ("doc.md#0", "organization::acme", "ACME"),
        ("doc.md#0", "person::ada", "Ada"),
        ("doc.md#1", "organization::acme", "Acme"),
        ("doc.md#1", "person::ada", "ADA"),
    ]


async def test_repeated_entity_within_a_chunk_is_emitted_once(extractor, script):
    script({"t": {"entities": [{"name": "Rome", "label": "Place"}, {"name": "ROME", "label": "Place"}]}})
    result = await extractor.extract([_chunk(0, "t")], GraphExtractionSettings(enabled=True))
    assert [(e.id, e.name) for e in result.entities] == [("place::rome", "Rome")]
    assert {m.entity_id for m in result.mentions} == {"place::rome"}


async def test_failing_chunk_is_skipped_and_logged(extractor, script, caplog):
    script({
        "ok": {"entities": [{"name": "Ada", "label": "Person"}]},
        "boom": RuntimeError("rate limited"),
    })

    with caplog.at_level(logging.WARNING, logger="src.graph_extraction"):
        result = await extractor.extract(
            [_chunk(0, "boom"), _chunk(1, "ok")], GraphExtractionSettings(enabled=True),
        )

    assert [e.name for e in result.entities] == ["Ada"]
    assert "Graph extraction failed on chunk doc.md#0: rate limited" in caplog.text


@pytest.mark.parametrize("concurrency", [0, 2])
async def test_chunk_concurrency_is_bounded(extractor, monkeypatch, concurrency):
    lock = threading.Lock()
    state = {"active": 0, "peak": 0}

    def slow_llm(prompt, provider, model, api_key, base_url):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.02)
        with lock:
            state["active"] -= 1
        return "{}"

    monkeypatch.setattr(GraphExtractor, "_call_llm", staticmethod(slow_llm))
    chunks = [_chunk(i, f"c{i}") for i in range(5)]
    await extractor.extract(chunks, GraphExtractionSettings(enabled=True, chunk_concurrency=concurrency))
    assert 1 <= state["peak"] <= max(1, concurrency)


@pytest.mark.xfail(strict=True, reason="BUG: non-dict 'properties' crashes _coerce (isinstance guard is misplaced)")
async def test_non_dict_entity_properties_do_not_abort_extraction(extractor, script):
    script({"t": {"entities": [{"name": "Ada", "label": "Person", "properties": "mathematician"}]}})
    result = await extractor.extract([_chunk(0, "t")], GraphExtractionSettings(enabled=True))
    assert result.entities == [Entity(id="person::ada", label="Person", name="Ada", properties={})]


@pytest.mark.xfail(strict=True, reason="BUG: _coerce runs outside the per-chunk try, so one malformed reply fails the whole batch")
async def test_malformed_reply_shape_only_drops_that_chunk(extractor, script):
    script({
        "bad": {"entities": {"name": "Ada", "label": "Person"}},
        "good": {"entities": [{"name": "Bob", "label": "Person"}]},
    })
    result = await extractor.extract(
        [_chunk(0, "bad"), _chunk(1, "good")], GraphExtractionSettings(enabled=True),
    )
    assert [e.name for e in result.entities] == ["Bob"]


# --- _parse_json -------------------------------------------------------------

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('{"entities": []}', {"entities": []}),
        ('  \n{"a": 1}\n ', {"a": 1}),
        ('```json\n{"a": 1}\n```', {"a": 1}),
        ('```JSON\n{"a": 1}\n```', {"a": 1}),
        ('```\n{"a": 1}\n```', {"a": 1}),
        ('Sure! Here it is:\n{"a": {"b": 2}}\nHope this helps.', {"a": {"b": 2}}),
        ("[1, 2, 3]", {}),
        ('"just a string"', {}),
        ("no json here", {}),
        ("{not: valid}", {}),
        ("", {}),
        (None, {}),
    ],
)
def test_parse_json(raw, expected):
    assert GraphExtractor._parse_json(raw) == expected


# --- _call_llm against the real SDKs -----------------------------------------

@pytest.fixture
def wire(monkeypatch):
    for var in ("OPENAI_BASE_URL", "ANTHROPIC_BASE_URL", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_GENAI_USE_VERTEXAI"):
        monkeypatch.delenv(var, raising=False)
    return FakeWire(monkeypatch)


def _chat_completion(content):
    return {
        "id": "chatcmpl-1", "object": "chat.completion", "created": 0, "model": "gpt-x",
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": content}}],
    }


def _anthropic_message(*blocks):
    return {
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": list(blocks), "stop_reason": "end_turn", "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


async def test_openai_extraction_end_to_end(extractor, wire):
    reply = {"entities": [{"name": "Ada", "label": "Person"}]}
    wire.reply("POST", "/v1/chat/completions", json=_chat_completion(json.dumps(reply)))
    cfg = GraphExtractionSettings(enabled=True, llm_provider="openai", llm_model="gpt-x", llm_api_key="sk-1")

    result = await extractor.extract([_chunk(0, "Ada wrote notes.")], cfg)

    assert [e.id for e in result.entities] == ["person::ada"]
    [request] = wire.requests
    assert request.url == "https://api.openai.com/v1/chat/completions"
    assert request.headers["authorization"] == "Bearer sk-1"
    body = request.json
    assert body["model"] == "gpt-x"
    assert body["response_format"] == {"type": "json_object"}
    assert body["temperature"] == 0.0
    assert [m["role"] for m in body["messages"]] == ["system", "user"]
    assert "Ada wrote notes." in body["messages"][1]["content"]


@pytest.mark.parametrize(("content", "expected"), [(None, "{}"), ('{"a": 1}', '{"a": 1}')])
def test_ollama_call_uses_placeholder_key_and_base_url(wire, content, expected):
    wire.reply("POST", "/v1/chat/completions", json=_chat_completion(content))
    raw = GraphExtractor._call_llm("prompt", "ollama", "llama3", "", "http://localhost:11434/v1")
    assert raw == expected
    [request] = wire.requests
    assert request.url == "http://localhost:11434/v1/chat/completions"
    assert request.headers["authorization"] == "Bearer ollama"


def test_anthropic_call_joins_text_blocks(wire):
    wire.reply("POST", "/v1/messages", json=_anthropic_message(
        {"type": "text", "text": '{"entities": '},
        {"type": "tool_use", "id": "t1", "name": "noop", "input": {}},
        {"type": "text", "text": "[]}"},
    ))

    raw = GraphExtractor._call_llm("the prompt", "anthropic", "claude-x", "ak-1", "")

    assert raw == '{"entities": []}'
    [request] = wire.requests
    assert request.api_key == "ak-1"
    assert request.json["model"] == "claude-x"
    assert request.json["messages"] == [{"role": "user", "content": "the prompt"}]
    assert "strict JSON" in request.json["system"]


def test_anthropic_call_without_text_returns_empty_object(wire):
    wire.reply("POST", "/v1/messages", json=_anthropic_message())
    assert GraphExtractor._call_llm("p", "anthropic", "claude-x", "ak-1", "") == "{}"


@pytest.mark.parametrize(("text", "expected"), [('{"a": 1}', '{"a": 1}'), (None, "{}")])
def test_google_call_requests_json(wire, text, expected):
    parts = [{"text": text}] if text is not None else []
    wire.reply("POST", "gemini-x:generateContent", json={
        "candidates": [{"content": {"role": "model", "parts": parts}}],
    })

    raw = GraphExtractor._call_llm("the prompt", "google", "gemini-x", "g-key", "")

    assert raw == expected
    [request] = wire.requests
    assert request.headers["x-goog-api-key"] == "g-key"
    assert request.json["generationConfig"]["responseMimeType"] == "application/json"
    assert request.json["contents"][0]["parts"][0]["text"] == "the prompt"


async def test_unsupported_provider_yields_no_graph(extractor, wire, caplog):
    cfg = GraphExtractionSettings(enabled=True, llm_provider="cohere")
    with caplog.at_level(logging.WARNING, logger="src.graph_extraction"):
        result = await extractor.extract([_chunk(0, "t")], cfg)
    assert result.entities == []
    assert "Unsupported LLM provider for extraction: cohere" in caplog.text
    assert wire.requests == []
