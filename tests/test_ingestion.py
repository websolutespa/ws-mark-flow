"""
Tests for ``IngestionService``: markdown -> chunks -> embeddings -> vector store.

Sources and vector stores are the in-memory fakes from ``tests.helpers``; the
embedder and graph extractor are recording stand-ins (one test drives the real
OpenAI embeddings client through ``FakeWire``). Chunking runs for real.
"""
import asyncio
import logging
import tempfile
from pathlib import Path

import pytest

from src.chunking import ChunkingParams, chunk_markdown, chunking_version
from src.graph_extraction import (
    GraphExtractionSettings,
    Ontology as ExtractorOntology,
    OntologyRelation as ExtractorOntologyRelation,
    _ExtractionResult,
)
from src.ingestion import IngestionService, _new_chunk_id
from src.integration.base import IntegrationType
from src.models import (
    ChunkingSettings,
    EmbeddingSettings,
    GraphOntology,
    GraphOntologyRelation,
    GraphSettings,
    IngestionFileStatus,
    IngestionJob,
    IntegrationConfig,
    JobStatus,
    VectorStoreConfig,
)
from src.vectorstore.base import (
    ChunkMention,
    Entity,
    GraphCapableVectorStore,
    Relation,
    VectorStoreType,
    hash_text,
)
from tests.helpers import FakeSource, FakeVectorStore, FakeWire

NAMESPACE = "kb"
DEFAULT_VERSION = chunking_version(ChunkingParams())

DOC = "# Guide\n\nIntro paragraph.\n\n## Setup\n\nInstall the tool.\n"
OTHER_DOC = "Just one paragraph without headings."


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #

class RecordingEmbedder:
    """Stand-in for ``EmbeddingService``: one deterministic vector per text."""

    def __init__(self):
        self.calls: list[dict] = []
        self.error: Exception | None = None
        self.drop_last = False
        self.delay = 0.0
        self.active = 0
        self.max_active = 0

    async def embed(self, texts, provider, model, api_key=None, base_url=None):
        self.calls.append(
            {"texts": list(texts), "provider": provider, "model": model,
             "api_key": api_key, "base_url": base_url}
        )
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.error is not None:
                raise self.error
            vectors = [[float(len(text)), float(index)] for index, text in enumerate(texts)]
            return vectors[:-1] if self.drop_last else vectors
        finally:
            self.active -= 1


class RecordingExtractor:
    """Stand-in for ``GraphExtractor``."""

    def __init__(self):
        self.calls: list[tuple[list, GraphExtractionSettings]] = []
        self.error: Exception | None = None
        self.result = _ExtractionResult(
            entities=[
                Entity(id="person:ada", label="Person", name="Ada"),
                Entity(id="org:acme", label="Organization", name="Acme"),
            ],
            relations=[Relation(source_id="person:ada", target_id="org:acme", type="WORKS_AT")],
            mentions=[ChunkMention(chunk_id="c", entity_id="person:ada")],
        )

    async def extract(self, chunks, cfg):
        self.calls.append((list(chunks), cfg))
        if self.error is not None:
            raise self.error
        return self.result


class GraphVectorStore(FakeVectorStore, GraphCapableVectorStore):
    """In-memory vector store that also accepts graph artifacts."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.graphs: dict[tuple[str, str], dict] = {}

    async def upsert_graph(self, namespace, doc_id, entities, relations, mentions):
        self.graphs[(namespace, doc_id)] = {
            "entities": list(entities), "relations": list(relations), "mentions": list(mentions),
        }

    async def delete_graph(self, namespace, doc_id):
        return self.graphs.pop((namespace, doc_id), None) is not None

    async def expand_chunks(self, namespace, chunk_ids, hops=1, limit=25):
        return []


class FailingDeleteStore(FakeVectorStore):
    async def delete_document(self, namespace, doc_id):
        raise RuntimeError("delete refused")


# --------------------------------------------------------------------------- #
# Fixtures / builders
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def system_temp(tmp_path, monkeypatch) -> Path:
    """Keep ``tempfile.mkdtemp`` fallbacks inside the test's tmp_path."""
    path = tmp_path / "system-temp"
    path.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


@pytest.fixture
def embedder() -> RecordingEmbedder:
    return RecordingEmbedder()


@pytest.fixture
def extractor() -> RecordingExtractor:
    return RecordingExtractor()


@pytest.fixture
def service(settings, embedder, extractor, monkeypatch) -> IngestionService:
    svc = IngestionService(settings)
    monkeypatch.setattr(svc, "_embedder", embedder)
    monkeypatch.setattr(svc, "_graph_extractor", extractor)
    return svc


def make_job(**overrides) -> IngestionJob:
    fields = dict(
        name="knowledge base",
        source=IntegrationConfig(type=IntegrationType.LOCAL),
        vector_store=VectorStoreConfig(type=VectorStoreType.CHROMA, namespace=NAMESPACE),
    )
    fields.update(overrides)
    return IngestionJob(**fields)


def by_path(job: IngestionJob) -> dict:
    return {result.source_path: result for result in job.file_results}


def index(store: FakeVectorStore, doc_id: str, *, text: str = "", model: str = "text-embedding-3-small",
          version: str = DEFAULT_VERSION, chunks: int = 1, namespace: str = NAMESPACE) -> None:
    """Seed the store as if ``doc_id`` had been ingested before."""
    store.docs[(namespace, doc_id)] = {
        "chunks": [object()] * chunks,
        "source_hash": hash_text(text),
        "embedding_model": model,
        "chunking_version": version,
    }


# --------------------------------------------------------------------------- #
# Chunk ids
# --------------------------------------------------------------------------- #

class TestChunkId:
    def test_is_deterministic_and_embeds_doc_index_and_content_hash(self):
        chunk_id = _new_chunk_id("docs/a.md", 3, "hello")

        assert chunk_id == _new_chunk_id("docs/a.md", 3, "hello")
        assert chunk_id == f"docs/a.md#0003#{hash_text('hello')[:12]}"

    @pytest.mark.parametrize(
        "other",
        [("docs/b.md", 3, "hello"), ("docs/a.md", 4, "hello"), ("docs/a.md", 3, "hello!")],
    )
    def test_differs_when_doc_position_or_content_differs(self, other):
        assert _new_chunk_id("docs/a.md", 3, "hello") != _new_chunk_id(*other)


# --------------------------------------------------------------------------- #
# Temp directory handling
# --------------------------------------------------------------------------- #

class TestTempDir:
    def test_configured_directory_is_used_when_it_exists(self, service, settings):
        configured = Path(settings.temp_dir) / "ingestion"
        configured.mkdir(parents=True)

        assert service._ensure_temp_dir() == configured

    def test_missing_configured_directory_falls_back_to_a_fresh_temp_dir(self, service, system_temp):
        temp = service._ensure_temp_dir()

        assert temp.is_dir()
        assert temp.parent == system_temp
        assert temp.name.startswith("ingestion_")

    def test_cleanup_removes_the_directory_and_forgets_it(self, service):
        temp = service._ensure_temp_dir()
        (temp / "leftover.md").write_text("x", encoding="utf-8")

        service.cleanup()

        assert not temp.exists()
        assert service._temp_dir is None

    def test_cleanup_is_a_no_op_when_there_is_nothing_to_remove(self, service, settings):
        service.cleanup()
        service._ensure_temp_dir()
        service.cleanup()
        service.cleanup()

        assert service._temp_dir is None
        assert not Path(settings.temp_dir).exists()

    def test_cleanup_errors_are_swallowed(self, service, monkeypatch):
        service._ensure_temp_dir()

        def refuse(path):
            raise PermissionError("locked")

        monkeypatch.setattr("src.ingestion.shutil.rmtree", refuse)

        service.cleanup()

        assert service._temp_dir is None

    def test_a_new_directory_is_created_after_cleanup(self, service):
        first = service._ensure_temp_dir()
        service.cleanup()

        second = service._ensure_temp_dir()

        assert second.is_dir()
        assert second != first


# --------------------------------------------------------------------------- #
# analyze
# --------------------------------------------------------------------------- #

class TestAnalyze:
    async def analyze(self, service, source, store, *, model="emb", version="v1", folder=None):
        return await service.analyze(
            source=source, vector_store=store, namespace=NAMESPACE, embedding_model=model,
            chunk_version=version, source_extensions=[".md"], source_folder=folder,
        )

    async def test_new_documents_need_ingestion(self, service):
        source = FakeSource({"/Docs/Guide.md": b"0123456789"})

        analysis, files = await self.analyze(service, source, FakeVectorStore())

        assert [f.path for f in files] == ["/Docs/Guide.md"]
        assert analysis.to_ingest == [{
            "doc_id": "docs/guide.md",
            "path": "/Docs/Guide.md",
            "name": "Guide.md",
            "size": 10,
            "modified_at": files[0].modified_at.isoformat(),
        }]
        assert analysis.up_to_date == [] and analysis.orphans == []
        assert analysis.source_documents == 1
        assert analysis.indexed_documents == 0
        assert analysis.completion_percentage == 0.0

    async def test_documents_indexed_with_same_model_and_chunker_are_up_to_date(self, service):
        source = FakeSource({"/a.md": b"a", "/b.md": b"b", "/c.md": b"c"})
        store = FakeVectorStore()
        index(store, "a.md", model="emb", version="v1")
        index(store, "b.md", model="old-model", version="v1")
        index(store, "c.md", model="emb", version="v0")

        analysis, _ = await self.analyze(service, source, store)

        assert [e["doc_id"] for e in analysis.up_to_date] == ["a.md"]
        assert sorted(e["doc_id"] for e in analysis.to_ingest) == ["b.md", "c.md"]
        assert analysis.completion_percentage == 33.33

    async def test_indexed_documents_missing_from_the_source_are_orphans(self, service):
        store = FakeVectorStore()
        index(store, "gone.md", chunks=4)
        index(store, "elsewhere.md", namespace="other")

        analysis, _ = await self.analyze(service, FakeSource({"/a.md": b"a"}), store)

        assert analysis.orphans == [{"doc_id": "gone.md", "chunk_count": 4}]
        assert analysis.indexed_documents == 1

    async def test_empty_source_reports_zero_completion(self, service):
        analysis, files = await self.analyze(service, FakeSource(), FakeVectorStore())

        assert files == []
        assert analysis.source_documents == 0
        assert analysis.completion_percentage == 0.0

    async def test_only_the_requested_folder_and_extensions_are_considered(self, service):
        source = FakeSource({"/kb/a.md": b"a", "/kb/b.txt": b"b", "/other/c.md": b"c"})

        analysis, files = await self.analyze(service, source, FakeVectorStore(), folder="kb")

        assert [f.path for f in files] == ["/kb/a.md"]
        assert [e["path"] for e in analysis.to_ingest] == ["/kb/a.md"]


# --------------------------------------------------------------------------- #
# Graph settings translation
# --------------------------------------------------------------------------- #

class TestExtractorSettings:
    def test_every_field_is_carried_over(self):
        stored = GraphSettings(
            enabled=True, mode="schema_guided", ontology_source="onto.yaml",
            llm_provider="anthropic", llm_model="claude", llm_api_key="k", llm_base_url="http://llm",
            max_entities_per_chunk=3, max_relations_per_chunk=2, chunk_concurrency=5,
        )

        runtime = IngestionService._to_extractor_settings(stored)

        assert runtime == GraphExtractionSettings(
            enabled=True, mode="schema_guided", ontology=None, ontology_source="onto.yaml",
            llm_provider="anthropic", llm_model="claude", llm_api_key="k", llm_base_url="http://llm",
            max_entities_per_chunk=3, max_relations_per_chunk=2, chunk_concurrency=5,
        )

    def test_ontology_is_translated_to_the_extractor_model(self):
        stored = GraphSettings(ontology=GraphOntology(
            node_labels=["Person", "Organization"],
            relations=[GraphOntologyRelation(type="WORKS_AT", source=["Person"], target=["Organization"])],
            node_properties={"Person": ["role"]},
        ))

        runtime = IngestionService._to_extractor_settings(stored)

        assert runtime.ontology == ExtractorOntology(
            node_labels=["Person", "Organization"],
            relations=[ExtractorOntologyRelation(type="WORKS_AT", source=["Person"], target=["Organization"])],
            node_properties={"Person": ["role"]},
        )

    def test_empty_ontology_and_blank_mode_get_safe_defaults(self):
        stored = GraphSettings(mode="", ontology=GraphOntology())

        runtime = IngestionService._to_extractor_settings(stored)

        assert runtime.enabled is False
        assert runtime.mode == "lexical"
        assert runtime.ontology == ExtractorOntology()


# --------------------------------------------------------------------------- #
# run
# --------------------------------------------------------------------------- #

class TestRun:
    async def test_documents_are_chunked_embedded_and_upserted(self, service, embedder):
        source = FakeSource({"/kb/Guide.md": DOC.encode(), "/kb/other.md": OTHER_DOC.encode()})
        store = FakeVectorStore()
        job = make_job(embedding=EmbeddingSettings(provider="ollama", model="nomic", api_key="k",
                                                   base_url="http://ollama/v1"))

        returned = await service.run(job, source, store)

        assert returned is job
        assert job.status == JobStatus.COMPLETED
        assert job.started_at is not None and job.completed_at is not None
        results = by_path(job)
        assert {r.status for r in results.values()} == {IngestionFileStatus.COMPLETED}
        assert results["/kb/Guide.md"].doc_id == "kb/guide.md"
        assert results["/kb/Guide.md"].chunk_count == 2
        assert results["/kb/other.md"].chunk_count == 1
        assert all(r.completed_at is not None and r.error_message is None for r in results.values())

        stored = store.docs[(NAMESPACE, "kb/guide.md")]
        assert stored["source_hash"] == hash_text(DOC)
        assert stored["embedding_model"] == "nomic"
        assert stored["chunking_version"] == DEFAULT_VERSION
        expected = chunk_markdown(DOC, ChunkingParams())
        assert [(c.text, c.metadata) for c in stored["chunks"]] == [
            (text, {**meta, "source_path": "/kb/Guide.md", "chunk_index": i})
            for i, (text, meta) in enumerate(expected)
        ]
        assert [c.metadata["headings"] for c in stored["chunks"]] == ["Guide", "Guide > Setup"]
        assert [c.chunk_id for c in stored["chunks"]] == [
            _new_chunk_id("kb/guide.md", i, text) for i, (text, _) in enumerate(expected)
        ]
        assert all(c.doc_id == "kb/guide.md" for c in stored["chunks"])
        assert [c.embedding for c in stored["chunks"]] == [[float(len(t)), float(i)] for i, (t, _) in enumerate(expected)]

        assert {(c["provider"], c["model"], c["api_key"], c["base_url"]) for c in embedder.calls} == {
            ("ollama", "nomic", "k", "http://ollama/v1")
        }
        assert job.stats.total_documents == 2
        assert job.stats.completed_documents == 2
        assert job.stats.total_chunks == 3
        assert job.stats.completion_percentage == 100.0

    async def test_job_chunking_settings_drive_the_chunker(self, service):
        text = "word " * 100  # 500 chars, no headings
        store = FakeVectorStore()
        chunking = ChunkingSettings(strategy="fixed", chunk_size=200, chunk_overlap=0)

        job = await service.run(make_job(chunking=chunking), FakeSource({"/a.md": text.encode()}), store)

        assert by_path(job)["/a.md"].chunk_count == 3
        assert store.docs[(NAMESPACE, "a.md")]["chunking_version"] == chunking_version(
            ChunkingParams(strategy="fixed", chunk_size=200, chunk_overlap=0)
        )

    async def test_rerun_skips_unchanged_documents_without_embedding_again(self, service, embedder):
        source = FakeSource({"/a.md": DOC.encode()})
        store = FakeVectorStore()
        await service.run(make_job(), source, store)
        embedder.calls.clear()

        job = await service.run(make_job(), source, store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.SKIPPED
        assert embedder.calls == []
        assert job.status == JobStatus.COMPLETED
        assert job.stats.skipped_documents == 1

    async def test_changed_content_is_reingested(self, service):
        store = FakeVectorStore()
        await service.run(make_job(), FakeSource({"/a.md": DOC.encode()}), store)

        job = await service.run(make_job(), FakeSource({"/a.md": OTHER_DOC.encode()}), store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED
        assert store.docs[(NAMESPACE, "a.md")]["source_hash"] == hash_text(OTHER_DOC)

    @pytest.mark.parametrize(
        "change",
        [
            {"embedding": EmbeddingSettings(model="text-embedding-3-large")},
            {"chunking": ChunkingSettings(chunk_size=500)},
        ],
        ids=["embedding-model", "chunking"],
    )
    async def test_unchanged_content_is_reingested_when_model_or_chunker_change(self, service, change):
        source = FakeSource({"/a.md": DOC.encode()})
        store = FakeVectorStore()
        await service.run(make_job(), source, store)

        job = await service.run(make_job(**change), source, store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED

    async def test_failed_download_is_reported_per_document(self, service):
        source = FakeSource({"/a.md": DOC.encode(), "/b.md": DOC.encode()}, fail_download=["/b.md"])
        store = FakeVectorStore()

        job = await service.run(make_job(), source, store)

        results = by_path(job)
        assert results["/a.md"].status == IngestionFileStatus.COMPLETED
        assert results["/b.md"].status == IngestionFileStatus.FAILED
        assert results["/b.md"].error_message == "Download failed"
        assert (NAMESPACE, "b.md") not in store.docs
        assert job.status == JobStatus.COMPLETED
        assert job.stats.failed_documents == 1
        assert job.stats.completion_percentage == 50.0

    async def test_document_without_content_is_skipped(self, service, embedder):
        store = FakeVectorStore()

        job = await service.run(make_job(), FakeSource({"/blank.md": b"  \n\n  "}), store)

        result = by_path(job)["/blank.md"]
        assert result.status == IngestionFileStatus.SKIPPED
        assert result.error_message == "No content to chunk"
        assert embedder.calls == []
        assert store.docs == {}

    async def test_invalid_utf8_is_replaced_not_fatal(self, service):
        store = FakeVectorStore()

        job = await service.run(make_job(), FakeSource({"/a.md": b"caf\xe9 au lait"}), store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED
        [chunk] = store.docs[(NAMESPACE, "a.md")]["chunks"]
        assert chunk.text == "caf\ufffd au lait"

    async def test_embedding_count_mismatch_fails_the_document(self, service, embedder):
        embedder.drop_last = True
        store = FakeVectorStore()

        job = await service.run(make_job(), FakeSource({"/a.md": DOC.encode()}), store)

        result = by_path(job)["/a.md"]
        assert result.status == IngestionFileStatus.FAILED
        assert result.error_message == "Embedding count mismatch"
        assert store.docs == {}

    async def test_embedding_error_fails_only_that_document(self, service, embedder, caplog):
        embedder.error = RuntimeError("quota exceeded")
        store = FakeVectorStore()

        with caplog.at_level(logging.ERROR, logger="src.ingestion"):
            job = await service.run(make_job(), FakeSource({"/a.md": DOC.encode()}), store)

        result = by_path(job)["/a.md"]
        assert result.status == IngestionFileStatus.FAILED
        assert result.error_message == "quota exceeded"
        assert job.status == JobStatus.COMPLETED
        assert "Ingestion failed for /a.md" in caplog.text

    async def test_progress_is_reported_after_each_document(self, service):
        source = FakeSource({"/a.md": DOC.encode(), "/b.md": DOC.encode(), "/c.md": b""})
        calls = []

        async def progress(job, processed, total):
            calls.append((job, processed, total, job.stats.total_documents))

        job = await service.run(make_job(), source, FakeVectorStore(), progress_callback=progress)

        assert [(processed, total) for _, processed, total, _ in calls] == [(1, 3), (2, 3), (3, 3)]
        assert all(reported is job and documents == 3 for reported, _, _, documents in calls)

    @pytest.mark.parametrize("batch_size, expected", [(1, 1), (2, 2), (5, 3)])
    async def test_batch_size_bounds_concurrency(self, service, embedder, batch_size, expected):
        embedder.delay = 0.01
        source = FakeSource({f"/{name}.md": DOC.encode() for name in "abc"})

        job = await service.run(make_job(batch_size=batch_size), source, FakeVectorStore())

        assert job.status == JobStatus.COMPLETED
        assert embedder.max_active == expected

    async def test_non_positive_batch_size_still_processes_documents(self, service):
        job = make_job(batch_size=0)

        await asyncio.wait_for(service.run(job, FakeSource({"/a.md": DOC.encode()}), FakeVectorStore()), 5)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED

    async def test_orphans_are_kept_by_default(self, service):
        store = FakeVectorStore()
        index(store, "gone.md")

        await service.run(make_job(), FakeSource({"/a.md": DOC.encode()}), store)

        assert (NAMESPACE, "gone.md") in store.docs

    async def test_orphans_are_deleted_when_requested(self, service):
        store = FakeVectorStore()
        index(store, "gone.md")
        index(store, "gone.md", namespace="other")

        job = await service.run(make_job(delete_orphans=True), FakeSource({"/a.md": DOC.encode()}), store)

        assert job.status == JobStatus.COMPLETED
        assert (NAMESPACE, "gone.md") not in store.docs
        assert (NAMESPACE, "a.md") in store.docs
        assert ("other", "gone.md") in store.docs

    async def test_orphan_delete_failure_is_logged_not_fatal(self, service, caplog):
        store = FailingDeleteStore()
        index(store, "gone.md")

        with caplog.at_level(logging.WARNING, logger="src.ingestion"):
            job = await service.run(make_job(delete_orphans=True), FakeSource({"/a.md": DOC.encode()}), store)

        assert job.status == JobStatus.COMPLETED
        assert "Orphan delete failed for gone.md: delete refused" in caplog.text

    async def test_listing_failure_fails_the_job_and_cleans_up(self, service, settings, caplog):
        configured = Path(settings.temp_dir) / "ingestion"
        configured.mkdir(parents=True)
        source = FakeSource(list_error=ConnectionError("source offline"))

        with caplog.at_level(logging.ERROR, logger="src.ingestion"):
            job = await service.run(make_job(), source, FakeVectorStore())

        assert job.status == JobStatus.FAILED
        assert job.error_message == "source offline"
        assert job.completed_at is None
        assert "Ingestion job failed" in caplog.text
        assert not configured.exists()

    async def test_downloads_land_in_the_temp_dir_which_is_removed_afterwards(self, service, settings):
        configured = Path(settings.temp_dir) / "ingestion"
        configured.mkdir(parents=True)
        seen = []

        class SpySource(FakeSource):
            async def download_file(self, file_info, local_path):
                seen.append(local_path)
                return await super().download_file(file_info, local_path)

        await service.run(make_job(), SpySource({"/kb/deep/a.md": DOC.encode()}), FakeVectorStore())

        assert seen == [configured / "kb" / "deep" / "a.md"]
        assert not configured.exists()
        assert service._temp_dir is None

    async def test_real_openai_embeddings_client(self, settings, extractor, monkeypatch):
        for var in ("OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID"):
            monkeypatch.delenv(var, raising=False)
        wire = FakeWire(monkeypatch)
        wire.reply("POST", "/embeddings", json={
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": [0.25, 0.5, 0.75]}],
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 3, "total_tokens": 3},
        })
        svc = IngestionService(settings)
        monkeypatch.setattr(svc, "_graph_extractor", extractor)
        store = FakeVectorStore()
        job = make_job(embedding=EmbeddingSettings(api_key="sk-job"))

        await svc.run(job, FakeSource({"/a.md": OTHER_DOC.encode()}), store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED
        [chunk] = store.docs[(NAMESPACE, "a.md")]["chunks"]
        assert chunk.embedding == [0.25, 0.5, 0.75]
        [request] = wire.requests
        assert request.url == "https://api.openai.com/v1/embeddings"
        assert request.headers["authorization"] == "Bearer sk-job"
        assert request.json["model"] == "text-embedding-3-small"
        assert request.json["input"] == [OTHER_DOC]


class TestRunWithGraph:
    async def test_entities_and_relations_are_stored_for_graph_capable_stores(self, service, extractor):
        store = GraphVectorStore()
        graph = GraphSettings(enabled=True, mode="schema_guided", llm_model="gpt-x", chunk_concurrency=3)

        job = await service.run(make_job(graph=graph), FakeSource({"/a.md": DOC.encode()}), store)

        result = by_path(job)["/a.md"]
        assert result.status == IngestionFileStatus.COMPLETED
        assert (result.entity_count, result.relation_count) == (2, 1)
        assert (job.stats.total_entities, job.stats.total_relations) == (2, 1)
        [(chunks, cfg)] = extractor.calls
        assert chunks == store.docs[(NAMESPACE, "a.md")]["chunks"]
        assert cfg == IngestionService._to_extractor_settings(graph)
        assert store.graphs[(NAMESPACE, "a.md")] == {
            "entities": extractor.result.entities,
            "relations": extractor.result.relations,
            "mentions": extractor.result.mentions,
        }

    async def test_nothing_is_written_when_no_entities_are_found(self, service, extractor):
        extractor.result = _ExtractionResult(entities=[], relations=[], mentions=[])
        store = GraphVectorStore()

        job = await service.run(make_job(graph=GraphSettings(enabled=True)), FakeSource({"/a.md": DOC.encode()}), store)

        result = by_path(job)["/a.md"]
        assert result.status == IngestionFileStatus.COMPLETED
        assert (result.entity_count, result.relation_count) == (0, 0)
        assert store.graphs == {}

    async def test_extraction_failure_keeps_the_vectors(self, service, extractor, caplog):
        extractor.error = RuntimeError("llm down")
        store = GraphVectorStore()

        with caplog.at_level(logging.WARNING, logger="src.ingestion"):
            job = await service.run(make_job(graph=GraphSettings(enabled=True)), FakeSource({"/a.md": DOC.encode()}), store)

        result = by_path(job)["/a.md"]
        assert result.status == IngestionFileStatus.COMPLETED
        assert result.chunk_count == 2
        assert (NAMESPACE, "a.md") in store.docs
        assert store.graphs == {}
        assert "Graph extraction failed for /a.md: llm down" in caplog.text

    async def test_disabled_graph_is_never_extracted(self, service, extractor):
        store = GraphVectorStore()

        await service.run(make_job(), FakeSource({"/a.md": DOC.encode()}), store)

        assert extractor.calls == []
        assert store.graphs == {}

    async def test_job_without_graph_settings_runs_plain_ingestion(self, service, extractor):
        job = make_job().model_copy(update={"graph": None})
        store = GraphVectorStore()

        await service.run(job, FakeSource({"/a.md": DOC.encode()}), store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED
        assert extractor.calls == []

    async def test_store_without_graph_support_logs_and_skips_extraction(self, service, extractor, caplog):
        store = FakeVectorStore()

        with caplog.at_level(logging.WARNING, logger="src.ingestion"):
            job = await service.run(make_job(graph=GraphSettings(enabled=True)), FakeSource({"/a.md": DOC.encode()}), store)

        assert by_path(job)["/a.md"].status == IngestionFileStatus.COMPLETED
        assert extractor.calls == []
        assert "vector store chroma is not graph-capable" in caplog.text

    async def test_graph_capable_store_does_not_warn(self, service, caplog):
        with caplog.at_level(logging.WARNING, logger="src.ingestion"):
            await service.run(make_job(graph=GraphSettings(enabled=True)), FakeSource({"/a.md": DOC.encode()}), GraphVectorStore())

        assert "not graph-capable" not in caplog.text
