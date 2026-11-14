"""API tests: execution history, LLM settings, vector stores, ingestion jobs and the UI."""
import logging
from unittest.mock import AsyncMock

import pytest

from src import main
from src.chunking import ChunkingParams, chunking_version
from src.embedding import EmbeddingService
from src.integration import IntegrationType
from src.models import (
    VECTOR_STORE_SCHEMAS,
    ChunkingSettings,
    EmbeddingSettings,
    IngestionAnalysis,
    IngestionJobCreateRequest,
    IngestionJobUpdateRequest,
    JobStatus,
    VectorStoreConfig,
)
from src.vectorstore.base import Chunk, VectorStoreType
from tests.helpers import FakeSource, FakeVectorStore
from tests.main_support import INGESTION_ID, JOB_ID, LOCAL_SOURCE, make_ingestion_job

INGESTION_BODY = {
    "name": "index-docs",
    "source": LOCAL_SOURCE,
    "vector_store": {"type": "chroma", "config": {"path": "/data/chroma"}},
}


# ----------------------------------------------------------- execution history

def test_history_is_empty_without_storage(client):
    assert client.get(f"/jobs/{JOB_ID}/history").json() == []
    assert client.get("/history").json() == []


def test_job_history(client, history_storage):
    history_storage.list_by_job.return_value = [{"job_id": JOB_ID, "status": "completed"}]

    response = client.get(f"/jobs/{JOB_ID}/history", params={"limit": 5})

    assert response.json() == [{"job_id": JOB_ID, "status": "completed"}]
    history_storage.list_by_job.assert_awaited_once_with(JOB_ID, limit=5)


def test_job_history_default_limit(client, history_storage):
    history_storage.list_by_job.return_value = []
    client.get(f"/jobs/{JOB_ID}/history")
    history_storage.list_by_job.assert_awaited_once_with(JOB_ID, limit=50)


def test_recent_history(client, history_storage):
    history_storage.list_recent.return_value = [{"job_id": "a"}, {"job_id": "b"}]

    assert client.get("/history").json() == [{"job_id": "a"}, {"job_id": "b"}]

    history_storage.list_recent.assert_awaited_once_with(limit=100)


@pytest.mark.parametrize(
    "path, params",
    [
        (f"/jobs/{JOB_ID}/history", {"limit": 0}),
        (f"/jobs/{JOB_ID}/history", {"limit": 201}),
        ("/history", {"limit": 501}),
    ],
)
def test_history_limits_are_validated(client, history_storage, path, params):
    assert client.get(path, params=params).status_code == 422


def test_history_cleanup(client, history_storage):
    history_storage.cleanup_old.return_value = 4

    response = client.delete("/history/cleanup", params={"days": 7})

    assert response.status_code == 200
    assert response.json() == {"deleted": 4, "days": 7}
    history_storage.cleanup_old.assert_awaited_once_with(days=7)


def test_history_cleanup_defaults_to_30_days(client, history_storage):
    history_storage.cleanup_old.return_value = 0
    assert client.delete("/history/cleanup").json() == {"deleted": 0, "days": 30}


@pytest.mark.parametrize("days", [0, 366])
def test_history_cleanup_validates_days(client, history_storage, days):
    assert client.delete("/history/cleanup", params={"days": days}).status_code == 422
    history_storage.cleanup_old.assert_not_awaited()


def test_history_cleanup_requires_storage(client):
    response = client.delete("/history/cleanup")
    assert response.status_code == 503
    assert response.json() == {"detail": "History storage not available"}


# --------------------------------------------------------------- LLM settings

def test_get_llm_settings_masks_the_api_key(client):
    main.settings.llm_api_key = "sk-secret"

    body = client.get("/settings/llm").json()

    assert body == {
        "llm_provider": "openai",
        "llm_model": main.settings.llm_model,
        "llm_base_url": "",
        "llm_api_key_set": True,
    }
    assert "sk-secret" not in str(body)


def test_get_llm_settings_without_api_key(client):
    assert client.get("/settings/llm").json()["llm_api_key_set"] is False


def test_update_llm_settings(client):
    response = client.put(
        "/settings/llm",
        json={
            "llm_provider": "ollama",
            "llm_model": "llava",
            "llm_base_url": "http://localhost:11434/v1",
            "llm_api_key": "k",
        },
    )

    assert response.json() == {
        "llm_provider": "ollama",
        "llm_model": "llava",
        "llm_base_url": "http://localhost:11434/v1",
        "llm_api_key_set": True,
    }
    assert main.settings.llm_api_key == "k"
    # the running services see the change (they share the settings object)
    assert main.settings.llm_provider == "ollama"


def test_update_llm_settings_is_partial(client):
    before = main.settings.model_dump()

    response = client.put("/settings/llm", json={"llm_model": "gpt-x"})

    assert response.json()["llm_model"] == "gpt-x"
    after = main.settings.model_dump()
    assert {k for k in after if after[k] != before[k]} == {"llm_model"}


def test_settings_changes_do_not_leak_between_tests(client):
    # Guards the `main_state` fixture: defaults are back after the PUT tests above.
    assert client.get("/settings/llm").json()["llm_provider"] == "openai"


# -------------------------------------------------------------- vector stores

def test_list_vector_store_types(client):
    body = client.get("/vector-stores").json()

    assert set(body["supported"]) == {t.value for t in VectorStoreType}
    assert set(body["schemas"]) == set(VECTOR_STORE_SCHEMAS)
    chroma = body["schemas"]["chroma"]
    assert chroma["name"] == "Chroma (filesystem)"
    assert [f["name"] for f in chroma["fields"]] == ["path"]
    # every supported backend can be configured from the UI
    assert set(body["supported"]) <= set(body["schemas"])


# ------------------------------------------------------- ingestion job CRUD

def test_create_ingestion_job(client, ingestion_storage):
    ingestion_storage.create.return_value = make_ingestion_job()

    response = client.post("/ingestion-jobs", json=INGESTION_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["_id"] == INGESTION_ID
    assert body["vector_store"] == {
        "type": "chroma",
        "config": {"path": "/data/chroma"},
        "namespace": "default",
    }
    assert body["source_extensions"] == [".md"]
    request = ingestion_storage.create.await_args.args[0]
    assert isinstance(request, IngestionJobCreateRequest)
    assert request.vector_store.type == "chroma"


def test_create_ingestion_job_validates_the_body(client, ingestion_storage):
    bad_store = {**INGESTION_BODY, "vector_store": {"type": "faiss", "config": {}}}
    assert client.post("/ingestion-jobs", json=bad_store).status_code == 422
    assert client.post("/ingestion-jobs", json={"name": "x"}).status_code == 422
    ingestion_storage.create.assert_not_awaited()


def test_list_ingestion_jobs_is_empty_without_storage(client):
    assert client.get("/ingestion-jobs").json() == []


def test_list_ingestion_jobs(client, ingestion_storage):
    ingestion_storage.list_jobs.return_value = [make_ingestion_job()]

    body = client.get("/ingestion-jobs", params={"status": "failed", "limit": 3, "offset": 1}).json()

    assert [j["name"] for j in body] == ["index-docs"]
    ingestion_storage.list_jobs.assert_awaited_once_with(status=JobStatus.FAILED, limit=3, offset=1)


def test_list_ingestion_jobs_defaults(client, ingestion_storage):
    ingestion_storage.list_jobs.return_value = []
    client.get("/ingestion-jobs")
    ingestion_storage.list_jobs.assert_awaited_once_with(status=None, limit=50, offset=0)


def test_get_ingestion_job(client, ingestion_storage):
    ingestion_storage.get.return_value = make_ingestion_job()

    response = client.get(f"/ingestion-jobs/{INGESTION_ID}")

    assert response.status_code == 200
    assert response.json()["_id"] == INGESTION_ID
    ingestion_storage.get.assert_awaited_once_with(INGESTION_ID)


def test_update_ingestion_job(client, ingestion_storage):
    ingestion_storage.update.return_value = make_ingestion_job(batch_size=9)

    response = client.patch(f"/ingestion-jobs/{INGESTION_ID}", json={"batch_size": 9})

    assert response.json()["batch_size"] == 9
    job_id, update = ingestion_storage.update.await_args.args
    assert job_id == INGESTION_ID
    assert isinstance(update, IngestionJobUpdateRequest)
    assert update.model_dump(exclude_none=True) == {"batch_size": 9}


def test_delete_ingestion_job(client, ingestion_storage):
    ingestion_storage.delete.return_value = True

    response = client.delete(f"/ingestion-jobs/{INGESTION_ID}")

    assert response.json() == {"message": "Ingestion job deleted"}
    ingestion_storage.delete.assert_awaited_once_with(INGESTION_ID)


INGESTION_ROUTES = [
    ("POST", "/ingestion-jobs", INGESTION_BODY),
    ("GET", f"/ingestion-jobs/{INGESTION_ID}", None),
    ("PATCH", f"/ingestion-jobs/{INGESTION_ID}", {"name": "x"}),
    ("DELETE", f"/ingestion-jobs/{INGESTION_ID}", None),
    ("POST", f"/ingestion-jobs/{INGESTION_ID}/analyze", None),
    ("POST", f"/ingestion-jobs/{INGESTION_ID}/run", None),
    ("POST", f"/ingestion-jobs/{INGESTION_ID}/cancel", None),
    ("POST", f"/ingestion-jobs/{INGESTION_ID}/query", {"query": "hello"}),
]


@pytest.mark.parametrize("method, path, body", INGESTION_ROUTES)
def test_ingestion_endpoints_require_storage(client, method, path, body):
    response = client.request(method, path, json=body)
    assert response.status_code == 503
    assert response.json() == {"detail": "Ingestion storage not available"}


@pytest.mark.parametrize("method, path, body", INGESTION_ROUTES[1:])
def test_ingestion_endpoints_return_404_for_unknown_job(client, ingestion_storage, method, path, body):
    ingestion_storage.get.return_value = None
    ingestion_storage.update.return_value = None
    ingestion_storage.delete.return_value = False

    response = client.request(method, path, json=body)

    assert response.status_code == 404
    assert response.json() == {"detail": "Ingestion job not found"}


# -------------------------------------------------------- ingestion: analyze

def test_analyze_ingestion_job(client, ingestion_storage, integrations, monkeypatch):
    job = make_ingestion_job(
        source_folder="kb",
        source_extensions=[".md", ".markdown"],
        vector_store=VectorStoreConfig(type="chroma", config={"path": "/c"}, namespace="tenant-a"),
        embedding=EmbeddingSettings(model="text-embedding-3-large"),
        chunking=ChunkingSettings(strategy="recursive", chunk_size=500, chunk_overlap=50),
    )
    ingestion_storage.get.return_value = job
    source, store = FakeSource(), FakeVectorStore()
    integrations.source, integrations.vector_store = source, store
    analysis = IngestionAnalysis(source_documents=4, indexed_documents=1, completion_percentage=25.0)
    analyze = AsyncMock(return_value=(analysis, ["internal"]))
    monkeypatch.setattr(main.ingester, "analyze", analyze)

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/analyze")

    assert response.status_code == 200
    assert response.json()["source_documents"] == 4
    assert response.json()["completion_percentage"] == 25.0
    analyze.assert_awaited_once_with(
        source=source,
        vector_store=store,
        namespace="tenant-a",
        embedding_model="text-embedding-3-large",
        chunk_version=chunking_version(ChunkingParams("recursive", 500, 50)),
        source_extensions=[".md", ".markdown"],
        source_folder="kb",
    )
    assert integrations.calls == [
        ("source", IntegrationType.LOCAL, LOCAL_SOURCE["config"]),
        ("vector_store", VectorStoreType.CHROMA, {"path": "/c"}),
    ]
    assert source.disconnect_calls == 1
    assert store.connected is False


def test_analyze_ingestion_job_with_real_service(client, ingestion_storage, integrations):
    """Real IngestionService.analyze over in-memory fakes: nothing indexed yet."""
    ingestion_storage.get.return_value = make_ingestion_job()
    integrations.source = FakeSource({"/kb/a.md": b"# A\n\ntext", "/kb/b.md": b"# B", "/kb/c.pdf": b"x"})
    integrations.vector_store = FakeVectorStore()

    body = client.post(f"/ingestion-jobs/{INGESTION_ID}/analyze").json()

    assert body["source_documents"] == 2
    assert body["indexed_documents"] == 0
    assert sorted(d["path"] for d in body["to_ingest"]) == ["/kb/a.md", "/kb/b.md"]
    assert body["up_to_date"] == []
    assert body["completion_percentage"] == 0.0


def test_analyze_ingestion_job_failure_is_500(client, ingestion_storage, integrations, caplog):
    ingestion_storage.get.return_value = make_ingestion_job()
    integrations.source = FakeSource()
    integrations.vector_store = FakeVectorStore(connect_ok=False)

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = client.post(f"/ingestion-jobs/{INGESTION_ID}/analyze")

    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to connect to vector store chroma"}
    assert "Ingestion analysis failed" in caplog.text
    # the source connection opened before the failure is released
    assert integrations.source.disconnect_calls == 1


# ------------------------------------------------------ ingestion: run/cancel

def test_run_ingestion_job(client, ingestion_storage, monkeypatch):
    ingestion_storage.get.return_value = make_ingestion_job(status=JobStatus.FAILED)
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_ingestion_job_background", runner)

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/run")

    assert response.status_code == 200
    assert response.json() == {"message": "Ingestion job started", "job_id": INGESTION_ID}
    ingestion_storage.update_status.assert_awaited_once_with(INGESTION_ID, JobStatus.PENDING)
    runner.assert_awaited_once_with(INGESTION_ID)


def test_run_ingestion_job_rejects_an_already_running_job(client, ingestion_storage, monkeypatch):
    ingestion_storage.get.return_value = make_ingestion_job(status=JobStatus.RUNNING)
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_ingestion_job_background", runner)

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/run")

    assert response.status_code == 400
    assert response.json() == {"detail": "Job is already running"}
    runner.assert_not_awaited()
    ingestion_storage.update_status.assert_not_awaited()


def test_cancel_ingestion_job(client, ingestion_storage):
    ingestion_storage.get.return_value = make_ingestion_job(status=JobStatus.RUNNING)

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/cancel")

    assert response.json() == {"message": "Ingestion job cancelled"}
    ingestion_storage.update_status.assert_awaited_once_with(INGESTION_ID, JobStatus.CANCELLED)


def test_cancel_ingestion_job_that_is_not_running(client, ingestion_storage):
    ingestion_storage.get.return_value = make_ingestion_job(status=JobStatus.PENDING)

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/cancel")

    assert response.status_code == 400
    assert response.json() == {"detail": "Job is not running"}
    ingestion_storage.update_status.assert_not_awaited()


# ------------------------------------------- run_ingestion_job_background

async def test_ingestion_background_is_a_noop_without_storage(main_state, monkeypatch):
    run = AsyncMock()
    monkeypatch.setattr(main.ingester, "run", run)
    assert await main.run_ingestion_job_background(INGESTION_ID) is None
    run.assert_not_awaited()


async def test_ingestion_background_ignores_unknown_job(ingestion_storage, monkeypatch):
    ingestion_storage.get.return_value = None
    run = AsyncMock()
    monkeypatch.setattr(main.ingester, "run", run)

    await main.run_ingestion_job_background(INGESTION_ID)

    run.assert_not_awaited()
    ingestion_storage.update_status.assert_not_awaited()


async def test_ingestion_background_success(ingestion_storage, integrations, monkeypatch):
    job = make_ingestion_job()
    ingestion_storage.get.return_value = job
    source, store = FakeSource(), FakeVectorStore()
    integrations.source, integrations.vector_store = source, store

    async def fake_run(job_, src, vs, progress_callback=None):
        assert (job_, src, vs) == (job, source, store)
        assert src.connected and vs.connected
        await progress_callback(job_, 1, 2)
        job_.status = JobStatus.COMPLETED
        return job_

    monkeypatch.setattr(main.ingester, "run", fake_run)

    await main.run_ingestion_job_background(INGESTION_ID)

    # progress save + final save
    assert [c.args for c in ingestion_storage.save.await_args_list] == [(job,), (job,)]
    assert job.status == JobStatus.COMPLETED
    assert source.disconnect_calls == 1 and store.connected is False
    ingestion_storage.update_status.assert_not_awaited()


async def test_ingestion_background_marks_job_failed_on_error(ingestion_storage, integrations, caplog):
    ingestion_storage.get.return_value = make_ingestion_job()
    integrations.source = FakeSource()
    integrations.vector_store = ValueError("Unsupported vector store type")

    with caplog.at_level(logging.ERROR, logger="src.main"):
        await main.run_ingestion_job_background(INGESTION_ID)

    ingestion_storage.update_status.assert_awaited_once_with(
        INGESTION_ID, JobStatus.FAILED, "Unsupported vector store type"
    )
    ingestion_storage.save.assert_not_awaited()
    assert f"Ingestion job {INGESTION_ID} failed" in caplog.text


# ---------------------------------------------------------- ingestion: query

@pytest.fixture
def embed(monkeypatch):
    mock = AsyncMock(return_value=[[0.1, 0.2, 0.3]])
    monkeypatch.setattr(EmbeddingService, "embed", mock)
    return mock


async def _indexed_store(namespace="default") -> FakeVectorStore:
    store = FakeVectorStore()
    await store.upsert_document(
        namespace,
        "kb/a.md",
        [
            Chunk(chunk_id="c1", doc_id="kb/a.md", text="alpha", metadata={"headings": "A"}),
            Chunk(chunk_id="c2", doc_id="kb/a.md", text="beta"),
        ],
        source_hash="h",
        embedding_model="m",
        chunking_version="v",
    )
    return store


async def test_query_ingestion_job(client, ingestion_storage, integrations, embed):
    ingestion_storage.get.return_value = make_ingestion_job(
        embedding=EmbeddingSettings(provider="ollama", model="nomic", api_key="k", base_url="http://e/v1")
    )
    store = await _indexed_store()
    integrations.vector_store = store

    response = client.post(
        f"/ingestion-jobs/{INGESTION_ID}/query",
        json={"query": "what is alpha?", "k": 1, "filter": {"doc_id": "kb/a.md"}},
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "chunk_id": "c1",
            "doc_id": "kb/a.md",
            "text": "alpha",
            "metadata": {"headings": "A"},
            "score": 1.0,
        }
    ]
    embed.assert_awaited_once_with(
        ["what is alpha?"], provider="ollama", model="nomic", api_key="k", base_url="http://e/v1"
    )
    assert store.queries == [
        {"namespace": "default", "embedding": [0.1, 0.2, 0.3], "k": 1, "filter": {"doc_id": "kb/a.md"}}
    ]
    assert store.connected is False


async def test_query_uses_requested_namespace_and_default_k(client, ingestion_storage, integrations, embed):
    ingestion_storage.get.return_value = make_ingestion_job()
    store = await _indexed_store("tenant-b")
    integrations.vector_store = store

    body = client.post(
        f"/ingestion-jobs/{INGESTION_ID}/query", json={"query": "q", "namespace": "tenant-b"}
    ).json()

    assert [hit["chunk_id"] for hit in body] == ["c1", "c2"]
    assert body[1]["score"] == pytest.approx(0.9)
    assert store.queries[0]["namespace"] == "tenant-b"
    assert store.queries[0]["k"] == 5
    assert store.queries[0]["filter"] is None


async def test_query_falls_back_to_the_job_namespace_when_blank(
    client, ingestion_storage, integrations, embed
):
    ingestion_storage.get.return_value = make_ingestion_job(
        vector_store=VectorStoreConfig(type="chroma", config={}, namespace="job-ns")
    )
    store = FakeVectorStore()
    integrations.vector_store = store

    assert client.post(f"/ingestion-jobs/{INGESTION_ID}/query", json={"query": "q", "namespace": ""}).json() == []
    assert store.queries[0]["namespace"] == "job-ns"


@pytest.mark.xfail(
    strict=True,
    reason="BUG: request namespace defaults to 'default', so a job's own namespace is never used "
    "unless the caller repeats it (or sends an empty string)",
)
async def test_query_defaults_to_the_job_namespace(client, ingestion_storage, integrations, embed):
    ingestion_storage.get.return_value = make_ingestion_job(
        vector_store=VectorStoreConfig(type="chroma", config={}, namespace="job-ns")
    )
    store = FakeVectorStore()
    integrations.vector_store = store

    client.post(f"/ingestion-jobs/{INGESTION_ID}/query", json={"query": "q"})

    assert store.queries[0]["namespace"] == "job-ns"


@pytest.mark.parametrize("body", [{}, {"query": "q", "k": 0}, {"query": "q", "k": 51}])
def test_query_validates_the_body(client, ingestion_storage, body):
    ingestion_storage.get.return_value = make_ingestion_job()
    assert client.post(f"/ingestion-jobs/{INGESTION_ID}/query", json=body).status_code == 422


def test_query_embedding_failure_is_500(client, ingestion_storage, integrations, embed, caplog):
    ingestion_storage.get.return_value = make_ingestion_job()
    integrations.vector_store = FakeVectorStore()
    embed.side_effect = RuntimeError("embedding quota exceeded")

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = client.post(f"/ingestion-jobs/{INGESTION_ID}/query", json={"query": "q"})

    assert response.status_code == 500
    assert response.json() == {"detail": "embedding quota exceeded"}
    assert "Vector query failed" in caplog.text


def test_query_vector_store_failure_is_500(client, ingestion_storage, integrations, embed):
    ingestion_storage.get.return_value = make_ingestion_job()
    store = FakeVectorStore(query_error=RuntimeError("index missing"))
    integrations.vector_store = store

    response = client.post(f"/ingestion-jobs/{INGESTION_ID}/query", json={"query": "q"})

    assert response.status_code == 500
    assert response.json() == {"detail": "index missing"}
    assert store.connected is False


# ------------------------------------------------------------------------- UI

def test_ui_is_served_with_inlined_assets(client):
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert "<html" in html.lower()
    # placeholders are replaced by the real CSS / JS
    assert "/* APP_CSS */" not in html
    assert "<!-- APP_JS -->" not in html
