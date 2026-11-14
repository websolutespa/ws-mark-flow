"""API tests: health/info, authentication, conversion-job CRUD, analysis, execution."""
import asyncio
import logging
from importlib.metadata import PackageNotFoundError
from unittest.mock import AsyncMock

import pytest
from apscheduler import ConflictPolicy
from apscheduler.triggers.cron import CronTrigger
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials
from fastapi.testclient import TestClient
from starlette.requests import Request

from src import main
from src.integration import CONVERTIBLE_EXTENSIONS, IntegrationType
from src.models import (
    ConversionAnalysis,
    ConversionStats,
    JobCreateRequest,
    JobStatus,
    JobUpdateRequest,
)
from tests.helpers import FakeDestination, FakeSource
from tests.main_support import JOB_ID, LOCAL_DEST, LOCAL_SOURCE, make_job

JOB_BODY = {"name": "nightly", "source": LOCAL_SOURCE, "destination": LOCAL_DEST}


def _request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
        }
    )


# ---------------------------------------------------------------- health/info

def test_health_without_storage(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "mongodb": False}


def test_health_with_storage(client, storage):
    assert client.get("/health").json() == {"status": "healthy", "mongodb": True}


def test_info(client, monkeypatch):
    import torch

    monkeypatch.setattr(main, "get_ws_mark_flow_package_version", lambda: "9.9.9")
    body = client.get("/info").json()

    assert body["name"] == main.settings.app_name
    assert body["version"] == main.settings.app_version
    assert body["package_name"] == "ws-mark-flow"
    assert body["package_version"] == "9.9.9"
    assert set(body["supported_sources"]) == {t.value for t in IntegrationType}
    assert "sitemap" not in body["supported_destinations"]
    assert "local" in body["supported_destinations"]
    assert body["convertible_extensions"] == CONVERTIBLE_EXTENSIONS
    cuda = torch.cuda.is_available()
    assert body["torch"] == {
        "version": torch.__version__,
        "cuda_available": cuda,
        "cuda_version": torch.version.cuda if cuda else None,
        "gpu_count": torch.cuda.device_count() if cuda else 0,
    }


def test_info_reports_cuda_details_when_available(client, monkeypatch):
    import torch

    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 2)
    monkeypatch.setattr(torch.version, "cuda", "12.4")

    info = client.get("/info").json()["torch"]

    assert info["cuda_available"] is True
    assert info["cuda_version"] == "12.4"
    assert info["gpu_count"] == 2


def test_package_version_falls_back_to_underscore_name(monkeypatch):
    seen = []

    def fake_version(name):
        seen.append(name)
        if name == "ws-mark-flow":
            raise PackageNotFoundError(name)
        return "1.2.3"

    monkeypatch.setattr(main, "package_version", fake_version)

    assert main.get_ws_mark_flow_package_version() == "1.2.3"
    assert seen == ["ws-mark-flow", "ws_mark_flow"]


def test_package_version_is_none_when_not_installed(monkeypatch):
    def missing(name):
        raise PackageNotFoundError(name)

    monkeypatch.setattr(main, "package_version", missing)

    assert main.get_ws_mark_flow_package_version() is None


# ------------------------------------------------------------- authentication

@pytest.fixture
def secured(client):
    main.settings.auth_username = "admin"
    main.settings.auth_password = "s3cret"
    return client


def test_auth_is_disabled_when_password_is_empty(client):
    assert client.get("/schemas").status_code == 200
    assert client.get("/schemas", auth=("anyone", "anything")).status_code == 200


def test_auth_missing_credentials_is_401_with_challenge(secured):
    response = secured.get("/schemas")
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}
    assert response.headers["WWW-Authenticate"] == "Basic"


@pytest.mark.parametrize(
    "credentials",
    [("admin", "wrong"), ("intruder", "s3cret"), ("", ""), ("admin", "s3cret ")],
)
def test_auth_rejects_invalid_credentials(secured, credentials):
    response = secured.get("/schemas", auth=credentials)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}
    assert response.headers["WWW-Authenticate"] == "Basic"


def test_auth_accepts_valid_credentials(secured):
    assert secured.get("/schemas", auth=("admin", "s3cret")).status_code == 200


def test_auth_protects_every_route_except_health(secured):
    assert secured.get("/health").status_code == 200
    for method, path in [
        ("GET", "/"),
        ("GET", "/info"),
        ("GET", "/jobs"),
        ("GET", "/configurations"),
        ("GET", "/settings/llm"),
        ("PUT", "/settings/llm"),
        ("GET", "/vector-stores"),
        ("GET", "/history"),
    ]:
        assert secured.request(method, path).status_code == 401, f"{method} {path}"


def test_verify_credentials_return_values(main_state):
    creds = HTTPBasicCredentials(username="bob", password="pw")

    # /health (with or without trailing slash) is always anonymous
    assert main.verify_credentials(_request("/health"), None) == "anonymous"
    assert main.verify_credentials(_request("/health/"), None) == "anonymous"
    # auth disabled
    assert main.verify_credentials(_request("/jobs"), None) == ""
    assert main.verify_credentials(_request("/jobs"), creds) == "bob"
    # auth enabled
    main.settings.auth_username = "bob"
    main.settings.auth_password = "pw"
    assert main.verify_credentials(_request("/jobs"), creds) == "bob"
    with pytest.raises(HTTPException) as excinfo:
        main.verify_credentials(_request("/healthz"), None)
    assert excinfo.value.status_code == 401


def test_auth_handles_non_ascii_credentials(secured):
    main.settings.auth_password = "pässwörd"
    creds = HTTPBasicCredentials(username="admin", password="pässwörd")
    assert main.verify_credentials(_request("/jobs"), creds) == "admin"


# ------------------------------------------------------------------ job CRUD

def test_create_job_requires_storage(client):
    response = client.post("/jobs", json=JOB_BODY)
    assert response.status_code == 503
    assert response.json() == {"detail": "Storage not available"}


def test_create_job(client, storage, scheduler):
    storage.create_job.return_value = make_job()

    response = client.post("/jobs", json={**JOB_BODY, "batch_size": 8, "source_folder": "in"})

    assert response.status_code == 200
    body = response.json()
    assert body["_id"] == JOB_ID
    assert body["name"] == "nightly"
    assert body["status"] == "pending"
    assert body["source"] == LOCAL_SOURCE
    request = storage.create_job.await_args.args[0]
    assert isinstance(request, JobCreateRequest)
    assert (request.name, request.batch_size, request.source_folder) == ("nightly", 8, "in")
    scheduler.add_schedule.assert_not_awaited()


def test_create_job_registers_cron_schedule(client, storage, scheduler):
    storage.create_job.return_value = make_job(schedule_enabled=True, schedule_cron="0 2 * * *")

    assert client.post("/jobs", json=JOB_BODY).status_code == 200

    scheduler.add_schedule.assert_awaited_once()
    args, kwargs = scheduler.add_schedule.await_args
    assert args[0] is main._scheduled_job_runner
    assert isinstance(args[1], CronTrigger)
    assert kwargs == {
        "id": f"job-{JOB_ID}",
        "args": [JOB_ID],
        "conflict_policy": ConflictPolicy.replace,
    }


def test_create_job_ignores_cron_when_schedule_disabled(client, storage, scheduler):
    storage.create_job.return_value = make_job(schedule_enabled=False, schedule_cron="0 2 * * *")
    assert client.post("/jobs", json=JOB_BODY).status_code == 200
    scheduler.add_schedule.assert_not_awaited()


def test_create_job_survives_an_invalid_cron_expression(client, storage, scheduler, caplog):
    storage.create_job.return_value = make_job(schedule_enabled=True, schedule_cron="not a cron")

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = client.post("/jobs", json=JOB_BODY)

    assert response.status_code == 200
    scheduler.add_schedule.assert_not_awaited()
    assert "Failed to create schedule for new job" in caplog.text


@pytest.mark.parametrize(
    "body",
    [
        {"source": LOCAL_SOURCE, "destination": LOCAL_DEST},  # no name
        {"name": "x", "source": LOCAL_SOURCE},  # no destination
        {"name": "x", "source": {"type": "ftp", "config": {}}, "destination": LOCAL_DEST},
        {**JOB_BODY, "conversion_strategy": "turbo"},
    ],
)
def test_create_job_validates_the_body(client, storage, body):
    assert client.post("/jobs", json=body).status_code == 422
    storage.create_job.assert_not_awaited()


def test_list_jobs_is_empty_without_storage(client):
    response = client.get("/jobs")
    assert response.status_code == 200
    assert response.json() == []


def test_list_jobs_defaults(client, storage):
    storage.list_jobs.return_value = [make_job(), make_job(name="other")]

    response = client.get("/jobs")

    assert [j["name"] for j in response.json()] == ["nightly", "other"]
    storage.list_jobs.assert_awaited_once_with(status=None, limit=50, offset=0)


def test_list_jobs_with_filters(client, storage):
    storage.list_jobs.return_value = []

    assert client.get("/jobs", params={"status": "running", "limit": 5, "offset": 2}).json() == []

    storage.list_jobs.assert_awaited_once_with(status=JobStatus.RUNNING, limit=5, offset=2)


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 101}, {"offset": -1}])
def test_list_jobs_rejects_out_of_range_paging(client, storage, params):
    assert client.get("/jobs", params=params).status_code == 422


@pytest.mark.xfail(strict=True, reason="BUG: unknown ?status= raises ValueError -> HTTP 500 instead of a 4xx")
def test_list_jobs_rejects_unknown_status_as_client_error(main_state, storage):
    lenient = TestClient(main.app, raise_server_exceptions=False)
    response = lenient.get("/jobs", params={"status": "bogus"})
    assert response.status_code in (400, 422)


def test_get_job(client, storage):
    storage.get_job.return_value = make_job()

    response = client.get(f"/jobs/{JOB_ID}")

    assert response.status_code == 200
    assert response.json()["_id"] == JOB_ID
    storage.get_job.assert_awaited_once_with(JOB_ID)


def test_get_job_not_found(client, storage):
    storage.get_job.return_value = None
    response = client.get(f"/jobs/{JOB_ID}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


def test_get_job_requires_storage(client):
    assert client.get(f"/jobs/{JOB_ID}").status_code == 503


def test_update_job_without_schedule_fields_leaves_scheduler_alone(client, storage, scheduler):
    storage.update_job.return_value = make_job(name="renamed")

    response = client.patch(f"/jobs/{JOB_ID}", json={"name": "renamed"})

    assert response.status_code == 200
    assert response.json()["name"] == "renamed"
    job_id, update = storage.update_job.await_args.args
    assert job_id == JOB_ID
    assert isinstance(update, JobUpdateRequest)
    assert update.model_dump(exclude_none=True) == {"name": "renamed"}
    scheduler.add_schedule.assert_not_awaited()
    scheduler.remove_schedule.assert_not_awaited()


def test_update_job_enabling_schedule_upserts_it(client, storage, scheduler):
    storage.update_job.return_value = make_job(schedule_enabled=True, schedule_cron="*/5 * * * *")

    response = client.patch(f"/jobs/{JOB_ID}", json={"schedule_enabled": True})

    assert response.status_code == 200
    assert scheduler.add_schedule.await_args.kwargs["id"] == f"job-{JOB_ID}"
    scheduler.remove_schedule.assert_not_awaited()


def test_update_job_disabling_schedule_removes_it(client, storage, scheduler):
    storage.update_job.return_value = make_job(schedule_enabled=False, schedule_cron="*/5 * * * *")

    response = client.patch(f"/jobs/{JOB_ID}", json={"schedule_enabled": False})

    assert response.status_code == 200
    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")
    scheduler.add_schedule.assert_not_awaited()


def test_update_job_survives_an_invalid_cron_expression(client, storage, scheduler, caplog):
    storage.update_job.return_value = make_job(schedule_enabled=True, schedule_cron="bad")

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = client.patch(f"/jobs/{JOB_ID}", json={"schedule_cron": "bad"})

    assert response.status_code == 200
    assert "Failed to update schedule for job" in caplog.text


def test_update_job_not_found(client, storage):
    storage.update_job.return_value = None
    assert client.patch(f"/jobs/{JOB_ID}", json={"name": "x"}).status_code == 404


def test_update_job_requires_storage(client):
    assert client.patch(f"/jobs/{JOB_ID}", json={"name": "x"}).status_code == 503


def test_delete_job_removes_schedule_then_job(client, storage, scheduler):
    storage.delete_job.return_value = True

    response = client.delete(f"/jobs/{JOB_ID}")

    assert response.status_code == 200
    assert response.json() == {"message": "Job deleted"}
    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")
    storage.delete_job.assert_awaited_once_with(JOB_ID)


def test_delete_job_tolerates_a_missing_schedule(client, storage, scheduler):
    scheduler.remove_schedule.side_effect = LookupError("no such schedule")
    storage.delete_job.return_value = True
    assert client.delete(f"/jobs/{JOB_ID}").status_code == 200


def test_delete_job_not_found(client, storage):
    storage.delete_job.return_value = False
    assert client.delete(f"/jobs/{JOB_ID}").status_code == 404


def test_delete_job_requires_storage(client):
    assert client.delete(f"/jobs/{JOB_ID}").status_code == 503


# ------------------------------------------------------------------- analysis

def test_analyze_diffs_source_against_destination(client, integrations):
    source = FakeSource({"/docs/a.pdf": b"1", "/docs/b.docx": b"22", "/docs/notes.txt": b"x"})
    destination = FakeDestination({"/docs/a.md": b"# a", "/docs/other.pdf": b"-"})
    integrations.source, integrations.destination = source, destination

    response = client.post("/analyze", json={"source": LOCAL_SOURCE, "destination": LOCAL_DEST})

    assert response.status_code == 200
    body = response.json()
    assert body["total_source_files"] == 2  # default extensions exclude .txt
    assert body["total_converted_files"] == 1
    assert body["completion_percentage"] == 50.0
    assert [f["path"] for f in body["files_to_convert"]] == ["/docs/b.docx"]
    assert [f["path"] for f in body["already_converted"]] == ["/docs/a.pdf"]
    assert [f["path"] for f in body["destination_files"]] == ["/docs/a.md"]
    assert integrations.calls == [
        ("source", IntegrationType.LOCAL, LOCAL_SOURCE["config"]),
        ("destination", IntegrationType.LOCAL, LOCAL_DEST["config"]),
    ]
    # both connections are opened and released
    assert (source.connect_calls, source.disconnect_calls) == (1, 1)
    assert (destination.connect_calls, destination.disconnect_calls) == (1, 1)


def test_analyze_honours_extension_and_folder_parameters(client, integrations, monkeypatch):
    integrations.source, integrations.destination = FakeSource(), FakeDestination()
    analyze = AsyncMock(return_value=ConversionAnalysis(total_source_files=7))
    monkeypatch.setattr(main.converter, "analyze_conversion", analyze)

    response = client.post(
        "/analyze",
        params={
            "source_extensions": [".txt", ".csv"],
            "source_folder": "in",
            "destination_folder": "out",
        },
        json={"source": LOCAL_SOURCE, "destination": LOCAL_DEST},
    )

    assert response.json()["total_source_files"] == 7
    analyze.assert_awaited_once_with(
        integrations.source, integrations.destination, [".txt", ".csv"], "in", "out"
    )


def test_analyze_with_real_filesystem_integrations(client, tmp_path):
    source_dir, dest_dir = tmp_path / "in", tmp_path / "out"
    source_dir.mkdir()
    dest_dir.mkdir()
    (source_dir / "todo.txt").write_text("one")
    (source_dir / "done.txt").write_text("two")
    (dest_dir / "done.md").write_text("# two")

    response = client.post(
        "/analyze",
        params={"source_extensions": [".txt"]},
        json={
            "source": {"type": "local", "config": {"path": str(source_dir)}},
            "destination": {"type": "local", "config": {"path": str(dest_dir)}},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert [f["name"] for f in body["files_to_convert"]] == ["todo.txt"]
    assert [f["name"] for f in body["already_converted"]] == ["done.txt"]
    assert body["completion_percentage"] == 50.0


def test_analyze_unsupported_destination_type_is_400(client):
    response = client.post(
        "/analyze",
        json={"source": LOCAL_SOURCE, "destination": {"type": "sitemap", "config": {}}},
    )
    assert response.status_code == 400
    assert "Unsupported destination type" in response.json()["detail"]


def test_analyze_connection_failure_is_500(client, integrations):
    integrations.source = FakeSource(connect_ok=False)
    integrations.destination = FakeDestination()

    response = client.post("/analyze", json={"source": LOCAL_SOURCE, "destination": LOCAL_DEST})

    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to connect to local"}


def test_analyze_job_uses_the_job_definition(client, storage, integrations, monkeypatch):
    storage.get_job.return_value = make_job(
        source_extensions=[".pdf"], source_folder="in", destination_folder="out"
    )
    integrations.source, integrations.destination = FakeSource(), FakeDestination()
    analyze = AsyncMock(return_value=ConversionAnalysis(total_source_files=3, completion_percentage=33.3))
    monkeypatch.setattr(main.converter, "analyze_conversion", analyze)

    response = client.post(f"/jobs/{JOB_ID}/analyze")

    assert response.status_code == 200
    assert response.json()["total_source_files"] == 3
    analyze.assert_awaited_once_with(
        integrations.source, integrations.destination, [".pdf"], "in", "out"
    )


def test_analyze_job_failure_is_500(client, storage, integrations):
    storage.get_job.return_value = make_job()
    integrations.source = RuntimeError("bad credentials")

    response = client.post(f"/jobs/{JOB_ID}/analyze")

    assert response.status_code == 500
    assert response.json() == {"detail": "bad credentials"}


def test_analyze_job_not_found(client, storage):
    storage.get_job.return_value = None
    assert client.post(f"/jobs/{JOB_ID}/analyze").status_code == 404


def test_analyze_job_requires_storage(client):
    assert client.post(f"/jobs/{JOB_ID}/analyze").status_code == 503


# ------------------------------------------------------- run / cancel endpoints

def test_run_job_schedules_a_background_run(client, storage, monkeypatch):
    storage.get_job.return_value = make_job()
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_job_background", runner)

    response = client.post(f"/jobs/{JOB_ID}/run")

    assert response.status_code == 200
    assert response.json() == {"message": "Job started", "job_id": JOB_ID}
    runner.assert_awaited_once_with(JOB_ID, "manual")


def test_run_job_rejects_an_already_running_job(client, storage, monkeypatch):
    storage.get_job.return_value = make_job(status=JobStatus.RUNNING)
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_job_background", runner)

    response = client.post(f"/jobs/{JOB_ID}/run")

    assert response.status_code == 400
    assert response.json() == {"detail": "Job is already running"}
    runner.assert_not_awaited()


def test_run_job_not_found(client, storage):
    storage.get_job.return_value = None
    assert client.post(f"/jobs/{JOB_ID}/run").status_code == 404


def test_run_job_requires_storage(client):
    assert client.post(f"/jobs/{JOB_ID}/run").status_code == 503


def test_cancel_running_job(client, storage):
    storage.get_job.return_value = make_job(status=JobStatus.RUNNING)

    response = client.post(f"/jobs/{JOB_ID}/cancel")

    assert response.status_code == 200
    assert response.json() == {"message": "Job cancelled"}
    storage.update_job_status.assert_awaited_once_with(JOB_ID, JobStatus.CANCELLED)


def test_cancel_job_that_is_not_running(client, storage):
    storage.get_job.return_value = make_job(status=JobStatus.COMPLETED)

    response = client.post(f"/jobs/{JOB_ID}/cancel")

    assert response.status_code == 400
    assert response.json() == {"detail": "Job is not running"}
    storage.update_job_status.assert_not_awaited()


def test_cancel_job_not_found(client, storage):
    storage.get_job.return_value = None
    assert client.post(f"/jobs/{JOB_ID}/cancel").status_code == 404


def test_cancel_job_requires_storage(client):
    assert client.post(f"/jobs/{JOB_ID}/cancel").status_code == 503


# --------------------------------------------------------- run_job_background

@pytest.fixture
def run_conversion(monkeypatch):
    """Replace the conversion service entry point with a controllable double."""
    mock = AsyncMock()
    monkeypatch.setattr(main.converter, "run_conversion", mock)
    return mock


async def test_background_run_is_a_noop_without_storage(main_state, run_conversion):
    assert await main.run_job_background(JOB_ID) is None
    run_conversion.assert_not_awaited()


@pytest.mark.parametrize(
    "existing, expected_log",
    [
        (None, "not found or could not be loaded"),
        (make_job(status=JobStatus.RUNNING), "already running on another instance"),
        (make_job(status=JobStatus.COMPLETED), "was not claimed for execution"),
    ],
)
async def test_background_run_skips_jobs_it_cannot_claim(
    storage, history_storage, run_conversion, caplog, existing, expected_log
):
    storage.claim_job_for_run.return_value = None
    storage.get_job.return_value = existing

    with caplog.at_level(logging.INFO, logger="src.main"):
        await main.run_job_background(JOB_ID)

    assert expected_log in caplog.text
    run_conversion.assert_not_awaited()
    history_storage.save.assert_not_awaited()
    storage.update_job_status.assert_not_awaited()


async def test_background_run_success_records_progress_and_history(
    storage, history_storage, integrations, monkeypatch
):
    job = make_job()
    storage.claim_job_for_run.return_value = job
    history_storage.save.return_value = "hist-1"
    source, destination = FakeSource(), FakeDestination()
    integrations.source, integrations.destination = source, destination

    async def fake_run(job_, src, dst, progress_callback=None):
        assert src.connected and dst.connected
        await progress_callback(job_, 1, 3)
        job_.status = JobStatus.COMPLETED
        job_.stats = ConversionStats(total_files=3, completed_files=2, failed_files=1)
        return job_

    monkeypatch.setattr(main.converter, "run_conversion", fake_run)

    await main.run_job_background(JOB_ID, trigger="scheduled")

    storage.claim_job_for_run.assert_awaited_once_with(JOB_ID)
    record = history_storage.save.await_args.args[0]
    assert (record.job_id, record.job_name, record.trigger) == (JOB_ID, "nightly", "scheduled")
    assert record.status == JobStatus.RUNNING
    # one save from the progress callback + the final save
    assert storage.save_job.await_count == 2
    assert storage.save_job.await_args.args[0] is job
    args, kwargs = history_storage.update.await_args
    assert args == ("hist-1",)
    assert kwargs["status"] == JobStatus.COMPLETED
    assert (kwargs["total_files"], kwargs["completed_files"], kwargs["failed_files"]) == (3, 2, 1)
    assert kwargs["error_message"] is None
    assert kwargs["completed_at"] is not None
    assert (source.disconnect_calls, destination.disconnect_calls) == (1, 1)
    storage.update_job_status.assert_not_awaited()


async def test_background_run_without_history_storage(storage, integrations, run_conversion):
    job = make_job()
    storage.claim_job_for_run.return_value = job
    integrations.source, integrations.destination = FakeSource(), FakeDestination()
    run_conversion.return_value = job

    await main.run_job_background(JOB_ID)

    storage.save_job.assert_awaited_once_with(job)


async def test_background_run_end_to_end_with_real_converter(storage, history_storage, integrations):
    """Real ConversionService + MarkItDown; only storage and the endpoints of the pipe are fakes."""
    job = make_job(source_extensions=[".txt", ".csv"])
    storage.claim_job_for_run.return_value = job
    history_storage.save.return_value = "hist-1"
    source = FakeSource({"notes.txt": b"hello from the source", "table.csv": b"a,b\n1,2\n"})
    destination = FakeDestination()
    integrations.source, integrations.destination = source, destination

    await main.run_job_background(JOB_ID)

    assert job.status == JobStatus.COMPLETED
    assert sorted(destination.files) == ["notes.md", "table.md"]
    assert "hello from the source" in destination.files["notes.md"].decode("utf-8")
    table = destination.files["table.md"].decode("utf-8")
    assert "| a | b |" in table and "| 1 | 2 |" in table
    assert (job.stats.total_files, job.stats.completed_files, job.stats.failed_files) == (2, 2, 0)
    assert job.stats.completion_percentage == 100.0
    assert history_storage.update.await_args.kwargs["completed_files"] == 2


async def test_background_run_marks_job_failed_on_error(storage, history_storage, integrations):
    storage.claim_job_for_run.return_value = make_job()
    history_storage.save.return_value = "hist-1"
    integrations.source = RuntimeError("boom")

    await main.run_job_background(JOB_ID)

    storage.update_job_status.assert_awaited_once_with(JOB_ID, JobStatus.FAILED, "boom")
    args, kwargs = history_storage.update.await_args
    assert args == ("hist-1",)
    assert kwargs["status"] == JobStatus.FAILED
    assert kwargs["error_message"] == "boom"
    storage.save_job.assert_not_awaited()


async def test_background_run_failure_before_history_record_exists(storage, history_storage):
    storage.claim_job_for_run.side_effect = RuntimeError("db down")

    await main.run_job_background(JOB_ID)

    storage.update_job_status.assert_awaited_once_with(JOB_ID, JobStatus.FAILED, "db down")
    history_storage.update.assert_not_awaited()


async def test_background_run_failure_without_history_storage(storage, integrations):
    storage.claim_job_for_run.return_value = make_job()
    integrations.source = RuntimeError("boom")

    await main.run_job_background(JOB_ID)

    storage.update_job_status.assert_awaited_once_with(JOB_ID, JobStatus.FAILED, "boom")


async def test_background_run_cancellation_is_recorded_and_propagated(
    storage, history_storage, integrations, run_conversion
):
    storage.claim_job_for_run.return_value = make_job()
    history_storage.save.return_value = "hist-1"
    integrations.source, integrations.destination = FakeSource(), FakeDestination()
    run_conversion.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await main.run_job_background(JOB_ID)

    storage.update_job_status.assert_awaited_once_with(
        JOB_ID, JobStatus.FAILED, "Job execution cancelled"
    )
    kwargs = history_storage.update.await_args.kwargs
    assert kwargs["status"] == JobStatus.FAILED
    assert kwargs["error_message"] == "Job execution cancelled"
    # connections are still released
    assert integrations.source.disconnect_calls == 1


async def test_background_run_cancellation_without_history_storage(
    storage, integrations, run_conversion
):
    storage.claim_job_for_run.return_value = make_job()
    integrations.source, integrations.destination = FakeSource(), FakeDestination()
    run_conversion.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await main.run_job_background(JOB_ID)

    storage.update_job_status.assert_awaited_once()


# -------------------------------------------------- source/destination probes

def test_probe_source_lists_convertible_files(client, integrations):
    files = {f"/in/file{i:02d}.pdf": b"x" for i in range(12)}
    files["/in/readme.md"] = b"not convertible"
    integrations.source = FakeSource(files)

    body = client.post("/test/source", json=LOCAL_SOURCE).json()

    assert body["success"] is True
    assert body["message"] == "Successfully connected. Found 12 files."
    assert len(body["sample_files"]) == 10  # preview is capped
    assert body["sample_files"][0] == {
        "name": "file00.pdf",
        "path": "/in/file00.pdf",
        "modified_at": "2026-01-15T12:30:00",
    }
    assert integrations.source.disconnect_calls == 1


def test_probe_source_reports_failure_in_the_payload(client, integrations):
    integrations.source = FakeSource(connect_ok=False)

    response = client.post("/test/source", json=LOCAL_SOURCE)

    assert response.status_code == 200
    assert response.json() == {
        "success": False,
        "message": "Failed to connect to local",
        "sample_files": [],
    }


def test_probe_destination_lists_markdown_only(client, integrations):
    integrations.destination = FakeDestination({"/out/a.md": b"#", "/out/b.pdf": b"x"})

    body = client.post("/test/destination", json=LOCAL_DEST).json()

    assert body["success"] is True
    assert body["message"] == "Successfully connected. Found 1 markdown files."
    assert [f["path"] for f in body["sample_files"]] == ["/out/a.md"]


def test_probe_destination_reports_failure_in_the_payload(client, integrations):
    integrations.destination = FakeDestination(list_error=PermissionError("denied"))

    body = client.post("/test/destination", json=LOCAL_DEST).json()

    assert body == {"success": False, "message": "denied", "sample_files": []}
    assert integrations.destination.disconnect_calls == 1


def test_probe_rejects_unknown_integration_type(client):
    assert client.post("/test/source", json={"type": "ftp", "config": {}}).status_code == 422
