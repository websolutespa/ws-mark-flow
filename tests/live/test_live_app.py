"""
Live tests: the FastAPI app with its real lifespan on MongoDB (``pytest --live``).

Storages and the APScheduler MongoDB data store run against the live database;
sources and destinations are local folders under ``tmp_path``. No LLM or Docling
is involved: inputs are HTML / CSV, converted by MarkItDown.
"""
import sys

import pytest

from src import main

pytestmark = pytest.mark.live

WINDOWS = sys.platform == "win32"


def local(path) -> dict:
    return {"type": "local", "config": {"path": str(path)}}


def create_job(client, source, destination, **overrides) -> dict:
    payload = {
        "name": "live-job",
        "source": local(source),
        "destination": local(destination),
        "source_extensions": [".html", ".csv"],
    }
    payload.update(overrides)
    response = client.post("/jobs", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def run_job(client, job_id) -> dict:
    """Start a run; TestClient executes the background task before returning."""
    response = client.post(f"/jobs/{job_id}/run")
    assert response.status_code == 200, response.text
    return client.get(f"/jobs/{job_id}").json()


def schedule_ids(client) -> set[str]:
    return {s.id for s in client.portal.call(main.scheduler.get_schedules)}


@pytest.fixture
def folders(tmp_path):
    source, destination = tmp_path / "in", tmp_path / "out"
    source.mkdir()
    return source, destination


def test_lifespan_connects_every_storage_and_the_scheduler(start_app):
    with start_app() as client:
        assert client.get("/health").status_code == 200
        assert main.storage is not None
        assert main.config_storage is not None
        assert main.history_storage is not None
        assert main.ingestion_storage is not None
        assert main.scheduler is not None
        assert client.get("/jobs").json() == []


def test_job_from_saved_configurations(start_app, folders):
    source, destination = folders
    (source / "a.html").write_text("<h1>A</h1>", encoding="utf-8")
    with start_app() as client:
        src = client.post("/configurations", json={"name": "in", "is_source": True, **local(source)}).json()
        dst = client.post("/configurations", json={"name": "out", "is_source": False, **local(destination)}).json()

        tested = client.post(f"/configurations/{src['_id']}/test").json()
        assert tested["success"] is True
        assert [f["name"] for f in tested["sample_files"]] == ["a.html"]

        response = client.post("/jobs/from-configs", json={
            "name": "from-configs",
            "source_config_id": src["_id"],
            "destination_config_id": dst["_id"],
        })
        assert response.status_code == 200, response.text
        job = client.get(f"/jobs/{response.json()['_id']}").json()

        assert job["source"] == local(source)
        assert job["destination"] == local(destination)
        assert client.get(f"/configurations/{src['_id']}").json()["use_count"] == 1
        assert client.get(f"/configurations/{dst['_id']}").json()["use_count"] == 1


def test_schedule_is_persisted_and_restored_after_a_restart(start_app, folders):
    with start_app() as client:
        job = create_job(client, *folders)
        response = client.put(f"/jobs/{job['_id']}/schedule",
                              json={"schedule_cron": "0 2 * * *", "schedule_enabled": True})
        assert response.status_code == 200, response.text
        assert schedule_ids(client) == {f"job-{job['_id']}"}

    with start_app() as client:
        assert schedule_ids(client) == {f"job-{job['_id']}"}
        client.put(f"/jobs/{job['_id']}/schedule", json={"schedule_enabled": False})
        assert schedule_ids(client) == set()


def test_invalid_cron_is_rejected(start_app, folders):
    with start_app() as client:
        job = create_job(client, *folders)

        response = client.put(f"/jobs/{job['_id']}/schedule",
                              json={"schedule_cron": "every day", "schedule_enabled": True})

        assert response.status_code == 400
        assert schedule_ids(client) == set()


def test_run_with_nothing_to_convert_completes_and_is_recorded(start_app, folders):
    with start_app() as client:
        job = create_job(client, *folders)

        finished = run_job(client, job["_id"])

        assert finished["status"] == "completed"
        assert finished["stats"]["total_files"] == 0
        [record] = client.get(f"/jobs/{job['_id']}/history").json()
        assert record["trigger"] == "manual"
        assert record["status"] == "completed"
        assert record["completed_at"] is not None


def test_run_with_unreachable_source_fails_and_is_recorded(start_app, tmp_path):
    with start_app() as client:
        job = create_job(client, tmp_path / "missing", tmp_path / "out")

        finished = run_job(client, job["_id"])

        assert finished["status"] == "failed"
        assert "Failed to connect" in finished["error_message"]
        [record] = client.get(f"/jobs/{job['_id']}/history").json()
        assert record["status"] == "failed"
        assert record["error_message"] == finished["error_message"]


@pytest.mark.skipif(
    WINDOWS,
    reason="BUG: converter.run_conversion builds paths with pathlib.Path, so on Windows it writes "
    "converted files to the drive root (pinned in tests/test_converter_llm.py)",
)
def test_run_converts_files_end_to_end_and_skips_them_next_time(start_app, folders):
    source, destination = folders
    (source / "a.html").write_text("<html><body><h1>Alpha</h1><p>first</p></body></html>", encoding="utf-8")
    (source / "docs").mkdir()
    (source / "docs" / "b.csv").write_text("name,qty\nbolt,3\n", encoding="utf-8")
    (source / "ignored.txt").write_text("not selected", encoding="utf-8")

    with start_app() as client:
        job = create_job(client, source, destination)

        finished = run_job(client, job["_id"])

        assert finished["status"] == "completed", finished
        assert finished["stats"]["completed_files"] == 2
        assert sorted(p.relative_to(destination).as_posix() for p in destination.rglob("*.md")) == [
            "a.md", "docs/b.md",
        ]
        assert "# Alpha" in (destination / "a.md").read_text(encoding="utf-8")
        assert "bolt" in (destination / "docs" / "b.md").read_text(encoding="utf-8")
        assert client.post(f"/jobs/{job['_id']}/analyze").json()["completion_percentage"] == 100.0

        rerun = run_job(client, job["_id"])

        assert rerun["status"] == "completed"
        assert rerun["stats"]["skipped_files"] == 2
        assert {r["status"] for r in rerun["file_results"]} == {"skipped"}
        history = client.get(f"/jobs/{job['_id']}/history").json()
        assert [r["status"] for r in history] == ["completed", "completed"]
