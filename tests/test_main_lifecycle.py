"""Application lifespan and APScheduler wiring in ``src.main``."""
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.cron import CronTrigger

from src import main
from src.models import JobUpdateRequest
from src.storage import (
    ConfigurationStorage,
    ExecutionHistoryStorage,
    IngestionJobStorage,
    JobStorage,
)
from tests.main_support import JOB_ID, make_job


# ------------------------------------------------------------ schedule helpers

async def test_upsert_schedule_is_a_noop_without_scheduler(main_state):
    assert await main._upsert_schedule(JOB_ID, "0 2 * * *") is None


async def test_upsert_schedule_registers_a_replacing_cron_schedule(scheduler):
    await main._upsert_schedule(JOB_ID, "30 4 * * 1-5")

    args, kwargs = scheduler.add_schedule.await_args
    assert args[0] is main._scheduled_job_runner
    trigger = args[1]
    assert isinstance(trigger, CronTrigger)
    assert (trigger.minute, trigger.hour) == ("30", "4")
    assert kwargs == {
        "id": f"job-{JOB_ID}",
        "args": [JOB_ID],
        "conflict_policy": ConflictPolicy.replace,
    }


@pytest.mark.parametrize("expression", ["", "* * *", "61 * * * *", "not a cron at all"])
async def test_upsert_schedule_rejects_invalid_cron(scheduler, expression):
    with pytest.raises(ValueError):
        await main._upsert_schedule(JOB_ID, expression)
    scheduler.add_schedule.assert_not_awaited()


async def test_remove_schedule(scheduler):
    await main._remove_schedule(JOB_ID)
    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")


async def test_remove_schedule_is_a_noop_without_scheduler(main_state):
    assert await main._remove_schedule(JOB_ID) is None


async def test_remove_schedule_swallows_lookup_errors(scheduler):
    scheduler.remove_schedule.side_effect = LookupError("unknown schedule")
    assert await main._remove_schedule(JOB_ID) is None


# ------------------------------------------------------- scheduled job runner

async def test_scheduled_runner_is_a_noop_without_storage(main_state, monkeypatch):
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_job_background", runner)
    await main._scheduled_job_runner(JOB_ID)
    runner.assert_not_awaited()


async def test_scheduled_runner_runs_the_job_with_scheduled_trigger(storage, monkeypatch):
    storage.get_job.return_value = make_job()
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_job_background", runner)

    await main._scheduled_job_runner(JOB_ID)

    runner.assert_awaited_once_with(JOB_ID, trigger="scheduled")


async def test_scheduled_runner_drops_the_schedule_of_a_deleted_job(storage, scheduler, monkeypatch, caplog):
    storage.get_job.return_value = None
    runner = AsyncMock()
    monkeypatch.setattr(main, "run_job_background", runner)

    with caplog.at_level(logging.WARNING, logger="src.main"):
        await main._scheduled_job_runner(JOB_ID)

    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")
    runner.assert_not_awaited()
    assert "not found, removing schedule" in caplog.text


async def test_scheduled_runner_never_raises_into_the_scheduler(storage, monkeypatch, caplog):
    storage.get_job.return_value = make_job()
    monkeypatch.setattr(main, "run_job_background", AsyncMock(side_effect=RuntimeError("kaput")))

    with caplog.at_level(logging.ERROR, logger="src.main"):
        await main._scheduled_job_runner(JOB_ID)

    assert f"Scheduled run failed for job {JOB_ID}: kaput" in caplog.text


# ------------------------------------------------------------ schedule syncing

def _schedule(schedule_id, task_id):
    return MagicMock(id=schedule_id, task_id=task_id)


async def test_sync_is_a_noop_without_storage_or_scheduler(main_state, monkeypatch):
    await main._sync_all_schedules()  # neither

    scheduler = AsyncMock(spec=AsyncScheduler)
    monkeypatch.setattr(main, "scheduler", scheduler)
    await main._sync_all_schedules()  # scheduler only
    scheduler.get_schedules.assert_not_awaited()

    monkeypatch.setattr(main, "scheduler", None)
    storage = AsyncMock(spec=JobStorage)
    monkeypatch.setattr(main, "storage", storage)
    await main._sync_all_schedules()  # storage only
    storage.list_jobs.assert_not_awaited()


async def test_sync_purges_schedules_whose_callable_cannot_be_imported(storage, scheduler, caplog):
    scheduler.get_schedules.return_value = [
        _schedule("job-stale", "no_such_module_xyz.app:_scheduled_job_runner"),
        _schedule("job-live", "src.main:_scheduled_job_runner"),
        _schedule("job-no-ref", None),
        _schedule("job-plain", "just_a_name"),
    ]
    storage.list_jobs.return_value = []

    with caplog.at_level(logging.WARNING, logger="src.main"):
        await main._sync_all_schedules()

    scheduler.remove_schedule.assert_awaited_once_with("job-stale")
    assert "Removed stale schedule 'job-stale'" in caplog.text


async def test_sync_logs_when_a_stale_schedule_cannot_be_removed(storage, scheduler, caplog):
    scheduler.get_schedules.return_value = [_schedule("job-stale", "no_such_module_xyz:f")]
    scheduler.remove_schedule.side_effect = RuntimeError("store offline")
    storage.list_jobs.return_value = []

    with caplog.at_level(logging.ERROR, logger="src.main"):
        await main._sync_all_schedules()

    assert "Failed to remove stale schedule job-stale: store offline" in caplog.text


async def test_sync_recreates_schedules_from_jobs(storage, scheduler, caplog):
    scheduler.get_schedules.side_effect = RuntimeError("cannot list")
    storage.list_jobs.return_value = [
        make_job(_id="64b7f0c2a1b2c3d4e5f60001", schedule_enabled=True, schedule_cron="0 1 * * *"),
        make_job(_id="64b7f0c2a1b2c3d4e5f60002", schedule_enabled=False, schedule_cron="0 2 * * *"),
        make_job(_id="64b7f0c2a1b2c3d4e5f60003", schedule_enabled=True, schedule_cron=None),
        make_job(_id="64b7f0c2a1b2c3d4e5f60004", schedule_enabled=True, schedule_cron="broken"),
        make_job(_id="64b7f0c2a1b2c3d4e5f60005", schedule_enabled=True, schedule_cron="0 5 * * *"),
    ]

    with caplog.at_level(logging.WARNING, logger="src.main"):
        await main._sync_all_schedules()

    storage.list_jobs.assert_awaited_once_with(limit=1000, offset=0)
    registered = [call.kwargs["id"] for call in scheduler.add_schedule.await_args_list]
    assert registered == ["job-64b7f0c2a1b2c3d4e5f60001", "job-64b7f0c2a1b2c3d4e5f60005"]
    assert "Could not list existing schedules: cannot list" in caplog.text
    assert "Failed to sync schedule for job 64b7f0c2a1b2c3d4e5f60004" in caplog.text


# ------------------------------------------------- PUT /jobs/{id}/schedule

def test_update_schedule_enables_cron(client, storage, scheduler):
    storage.get_job.return_value = make_job()
    storage.update_job.return_value = make_job(schedule_enabled=True, schedule_cron="0 6 * * *")

    response = client.put(
        f"/jobs/{JOB_ID}/schedule", json={"schedule_cron": "0 6 * * *", "schedule_enabled": True}
    )

    assert response.status_code == 200
    assert response.json() == {
        "job_id": JOB_ID,
        "schedule_cron": "0 6 * * *",
        "schedule_enabled": True,
    }
    job_id, update = storage.update_job.await_args.args
    assert job_id == JOB_ID
    assert isinstance(update, JobUpdateRequest)
    assert update.model_dump(exclude_none=True) == {
        "schedule_cron": "0 6 * * *",
        "schedule_enabled": True,
    }
    assert scheduler.add_schedule.await_args.kwargs["id"] == f"job-{JOB_ID}"
    scheduler.remove_schedule.assert_not_awaited()


def test_update_schedule_disable_removes_it(client, storage, scheduler):
    storage.get_job.return_value = make_job(schedule_enabled=True, schedule_cron="0 6 * * *")
    storage.update_job.return_value = make_job(schedule_enabled=False, schedule_cron="0 6 * * *")

    response = client.put(f"/jobs/{JOB_ID}/schedule", json={"schedule_enabled": False})

    assert response.json()["schedule_enabled"] is False
    update = storage.update_job.await_args.args[1]
    assert update.model_dump(exclude_none=True) == {"schedule_enabled": False}
    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")
    scheduler.add_schedule.assert_not_awaited()


def test_update_schedule_with_empty_body_changes_nothing_but_resyncs(client, storage, scheduler):
    storage.get_job.return_value = make_job()
    storage.update_job.return_value = make_job()

    response = client.put(f"/jobs/{JOB_ID}/schedule", json={})

    assert response.status_code == 200
    assert storage.update_job.await_args.args[1].model_dump(exclude_none=True) == {}
    scheduler.remove_schedule.assert_awaited_once_with(f"job-{JOB_ID}")


def test_update_schedule_invalid_cron_is_400(client, storage, scheduler):
    storage.get_job.return_value = make_job()
    storage.update_job.return_value = make_job(schedule_enabled=True, schedule_cron="every day")

    response = client.put(
        f"/jobs/{JOB_ID}/schedule", json={"schedule_cron": "every day", "schedule_enabled": True}
    )

    assert response.status_code == 400
    assert response.json()["detail"].startswith("Invalid cron expression:")
    scheduler.add_schedule.assert_not_awaited()


@pytest.mark.xfail(
    strict=True,
    reason="BUG: an invalid cron is persisted to the job before it is validated (400 is returned "
    "but the bad expression is already saved)",
)
def test_update_schedule_does_not_persist_an_invalid_cron(client, storage, scheduler):
    storage.get_job.return_value = make_job()
    storage.update_job.return_value = make_job(schedule_enabled=True, schedule_cron="every day")

    response = client.put(
        f"/jobs/{JOB_ID}/schedule", json={"schedule_cron": "every day", "schedule_enabled": True}
    )

    assert response.status_code == 400
    storage.update_job.assert_not_awaited()


def test_update_schedule_job_not_found(client, storage):
    storage.get_job.return_value = None
    assert client.put(f"/jobs/{JOB_ID}/schedule", json={}).status_code == 404
    storage.update_job.assert_not_awaited()


def test_update_schedule_job_deleted_meanwhile(client, storage):
    storage.get_job.return_value = make_job()
    storage.update_job.return_value = None
    assert client.put(f"/jobs/{JOB_ID}/schedule", json={"schedule_enabled": True}).status_code == 404


def test_update_schedule_requires_storage(client):
    assert client.put(f"/jobs/{JOB_ID}/schedule", json={}).status_code == 503


# ------------------------------------------------------------------- lifespan

@pytest.fixture
def backends(main_state, monkeypatch):
    """Replace every external backend the lifespan touches with spec'd doubles."""

    class Backends:
        pass

    b = Backends()
    b.jobs = AsyncMock(spec=JobStorage)
    b.jobs.connect.return_value = True
    b.jobs.list_jobs.return_value = []
    b.configs = AsyncMock(spec=ConfigurationStorage)
    b.configs.connect.return_value = True
    b.history = AsyncMock(spec=ExecutionHistoryStorage)
    b.history.cleanup_old.return_value = 0
    b.ingestion = AsyncMock(spec=IngestionJobStorage)
    b.ingestion.connect.return_value = True
    b.scheduler = AsyncMock(spec=AsyncScheduler)
    b.scheduler.get_schedules.return_value = []

    b.JobStorage = MagicMock(return_value=b.jobs)
    b.ConfigurationStorage = MagicMock(return_value=b.configs)
    b.ExecutionHistoryStorage = MagicMock(return_value=b.history)
    b.IngestionJobStorage = MagicMock(return_value=b.ingestion)
    b.MongoDBDataStore = MagicMock(return_value="data-store")
    b.AsyncScheduler = MagicMock(return_value=b.scheduler)
    for name in (
        "JobStorage",
        "ConfigurationStorage",
        "ExecutionHistoryStorage",
        "IngestionJobStorage",
        "MongoDBDataStore",
        "AsyncScheduler",
    ):
        monkeypatch.setattr(main, name, getattr(b, name))

    b.converter_cleanup = MagicMock()
    b.ingester_cleanup = MagicMock()
    monkeypatch.setattr(main.converter, "cleanup", b.converter_cleanup)
    monkeypatch.setattr(main.ingester, "cleanup", b.ingester_cleanup)
    return b


async def test_lifespan_wires_everything_and_tears_it_down(backends, caplog):
    backends.history.cleanup_old.return_value = 3
    backends.jobs.list_jobs.return_value = [
        make_job(schedule_enabled=True, schedule_cron="0 2 * * *")
    ]
    uri, database = main.settings.mongodb_uri, main.settings.mongodb_database

    with caplog.at_level(logging.INFO, logger="src.main"):
        async with main.lifespan(main.app):
            assert main.storage is backends.jobs
            assert main.config_storage is backends.configs
            assert main.history_storage is backends.history
            assert main.ingestion_storage is backends.ingestion
            assert main.scheduler is backends.scheduler

            backends.JobStorage.assert_called_once_with(uri, database)
            backends.ConfigurationStorage.assert_called_once_with(uri, database)
            backends.IngestionJobStorage.assert_called_once_with(uri, database)
            backends.history.connect.assert_awaited_once_with(uri, database)
            backends.MongoDBDataStore.assert_called_once_with(uri, database=database)
            backends.AsyncScheduler.assert_called_once_with(data_store="data-store")

            # schedules are synced BEFORE the scheduler loop starts
            calls = [c[0] for c in backends.scheduler.method_calls]
            assert calls.index("add_schedule") < calls.index("start_in_background")
            backends.scheduler.__aenter__.assert_awaited_once()
            backends.history.cleanup_old.assert_awaited_once_with(days=30)
            backends.scheduler.__aexit__.assert_not_awaited()

    assert "Cleaned up 3 old execution history records" in caplog.text
    backends.scheduler.__aexit__.assert_awaited_once_with(None, None, None)
    backends.jobs.disconnect.assert_awaited_once()
    backends.configs.disconnect.assert_awaited_once()
    backends.ingestion.disconnect.assert_awaited_once()
    backends.converter_cleanup.assert_called_once_with()
    backends.ingester_cleanup.assert_called_once_with()


async def test_lifespan_degrades_gracefully_when_mongodb_is_down(backends, caplog):
    backends.jobs.connect.return_value = False
    backends.configs.connect.return_value = False
    backends.history.connect.side_effect = ConnectionError("refused")
    backends.ingestion.connect.return_value = False
    backends.MongoDBDataStore.side_effect = ConnectionError("refused")

    with caplog.at_level(logging.WARNING, logger="src.main"):
        async with main.lifespan(main.app):
            assert main.storage is None
            assert main.config_storage is None
            assert main.history_storage is None
            assert main.ingestion_storage is None
            assert main.scheduler is None

    for message in (
        "MongoDB not available - job persistence disabled",
        "Configuration storage not available",
        "Execution history storage not available: refused",
        "Ingestion storage not available",
        "APScheduler not available: refused",
    ):
        assert message in caplog.text
    # nothing was connected, so nothing is disconnected - but temp dirs are still cleaned
    backends.jobs.disconnect.assert_not_awaited()
    backends.configs.disconnect.assert_not_awaited()
    backends.ingestion.disconnect.assert_not_awaited()
    backends.scheduler.__aexit__.assert_not_awaited()
    backends.converter_cleanup.assert_called_once_with()
    backends.ingester_cleanup.assert_called_once_with()


async def test_lifespan_skips_schedule_sync_and_cleanup_without_their_storages(backends):
    backends.jobs.connect.return_value = False
    backends.history.connect.side_effect = ConnectionError("refused")

    async with main.lifespan(main.app):
        assert main.scheduler is backends.scheduler
        backends.scheduler.get_schedules.assert_not_awaited()
        backends.scheduler.start_in_background.assert_awaited_once()
        backends.history.cleanup_old.assert_not_awaited()


async def test_lifespan_disables_scheduler_when_it_fails_to_start(backends, caplog):
    backends.scheduler.start_in_background.side_effect = RuntimeError("no event loop")

    with caplog.at_level(logging.WARNING, logger="src.main"):
        async with main.lifespan(main.app):
            assert main.scheduler is None
            assert main.storage is backends.jobs  # storage keeps working

    assert "APScheduler not available: no event loop" in caplog.text


async def test_lifespan_no_history_cleanup_log_when_nothing_deleted(backends, caplog):
    with caplog.at_level(logging.INFO, logger="src.main"):
        async with main.lifespan(main.app):
            pass
    assert "Cleaned up" not in caplog.text
    assert "APScheduler started with MongoDB data store" in caplog.text


async def test_lifespan_shutdown_survives_scheduler_exit_errors(backends):
    backends.scheduler.__aexit__.side_effect = RuntimeError("already stopped")

    async with main.lifespan(main.app):
        pass

    backends.jobs.disconnect.assert_awaited_once()
    backends.converter_cleanup.assert_called_once_with()


def test_lifespan_runs_under_the_real_test_client(backends):
    """Starlette drives startup/shutdown; `/health` reflects the connected storage."""
    from fastapi.testclient import TestClient

    with TestClient(main.app) as client:
        assert client.get("/health").json() == {"status": "healthy", "mongodb": True}
    backends.jobs.disconnect.assert_awaited_once()
