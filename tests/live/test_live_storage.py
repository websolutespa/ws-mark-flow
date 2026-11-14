"""
Live tests: the MongoDB storages against a real server (``pytest --live``).

They cover what mocks cannot: query semantics, sort order, atomic updates under
concurrency, BSON round trips (e.g. datetimes stored at millisecond precision).
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from bson import ObjectId

from src.models import (
    ConversionJob,
    ConversionStrategy,
    FileConversionResult,
    FileConversionStatus,
    IngestionJobCreateRequest,
    IngestionJobUpdateRequest,
    IntegrationConfig,
    JobCreateRequest,
    JobExecutionHistory,
    JobStatus,
    JobUpdateRequest,
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    VectorStoreConfig,
)

pytestmark = pytest.mark.live

SOURCE = IntegrationConfig(type="local", config={"path": "/data/in"})
DESTINATION = IntegrationConfig(type="local", config={"path": "/data/out"})


def job_request(name: str = "nightly", **overrides) -> JobCreateRequest:
    data = {"name": name, "source": SOURCE, "destination": DESTINATION}
    data.update(overrides)
    return JobCreateRequest(**data)


def same_ms(a: datetime, b: datetime) -> bool:
    """BSON dates have millisecond precision."""
    return abs(a - b) < timedelta(milliseconds=1)


async def tick() -> None:
    """Let the clock move past BSON's millisecond precision, so sorting by date is deterministic."""
    await asyncio.sleep(0.005)


# --- JobStorage --------------------------------------------------------------

class TestJobStorage:
    async def test_connect_creates_the_query_indexes(self, jobs, live_raw_db):
        indexes = set(live_raw_db[jobs.COLLECTION_NAME].index_information())

        assert {"status_1", "created_at_1", "status_1_created_at_-1"} <= indexes

    async def test_created_job_round_trips(self, jobs, live_raw_db):
        created = await jobs.create_job(job_request(
            description="d",
            source_extensions=[".html"],
            source_folder="in",
            destination_folder="out",
            conversion_strategy=ConversionStrategy.BALANCED,
            schedule_cron="0 2 * * *",
        ))

        assert ObjectId.is_valid(created.id)
        stored = live_raw_db[jobs.COLLECTION_NAME].find_one({"_id": ObjectId(created.id)})
        assert stored["status"] == "pending"
        assert stored["source"] == {"type": "local", "config": {"path": "/data/in"}}

        loaded = await jobs.get_job(created.id)
        assert loaded.model_dump(exclude={"created_at", "updated_at"}) == created.model_dump(
            exclude={"created_at", "updated_at"}
        )
        assert same_ms(loaded.created_at, created.created_at)

    @pytest.mark.parametrize("job_id", [str(ObjectId()), "not-an-object-id"])
    async def test_unknown_or_invalid_id_is_not_found(self, jobs, job_id):
        assert await jobs.get_job(job_id) is None

    async def test_list_is_newest_first_with_status_filter_and_paging(self, jobs):
        ids = []
        for i in range(3):
            await tick()
            ids.append((await jobs.create_job(job_request(f"job-{i}"))).id)
        await jobs.update_job_status(ids[1], JobStatus.COMPLETED)

        assert [j.id for j in await jobs.list_jobs()] == ids[::-1]
        assert [j.id for j in await jobs.list_jobs(limit=1, offset=1)] == [ids[1]]
        assert [j.id for j in await jobs.list_jobs(status=JobStatus.COMPLETED)] == [ids[1]]
        assert [j.id for j in await jobs.get_pending_jobs()] == [ids[2], ids[0]]
        assert await jobs.count_jobs() == 3
        assert await jobs.count_jobs(JobStatus.COMPLETED) == 1

    async def test_update_sets_only_the_given_fields(self, jobs):
        job = await jobs.create_job(job_request(description="keep me"))
        await tick()

        updated = await jobs.update_job(job.id, JobUpdateRequest(name="renamed", schedule_enabled=True))

        assert updated.name == "renamed"
        assert updated.schedule_enabled is True
        assert updated.description == "keep me"
        assert updated.updated_at > job.updated_at

    async def test_update_of_a_missing_job_returns_none(self, jobs):
        assert await jobs.update_job(str(ObjectId()), JobUpdateRequest(name="x")) is None

    async def test_status_transitions_stamp_timestamps_and_error(self, jobs):
        job = await jobs.create_job(job_request())

        assert await jobs.update_job_status(job.id, JobStatus.RUNNING)
        running = await jobs.get_job(job.id)
        assert running.started_at is not None and running.completed_at is None

        assert await jobs.update_job_status(job.id, JobStatus.FAILED, "boom")
        failed = await jobs.get_job(job.id)
        assert failed.status == JobStatus.FAILED
        assert failed.completed_at is not None
        assert failed.error_message == "boom"

    async def test_save_replaces_the_whole_document(self, jobs):
        job = await jobs.create_job(job_request())
        job.file_results = [
            FileConversionResult(source_path="/a.html", status=FileConversionStatus.COMPLETED, file_size=10),
            FileConversionResult(source_path="/b.csv", status=FileConversionStatus.FAILED, error_message="x"),
        ]
        job.update_stats()
        job.status = JobStatus.COMPLETED

        await jobs.save_job(job)
        loaded = await jobs.get_job(job.id)

        assert loaded.status == JobStatus.COMPLETED
        assert loaded.file_results == job.file_results
        assert loaded.stats == job.stats

    async def test_save_without_id_inserts(self, jobs):
        job = ConversionJob(name="fresh", source=SOURCE, destination=DESTINATION)

        saved = await jobs.save_job(job)

        assert ObjectId.is_valid(saved.id)
        assert (await jobs.get_job(saved.id)).name == "fresh"

    async def test_delete(self, jobs):
        job = await jobs.create_job(job_request())

        assert await jobs.delete_job(job.id) is True
        assert await jobs.delete_job(job.id) is False
        assert await jobs.get_job(job.id) is None

    async def test_only_one_of_many_concurrent_claims_wins(self, jobs):
        job = await jobs.create_job(job_request())

        claims = await asyncio.gather(*(jobs.claim_job_for_run(job.id) for _ in range(10)))

        winners = [c for c in claims if c is not None]
        assert len(winners) == 1
        assert winners[0].status == JobStatus.RUNNING
        assert (await jobs.get_job(job.id)).status == JobStatus.RUNNING

    async def test_claim_resets_the_previous_run_outcome(self, jobs):
        job = await jobs.create_job(job_request())
        await jobs.update_job_status(job.id, JobStatus.FAILED, "previous failure")

        claimed = await jobs.claim_job_for_run(job.id)

        assert claimed.status == JobStatus.RUNNING
        assert claimed.error_message is None
        assert claimed.completed_at is None
        assert claimed.started_at is not None

    async def test_running_or_missing_job_cannot_be_claimed(self, jobs):
        job = await jobs.create_job(job_request())
        await jobs.update_job_status(job.id, JobStatus.RUNNING)

        assert await jobs.claim_job_for_run(job.id) is None
        assert await jobs.claim_job_for_run(str(ObjectId())) is None


# --- ConfigurationStorage ----------------------------------------------------

class TestConfigurationStorage:
    async def _create(self, configs, name, *, is_source=True, type_="local"):
        await tick()
        return await configs.create(SavedConfigurationCreate(
            name=name, type=type_, config={"path": f"/{name}"}, is_source=is_source,
        ))

    async def test_list_filters_by_role_and_type(self, configs):
        src_local = await self._create(configs, "src-local")
        src_s3 = await self._create(configs, "src-s3", type_="s3")
        dst_local = await self._create(configs, "dst-local", is_source=False)

        assert [c.id for c in await configs.list_all()] == [dst_local.id, src_s3.id, src_local.id]
        assert {c.id for c in await configs.list_sources()} == {src_local.id, src_s3.id}
        assert [c.id for c in await configs.list_sources("s3")] == [src_s3.id]
        assert [c.id for c in await configs.list_destinations()] == [dst_local.id]
        assert await configs.count() == 3
        assert await configs.count(is_source=False) == 1

    async def test_update_merges_and_keeps_type(self, configs):
        created = await self._create(configs, "src")

        updated = await configs.update(created.id, SavedConfigurationUpdate(config={"path": "/new"}))

        assert updated.config == {"path": "/new"}
        assert updated.name == "src"
        assert updated.type == created.type

    async def test_use_count_is_incremented_atomically(self, configs):
        created = await self._create(configs, "src")

        await asyncio.gather(*(configs.increment_use_count(created.id) for _ in range(5)))

        loaded = await configs.get(created.id)
        assert loaded.use_count == 5
        assert loaded.last_used_at is not None

    async def test_delete(self, configs):
        created = await self._create(configs, "src")

        assert await configs.delete(created.id) is True
        assert await configs.get(created.id) is None
        assert await configs.increment_use_count(created.id) is False


# --- ExecutionHistoryStorage -------------------------------------------------

class TestExecutionHistoryStorage:
    async def test_records_are_listed_newest_first_per_job(self, history):
        now = datetime.utcnow()
        old = await history.save(JobExecutionHistory(job_id="j1", started_at=now - timedelta(hours=1)))
        new = await history.save(JobExecutionHistory(job_id="j1", started_at=now))
        other = await history.save(JobExecutionHistory(job_id="j2", started_at=now - timedelta(minutes=1)))

        assert [r["_id"] for r in await history.list_by_job("j1")] == [new, old]
        assert [r["_id"] for r in await history.list_recent(limit=2)] == [new, other]

    async def test_update_sets_the_outcome(self, history):
        record_id = await history.save(JobExecutionHistory(job_id="j1", status=JobStatus.RUNNING))

        await history.update(record_id, status=JobStatus.COMPLETED, completed_files=3)

        [record] = await history.list_by_job("j1")
        assert record["status"] == "completed"
        assert record["completed_files"] == 3

    async def test_cleanup_deletes_only_records_older_than_the_cutoff(self, history):
        now = datetime.utcnow()
        await history.save(JobExecutionHistory(job_id="j1", started_at=now - timedelta(days=31)))
        kept = await history.save(JobExecutionHistory(job_id="j1", started_at=now - timedelta(days=29)))

        assert await history.cleanup_old(days=30) == 1
        assert [r["_id"] for r in await history.list_recent()] == [kept]


# --- IngestionJobStorage -----------------------------------------------------

class TestIngestionJobStorage:
    def _request(self, name="index-docs"):
        return IngestionJobCreateRequest(
            name=name,
            source=SOURCE,
            vector_store=VectorStoreConfig(type="chroma", config={"path": "/chroma"}, namespace="docs"),
        )

    async def test_created_job_round_trips_nested_settings(self, ingestions):
        created = await ingestions.create(self._request())

        loaded = await ingestions.get(created.id)

        assert loaded.vector_store == created.vector_store
        assert loaded.embedding == created.embedding
        assert loaded.chunking == created.chunking
        assert loaded.graph == created.graph

    async def test_update_status_list_and_delete(self, ingestions):
        first = await ingestions.create(self._request("first"))
        await tick()
        second = await ingestions.create(self._request("second"))

        assert await ingestions.update_status(first.id, JobStatus.FAILED, "no embeddings")
        updated = await ingestions.update(second.id, IngestionJobUpdateRequest(batch_size=8))

        assert updated.batch_size == 8
        assert [j.id for j in await ingestions.list_jobs()] == [second.id, first.id]
        [failed] = await ingestions.list_jobs(status=JobStatus.FAILED)
        assert failed.error_message == "no embeddings"
        assert failed.completed_at is not None
        assert await ingestions.delete(first.id) is True
        assert await ingestions.get(first.id) is None

    async def test_update_of_a_missing_job_returns_none(self, ingestions):
        assert await ingestions.update(str(ObjectId()), IngestionJobUpdateRequest(name="x")) is None
