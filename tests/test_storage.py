"""
Tests for ``src.storage`` - the four MongoDB-backed storage classes.

No MongoDB is involved. ``src.storage.AsyncMongoClient`` is replaced by a small
in-memory fake (below) that supports exactly the operations the storage layer
uses. To keep the fake honest against real pymongo:

* every document, filter and update goes through a real BSON encode/decode
  round-trip, so anything that is not BSON-encodable fails, enums arrive as
  plain strings and datetimes are truncated to milliseconds (as on a server);
* every call is bound against the signature of the real ``AsyncCollection`` /
  ``AsyncCursor`` / ``AsyncDatabase`` method;
* results are real ``pymongo.results`` objects;
* unsupported query/update operators raise instead of silently matching.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable, Optional

import bson
import pytest
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import ConfigurationError, OperationFailure, ServerSelectionTimeoutError
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

import src.storage as storage_module
from src.integration.base import IntegrationType
from src.models import (
    ChunkingSettings,
    ConversionJob,
    ConversionStrategy,
    EmbeddingSettings,
    FileConversionResult,
    FileConversionStatus,
    GraphOntology,
    GraphOntologyRelation,
    GraphSettings,
    IngestionFileResult,
    IngestionFileStatus,
    IngestionJob,
    IngestionJobCreateRequest,
    IngestionJobUpdateRequest,
    IntegrationConfig,
    JobCreateRequest,
    JobExecutionHistory,
    JobLLMSettings,
    JobStatus,
    JobUpdateRequest,
    SavedConfiguration,
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    VectorStoreConfig,
)
from src.storage import (
    ConfigurationStorage,
    ExecutionHistoryStorage,
    IngestionJobStorage,
    JobStorage,
)

URI = "mongodb://fake-host:27017"
DB = "testdb"

#: Frozen "now" used by the storage layer in most tests (millisecond aligned,
#: so it survives a BSON round-trip unchanged).
NOW = datetime(2026, 3, 4, 5, 6, 7, 891000)

T1 = datetime(2026, 1, 1, 8, 0, 0)
T2 = datetime(2026, 1, 2, 8, 0, 0)
T3 = datetime(2026, 1, 3, 8, 0, 0)
T4 = datetime(2026, 1, 4, 8, 0, 0)


# =============================================================================
# In-memory fake of the pymongo async API used by src.storage
# =============================================================================

def _wire(document: dict) -> dict:
    """What MongoDB would store / return for ``document``: a BSON round-trip."""
    return bson.decode(bson.encode(document))


def _wire_args(args: tuple) -> tuple:
    return tuple(_wire(a) if isinstance(a, dict) else a for a in args)


def _bind(real_class: type, method: str, args: tuple, kwargs: dict) -> None:
    """Fail if the real pymongo method could not be called with these arguments."""
    inspect.signature(getattr(real_class, method)).bind(None, *args, **kwargs)


def _matches(doc: dict, query: dict) -> bool:
    for key, condition in query.items():
        if key.startswith("$"):
            raise NotImplementedError(f"fake MongoDB: top-level operator {key}")
        value = doc.get(key)
        if isinstance(condition, dict) and any(k.startswith("$") for k in condition):
            for operator, operand in condition.items():
                if operator == "$ne":
                    if value == operand:
                        return False
                elif operator == "$lt":
                    if value is None or not value < operand:
                        return False
                else:
                    raise NotImplementedError(f"fake MongoDB: query operator {operator}")
        elif value != condition:
            return False
    return True


def _apply_update(doc: dict, update: dict) -> dict:
    new = dict(doc)
    for operator, changes in update.items():
        if operator == "$set":
            new.update(changes)
        elif operator == "$inc":
            for field, amount in changes.items():
                new[field] = new.get(field, 0) + amount
        else:
            raise NotImplementedError(f"fake MongoDB: update operator {operator}")
    return new


class FakeCursor:
    def __init__(self, collection: "FakeCollection", query: dict):
        self._collection = collection
        self._query = query
        self._sort: Optional[tuple[str, int]] = None
        self._skip = 0
        self._limit = 0

    def sort(self, *args, **kwargs) -> "FakeCursor":
        self._collection._record(AsyncCursor, "sort", args, kwargs, label="cursor.sort")
        key, direction = args
        if not isinstance(key, str) or direction not in (1, -1):
            raise NotImplementedError("fake MongoDB: only single-key sorts are modelled")
        self._sort = (key, direction)
        return self

    def skip(self, *args, **kwargs) -> "FakeCursor":
        self._collection._record(AsyncCursor, "skip", args, kwargs, label="cursor.skip")
        (self._skip,) = args
        return self

    def limit(self, *args, **kwargs) -> "FakeCursor":
        self._collection._record(AsyncCursor, "limit", args, kwargs, label="cursor.limit")
        (self._limit,) = args
        return self

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        docs = [d for d in self._collection.docs.values() if _matches(d, self._query)]
        if self._sort:
            key, direction = self._sort
            docs.sort(key=lambda d: d[key], reverse=direction == -1)
        docs = docs[self._skip:]
        if self._limit:  # 0 means "no limit" in MongoDB
            docs = docs[: self._limit]
        for doc in docs:
            yield _wire(doc)  # fresh copy: callers mutate what they get back


class FakeCollection:
    """One collection: documents keyed by ``_id`` plus a log of every call."""

    def __init__(self, name: str):
        self.name = name
        self.docs: dict[ObjectId, dict] = {}
        self.calls: list[tuple[str, tuple, dict]] = []
        self.errors: dict[str, Exception] = {}

    # ---- test helpers ----

    def _record(self, real_class, method, args, kwargs, label=None) -> None:
        _bind(real_class, method, args, kwargs)
        label = label or method
        self.calls.append((label, _wire_args(args), kwargs))
        if label in self.errors:
            raise self.errors[label]

    def calls_to(self, method: str) -> list[tuple[tuple, dict]]:
        return [(args, kwargs) for name, args, kwargs in self.calls if name == method]

    def clear_calls(self) -> None:
        self.calls.clear()

    def seed(self, document: dict) -> str:
        """Insert a document directly (not logged); returns its id as a string."""
        oid = ObjectId()
        self.docs[oid] = _wire({**document, "_id": oid})
        return str(oid)

    def _first(self, query: dict) -> Optional[dict]:
        return next((d for d in self.docs.values() if _matches(d, query)), None)

    # ---- pymongo AsyncCollection API ----

    async def create_index(self, *args, **kwargs) -> str:
        self._record(AsyncCollection, "create_index", args, kwargs)
        keys = args[0]
        if isinstance(keys, str):
            keys = [(keys, 1)]
        return "_".join(f"{field}_{direction}" for field, direction in keys)

    async def insert_one(self, *args, **kwargs) -> InsertOneResult:
        self._record(AsyncCollection, "insert_one", args, kwargs)
        (document,) = args
        if "_id" not in document:
            document["_id"] = ObjectId()  # pymongo adds the id in place
        self.docs[document["_id"]] = _wire(document)
        return InsertOneResult(document["_id"], acknowledged=True)

    async def find_one(self, *args, **kwargs) -> Optional[dict]:
        self._record(AsyncCollection, "find_one", args, kwargs)
        (query,) = _wire_args(args) or ({},)
        found = self._first(query)
        return _wire(found) if found is not None else None

    def find(self, *args, **kwargs) -> FakeCursor:
        self._record(AsyncCollection, "find", args, kwargs)
        (query,) = _wire_args(args) or ({},)
        return FakeCursor(self, query)

    async def update_one(self, *args, **kwargs) -> UpdateResult:
        self._record(AsyncCollection, "update_one", args, kwargs)
        if kwargs:
            raise NotImplementedError(f"fake MongoDB: update_one options {kwargs}")
        query, update = _wire_args(args)
        found = self._first(query)
        if found is None:
            return UpdateResult({"n": 0, "nModified": 0}, acknowledged=True)
        new = _apply_update(found, update)
        self.docs[found["_id"]] = new
        return UpdateResult({"n": 1, "nModified": int(new != found)}, acknowledged=True)

    async def replace_one(self, *args, **kwargs) -> UpdateResult:
        self._record(AsyncCollection, "replace_one", args, kwargs)
        if kwargs:
            raise NotImplementedError(f"fake MongoDB: replace_one options {kwargs}")
        query, replacement = _wire_args(args)
        found = self._first(query)
        if found is None:
            return UpdateResult({"n": 0, "nModified": 0}, acknowledged=True)
        new = {"_id": found["_id"], **replacement}
        self.docs[found["_id"]] = new
        return UpdateResult({"n": 1, "nModified": int(new != found)}, acknowledged=True)

    async def find_one_and_update(self, *args, **kwargs) -> Optional[dict]:
        self._record(AsyncCollection, "find_one_and_update", args, kwargs)
        options = dict(kwargs)
        return_document = options.pop("return_document", ReturnDocument.BEFORE)
        if options:
            raise NotImplementedError(f"fake MongoDB: find_one_and_update options {options}")
        query, update = _wire_args(args)
        found = self._first(query)
        if found is None:
            return None
        new = _apply_update(found, update)
        self.docs[found["_id"]] = new
        return _wire(new if return_document == ReturnDocument.AFTER else found)

    async def delete_one(self, *args, **kwargs) -> DeleteResult:
        self._record(AsyncCollection, "delete_one", args, kwargs)
        (query,) = _wire_args(args)
        found = self._first(query)
        if found is None:
            return DeleteResult({"n": 0}, acknowledged=True)
        del self.docs[found["_id"]]
        return DeleteResult({"n": 1}, acknowledged=True)

    async def delete_many(self, *args, **kwargs) -> DeleteResult:
        self._record(AsyncCollection, "delete_many", args, kwargs)
        (query,) = _wire_args(args)
        doomed = [oid for oid, doc in self.docs.items() if _matches(doc, query)]
        for oid in doomed:
            del self.docs[oid]
        return DeleteResult({"n": len(doomed)}, acknowledged=True)

    async def count_documents(self, *args, **kwargs) -> int:
        self._record(AsyncCollection, "count_documents", args, kwargs)
        (query,) = _wire_args(args)
        return sum(1 for doc in self.docs.values() if _matches(doc, query))


class FakeDatabase:
    def __init__(self, server: "FakeMongoServer", name: str):
        self._server = server
        self.name = name

    def __getitem__(self, collection_name: str) -> FakeCollection:
        return self._server.collection(self.name, collection_name)

    async def command(self, *args, **kwargs) -> dict:
        _bind(AsyncDatabase, "command", args, kwargs)
        self._server.commands.append((self.name, args[0]))
        if self._server.ping_error is not None:
            raise self._server.ping_error
        return {"ok": 1.0}


class _CloseAwaitable:
    """Mirrors ``AsyncMongoClient.close()``: nothing happens unless it is awaited."""

    def __init__(self, client: "FakeMongoClient"):
        self._client = client

    def __await__(self):
        self._client.closed = True
        return
        yield  # pragma: no cover - makes this function a generator


class FakeMongoClient:
    def __init__(self, server: "FakeMongoServer", uri: str):
        self._server = server
        self.uri = uri
        self.close_calls = 0
        self.closed = False

    def __getitem__(self, database_name: str) -> FakeDatabase:
        return FakeDatabase(self._server, database_name)

    @property
    def admin(self) -> FakeDatabase:
        return self["admin"]

    def close(self) -> _CloseAwaitable:
        self.close_calls += 1
        return _CloseAwaitable(self)


class FakeMongoServer:
    """Shared state behind every fake client created during one test."""

    def __init__(self):
        self.clients: list[FakeMongoClient] = []
        self.commands: list[tuple[str, Any]] = []
        self.client_error: Optional[Exception] = None
        self.ping_error: Optional[Exception] = None
        self._collections: dict[tuple[str, str], FakeCollection] = {}

    def client_factory(self, *args, **kwargs) -> FakeMongoClient:
        _bind(AsyncMongoClient, "__init__", args, kwargs)
        if self.client_error is not None:
            raise self.client_error
        client = FakeMongoClient(self, *args, **kwargs)
        self.clients.append(client)
        return client

    def collection(self, database: str, name: str) -> FakeCollection:
        return self._collections.setdefault((database, name), FakeCollection(name))


# =============================================================================
# Fixtures and builders
# =============================================================================

class _FrozenDatetime(datetime):
    """``datetime`` whose clock always reads ``NOW``."""

    @classmethod
    def utcnow(cls):
        return NOW

    @classmethod
    def now(cls, tz=None):
        return NOW if tz is None else NOW.replace(tzinfo=timezone.utc).astimezone(tz)


@pytest.fixture(autouse=True)
def mongo(monkeypatch) -> FakeMongoServer:
    """Replace the pymongo client used by ``src.storage`` (for every test here)."""
    server = FakeMongoServer()
    monkeypatch.setattr(storage_module, "AsyncMongoClient", server.client_factory)
    return server


@pytest.fixture
def now(monkeypatch) -> datetime:
    """Freeze the storage layer's clock at ``NOW``."""
    monkeypatch.setattr(storage_module, "datetime", _FrozenDatetime)
    return NOW


@pytest.fixture
def errors(caplog):
    """Messages logged at ERROR level by ``src.storage``."""
    caplog.set_level(logging.ERROR, logger="src.storage")
    return lambda: [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


def _ms(value: datetime) -> datetime:
    """Truncate to the millisecond precision MongoDB stores."""
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def job_request(**overrides) -> JobCreateRequest:
    data: dict[str, Any] = {
        "name": "nightly",
        "source": IntegrationConfig(type="local", config={"path": "/data/in"}),
        "destination": IntegrationConfig(type="s3", config={"bucket": "out"}),
    }
    data.update(overrides)
    return JobCreateRequest(**data)


def conversion_job(**overrides) -> ConversionJob:
    data: dict[str, Any] = {
        "name": "nightly",
        "source": IntegrationConfig(type="local", config={"path": "/data/in"}),
        "destination": IntegrationConfig(type="s3", config={"bucket": "out"}),
    }
    data.update(overrides)
    return ConversionJob(**data)


def ingestion_request(**overrides) -> IngestionJobCreateRequest:
    data: dict[str, Any] = {
        "name": "index-docs",
        "source": IntegrationConfig(type="local", config={"path": "/data/md"}),
        "vector_store": VectorStoreConfig(type="chroma", config={"path": "/data/chroma"}),
    }
    data.update(overrides)
    return IngestionJobCreateRequest(**data)


def ingestion_job(**overrides) -> IngestionJob:
    data: dict[str, Any] = {
        "name": "index-docs",
        "source": IntegrationConfig(type="local", config={"path": "/data/md"}),
        "vector_store": VectorStoreConfig(type="chroma", config={"path": "/data/chroma"}),
    }
    data.update(overrides)
    return IngestionJob(**data)


def config_request(**overrides) -> SavedConfigurationCreate:
    data: dict[str, Any] = {"name": "My S3", "type": "s3", "config": {"bucket": "b"}}
    data.update(overrides)
    return SavedConfigurationCreate(**data)


def _document(model) -> dict:
    """The document the storage layer writes for ``model`` (no id)."""
    return model.model_dump(by_alias=True, exclude={"id"})


@pytest.fixture
async def jobs(mongo) -> JobStorage:
    storage = JobStorage(URI, DB)
    assert await storage.connect() is True
    mongo.collection(DB, JobStorage.COLLECTION_NAME).clear_calls()
    return storage


@pytest.fixture
def job_docs(mongo) -> FakeCollection:
    return mongo.collection(DB, JobStorage.COLLECTION_NAME)


@pytest.fixture
async def configs(mongo) -> ConfigurationStorage:
    storage = ConfigurationStorage(URI, DB)
    assert await storage.connect() is True
    mongo.collection(DB, ConfigurationStorage.COLLECTION_NAME).clear_calls()
    return storage


@pytest.fixture
def config_docs(mongo) -> FakeCollection:
    return mongo.collection(DB, ConfigurationStorage.COLLECTION_NAME)


@pytest.fixture
async def history(mongo) -> ExecutionHistoryStorage:
    storage = ExecutionHistoryStorage()
    await storage.connect(URI, DB)
    mongo.collection(DB, ExecutionHistoryStorage.COLLECTION_NAME).clear_calls()
    return storage


@pytest.fixture
def history_docs(mongo) -> FakeCollection:
    return mongo.collection(DB, ExecutionHistoryStorage.COLLECTION_NAME)


@pytest.fixture
async def ingestions(mongo) -> IngestionJobStorage:
    storage = IngestionJobStorage(URI, DB)
    assert await storage.connect() is True
    mongo.collection(DB, IngestionJobStorage.COLLECTION_NAME).clear_calls()
    return storage


@pytest.fixture
def ingestion_docs(mongo) -> FakeCollection:
    return mongo.collection(DB, IngestionJobStorage.COLLECTION_NAME)


# =============================================================================
# The fake must stay in step with the real pymongo API
# =============================================================================

class TestFakeMatchesPymongo:
    """If a pymongo upgrade breaks one of these, the fake above needs revisiting."""

    @pytest.mark.parametrize(
        "method",
        [
            "create_index", "insert_one", "find_one", "find", "update_one", "replace_one",
            "find_one_and_update", "delete_one", "delete_many", "count_documents",
        ],
    )
    def test_collection_methods_are_awaited_the_same_way(self, method):
        real = inspect.iscoroutinefunction(getattr(AsyncCollection, method))
        fake = inspect.iscoroutinefunction(getattr(FakeCollection, method))
        assert fake == real

    @pytest.mark.parametrize("method", ["sort", "skip", "limit"])
    def test_cursor_builders_are_synchronous(self, method):
        assert not inspect.iscoroutinefunction(getattr(AsyncCursor, method))

    def test_cursor_supports_async_iteration(self):
        assert hasattr(AsyncCursor, "__aiter__")

    def test_database_command_is_a_coroutine(self):
        assert inspect.iscoroutinefunction(AsyncDatabase.command)
        assert inspect.iscoroutinefunction(FakeDatabase.command)

    def test_client_close_must_be_awaited(self):
        assert inspect.iscoroutinefunction(AsyncMongoClient.close)

    def test_client_exposes_databases_by_item_and_attribute(self):
        assert hasattr(AsyncMongoClient, "__getitem__")
        assert hasattr(AsyncMongoClient, "__getattr__")  # ``client.admin``
        assert hasattr(AsyncDatabase, "__getitem__")

    def test_return_document_constants(self):
        assert ReturnDocument.AFTER is True
        assert ReturnDocument.BEFORE is False

    def test_result_objects_expose_the_counters_storage_reads(self):
        oid = ObjectId()
        assert InsertOneResult(oid, acknowledged=True).inserted_id == oid
        update = UpdateResult({"n": 1, "nModified": 0}, acknowledged=True)
        assert (update.matched_count, update.modified_count) == (1, 0)
        assert DeleteResult({"n": 3}, acknowledged=True).deleted_count == 3

    def test_storage_module_uses_the_async_client(self, monkeypatch):
        monkeypatch.undo()  # drop the autouse fake for this one check
        assert storage_module.AsyncMongoClient is AsyncMongoClient


# =============================================================================
# Connection handling shared by the three storages with connect() -> bool
# =============================================================================

CONNECTABLE = [
    pytest.param(
        JobStorage,
        ["status", "created_at", [("status", 1), ("created_at", -1)]],
        id="JobStorage",
    ),
    pytest.param(
        ConfigurationStorage,
        [
            "type", "is_source", "name",
            [("is_source", 1), ("type", 1)],
            [("is_source", 1), ("created_at", -1)],
        ],
        id="ConfigurationStorage",
    ),
    pytest.param(IngestionJobStorage, ["status", "created_at"], id="IngestionJobStorage"),
]


@pytest.mark.parametrize("storage_class, indexes", CONNECTABLE)
class TestConnection:
    async def test_connect_pings_server_and_creates_indexes(self, mongo, storage_class, indexes):
        storage = storage_class(URI, DB)

        assert await storage.connect() is True

        assert [client.uri for client in mongo.clients] == [URI]
        assert mongo.commands == [("admin", "ping")]
        collection = mongo.collection(DB, storage_class.COLLECTION_NAME)
        assert collection.calls_to("create_index") == [((keys,), {}) for keys in indexes]

    async def test_database_name_defaults_to_converter(self, mongo, storage_class, indexes):
        storage = storage_class(URI)

        assert await storage.connect() is True

        collection = mongo.collection("converter", storage_class.COLLECTION_NAME)
        assert len(collection.calls_to("create_index")) == len(indexes)

    async def test_connect_returns_false_when_server_is_unreachable(
        self, mongo, errors, storage_class, indexes
    ):
        mongo.ping_error = ServerSelectionTimeoutError("no servers available")

        assert await storage_class(URI, DB).connect() is False

        assert any("no servers available" in message for message in errors())
        assert mongo.collection(DB, storage_class.COLLECTION_NAME).calls == []

    async def test_connect_returns_false_when_uri_is_rejected(
        self, mongo, errors, storage_class, indexes
    ):
        mongo.client_error = ConfigurationError("bad uri")

        assert await storage_class("not-a-uri", DB).connect() is False

        assert any("bad uri" in message for message in errors())

    async def test_connect_returns_false_when_index_creation_fails(
        self, mongo, errors, storage_class, indexes
    ):
        collection = mongo.collection(DB, storage_class.COLLECTION_NAME)
        collection.errors["create_index"] = OperationFailure("not authorized")

        assert await storage_class(URI, DB).connect() is False

        assert any("not authorized" in message for message in errors())

    async def test_operations_are_refused_before_connect(self, storage_class, indexes):
        with pytest.raises(RuntimeError, match="[Nn]ot connected"):
            storage_class(URI, DB)._get_collection()

    async def test_disconnect_drops_the_connection(self, mongo, storage_class, indexes):
        storage = storage_class(URI, DB)
        await storage.connect()

        await storage.disconnect()

        assert mongo.clients[0].close_calls == 1
        with pytest.raises(RuntimeError, match="[Nn]ot connected"):
            storage._get_collection()

    async def test_disconnect_is_idempotent(self, mongo, storage_class, indexes):
        storage = storage_class(URI, DB)
        await storage.disconnect()  # never connected: nothing to do
        assert mongo.clients == []

        await storage.connect()
        await storage.disconnect()
        await storage.disconnect()

        assert mongo.clients[0].close_calls == 1

    async def test_reconnect_after_disconnect(self, mongo, storage_class, indexes):
        storage = storage_class(URI, DB)
        await storage.connect()
        await storage.disconnect()

        assert await storage.connect() is True

        assert len(mongo.clients) == 2
        assert storage._get_collection() is mongo.collection(DB, storage_class.COLLECTION_NAME)

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: disconnect() calls AsyncMongoClient.close() without awaiting it "
        "(close is a coroutine in pymongo's async API), so the client is never closed",
    )
    async def test_disconnect_really_closes_the_client(self, mongo, storage_class, indexes):
        storage = storage_class(URI, DB)
        await storage.connect()

        await storage.disconnect()

        assert mongo.clients[0].closed is True


# =============================================================================
# JobStorage
# =============================================================================

class TestJobStorageCreateAndGet:
    async def test_create_job_persists_the_request(self, jobs, job_docs):
        request = job_request(
            description="every night",
            source_extensions=[".pdf", ".docx"],
            source_folder="/in",
            destination_folder="/out",
            conversion_strategy=ConversionStrategy.BALANCED,
            schedule_cron="0 2 * * *",
            schedule_enabled=True,
        )

        job = await jobs.create_job(request)

        [(oid, stored)] = job_docs.docs.items()
        assert job.id == str(oid)
        assert "id" not in stored
        assert stored["name"] == "nightly"
        assert stored["description"] == "every night"
        assert stored["source"] == {"type": "local", "config": {"path": "/data/in"}}
        assert stored["destination"] == {"type": "s3", "config": {"bucket": "out"}}
        assert stored["source_extensions"] == [".pdf", ".docx"]
        assert stored["source_folder"] == "/in"
        assert stored["destination_folder"] == "/out"
        assert stored["conversion_strategy"] == "balanced"
        assert stored["schedule_cron"] == "0 2 * * *"
        assert stored["schedule_enabled"] is True
        assert stored["status"] == "pending"
        assert stored["file_results"] == []
        assert stored["stats"]["total_files"] == 0
        assert stored["started_at"] is None and stored["completed_at"] is None
        assert stored["created_at"] == _ms(job.created_at)

    async def test_create_job_lets_mongo_assign_the_id(self, jobs, job_docs):
        await jobs.create_job(job_request())

        [(args, kwargs)] = job_docs.calls_to("insert_one")
        (document,) = args
        assert "_id" not in document and "id" not in document
        assert kwargs == {}

    async def test_create_job_returns_a_pending_job(self, jobs):
        job = await jobs.create_job(job_request(name="first"))

        assert isinstance(job, ConversionJob)
        assert ObjectId.is_valid(job.id)
        assert job.name == "first"
        assert job.status == JobStatus.PENDING
        assert job.started_at is None and job.completed_at is None

    async def test_each_job_gets_its_own_id(self, jobs, job_docs):
        first = await jobs.create_job(job_request(name="a"))
        second = await jobs.create_job(job_request(name="b"))

        assert first.id != second.id
        assert len(job_docs.docs) == 2

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: JobStorage.create_job does not copy batch_size from the request "
        "(storage.py:97-108), so new jobs always get the default of 4",
    )
    async def test_create_job_keeps_batch_size(self, jobs, job_docs):
        job = await jobs.create_job(job_request(batch_size=16))

        assert job.batch_size == 16
        assert job_docs.docs[ObjectId(job.id)]["batch_size"] == 16

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: JobStorage.create_job does not copy llm_settings from the request "
        "(storage.py:97-108), so per-job LLM overrides are lost on creation",
    )
    async def test_create_job_keeps_llm_settings(self, jobs, job_docs):
        llm = JobLLMSettings(llm_provider="ollama", llm_model="llava", llm_max_pages=5)

        job = await jobs.create_job(job_request(llm_settings=llm))

        assert job.llm_settings == llm
        assert job_docs.docs[ObjectId(job.id)]["llm_settings"]["llm_model"] == "llava"

    async def test_get_job_round_trips_a_created_job(self, jobs, job_docs):
        created = await jobs.create_job(
            job_request(description="d", conversion_strategy="accurate", source_folder="/in")
        )

        fetched = await jobs.get_job(created.id)

        assert job_docs.calls_to("find_one") == [(({"_id": ObjectId(created.id)},), {})]
        timestamps = {"created_at", "updated_at"}
        assert fetched.id == created.id
        assert fetched.model_dump(exclude=timestamps) == created.model_dump(exclude=timestamps)
        assert fetched.created_at == _ms(created.created_at)
        assert fetched.updated_at == _ms(created.updated_at)

    async def test_get_job_returns_none_for_unknown_id(self, jobs):
        assert await jobs.get_job(str(ObjectId())) is None

    @pytest.mark.parametrize("bad_id", ["not-an-object-id", "", "123"])
    async def test_get_job_returns_none_for_malformed_id(self, jobs, job_docs, errors, bad_id):
        assert await jobs.get_job(bad_id) is None

        assert job_docs.calls == []  # never reached MongoDB
        assert len(errors()) == 1

    async def test_get_job_returns_none_when_not_connected(self, errors):
        assert await JobStorage(URI, DB).get_job(str(ObjectId())) is None

        assert any("Not connected" in message for message in errors())

    async def test_get_job_returns_none_when_mongo_fails(self, jobs, job_docs, errors):
        created = await jobs.create_job(job_request())
        job_docs.errors["find_one"] = OperationFailure("cursor killed")

        assert await jobs.get_job(created.id) is None

        assert any("cursor killed" in message for message in errors())


class TestJobStorageListAndCount:
    @pytest.fixture
    def seeded(self, jobs, job_docs) -> dict[str, str]:
        """Three jobs, oldest to newest: a (pending), b (running), c (pending)."""
        ids = {}
        for name, status, created in [
            ("a", JobStatus.PENDING, T1),
            ("b", JobStatus.RUNNING, T2),
            ("c", JobStatus.PENDING, T3),
        ]:
            job = conversion_job(name=name, status=status, created_at=created)
            ids[name] = job_docs.seed(_document(job))
        return ids

    async def test_list_jobs_returns_newest_first(self, jobs, job_docs, seeded):
        listed = await jobs.list_jobs()

        assert [job.name for job in listed] == ["c", "b", "a"]
        assert [job.id for job in listed] == [seeded["c"], seeded["b"], seeded["a"]]
        assert all(isinstance(job, ConversionJob) for job in listed)
        assert job_docs.calls == [
            ("find", ({},), {}),
            ("cursor.sort", ("created_at", -1), {}),
            ("cursor.skip", (0,), {}),
            ("cursor.limit", (50,), {}),
        ]

    async def test_list_jobs_pages_with_limit_and_offset(self, jobs, job_docs, seeded):
        listed = await jobs.list_jobs(limit=1, offset=1)

        assert [job.name for job in listed] == ["b"]
        assert job_docs.calls_to("cursor.skip") == [((1,), {})]
        assert job_docs.calls_to("cursor.limit") == [((1,), {})]

    @pytest.mark.parametrize("status", [JobStatus.PENDING, "pending"], ids=["enum", "string"])
    async def test_list_jobs_filters_by_status(self, jobs, job_docs, seeded, status):
        listed = await jobs.list_jobs(status=status)

        assert [job.name for job in listed] == ["c", "a"]
        assert job_docs.calls_to("find") == [(({"status": "pending"},), {})]

    async def test_list_jobs_on_empty_collection(self, jobs):
        assert await jobs.list_jobs() == []

    async def test_listing_does_not_alter_stored_documents(self, jobs, job_docs, seeded):
        await jobs.list_jobs()

        assert all(isinstance(oid, ObjectId) for oid in job_docs.docs)
        assert all(isinstance(doc["_id"], ObjectId) for doc in job_docs.docs.values())

    async def test_get_pending_jobs(self, jobs, job_docs, seeded):
        pending = await jobs.get_pending_jobs()

        assert [job.name for job in pending] == ["c", "a"]
        assert job_docs.calls_to("find") == [(({"status": "pending"},), {})]

    async def test_get_running_jobs(self, jobs, job_docs, seeded):
        running = await jobs.get_running_jobs()

        assert [job.name for job in running] == ["b"]
        assert job_docs.calls_to("find") == [(({"status": "running"},), {})]

    async def test_count_jobs(self, jobs, job_docs, seeded):
        assert await jobs.count_jobs() == 3
        assert await jobs.count_jobs(JobStatus.RUNNING) == 1
        assert await jobs.count_jobs("pending") == 2
        assert await jobs.count_jobs(JobStatus.FAILED) == 0

        assert job_docs.calls_to("count_documents") == [
            (({},), {}),
            (({"status": "running"},), {}),
            (({"status": "pending"},), {}),
            (({"status": "failed"},), {}),
        ]


class TestJobStorageUpdate:
    async def test_update_job_sets_only_the_provided_fields(self, jobs, job_docs, now):
        created = await jobs.create_job(job_request(description="keep me", schedule_enabled=True))
        job_docs.clear_calls()
        update = JobUpdateRequest(
            name="renamed",
            conversion_strategy=ConversionStrategy.ACCURATE,
            batch_size=8,
            schedule_enabled=False,  # falsy but explicitly set: must be written
            llm_settings=JobLLMSettings(llm_model="gpt-x"),
        )

        updated = await jobs.update_job(created.id, update)

        assert job_docs.calls_to("update_one") == [
            (
                (
                    {"_id": ObjectId(created.id)},
                    {
                        "$set": {
                            "name": "renamed",
                            "conversion_strategy": "accurate",
                            "batch_size": 8,
                            "schedule_enabled": False,
                            "llm_settings": {
                                "llm_provider": None,
                                "llm_model": "gpt-x",
                                "llm_api_key": None,
                                "llm_base_url": None,
                                "llm_max_pages": None,
                                "pdf_complexity_threshold": None,
                            },
                            "updated_at": now,
                        }
                    },
                ),
                {},
            )
        ]
        assert updated.id == created.id
        assert updated.name == "renamed"
        assert updated.description == "keep me"
        assert updated.conversion_strategy == ConversionStrategy.ACCURATE
        assert updated.batch_size == 8
        assert updated.schedule_enabled is False
        assert updated.llm_settings == JobLLMSettings(llm_model="gpt-x")
        assert updated.updated_at == now
        assert updated.created_at == _ms(created.created_at)

    async def test_update_job_can_replace_integrations_and_filters(self, jobs, job_docs):
        created = await jobs.create_job(job_request())

        updated = await jobs.update_job(
            created.id,
            JobUpdateRequest(
                source=IntegrationConfig(type=IntegrationType.SFTP, config={"host": "h"}),
                source_extensions=[".txt"],
                source_folder="/new",
                schedule_cron="*/5 * * * *",
            ),
        )

        stored = job_docs.docs[ObjectId(created.id)]
        assert stored["source"] == {"type": "sftp", "config": {"host": "h"}}
        assert stored["destination"] == {"type": "s3", "config": {"bucket": "out"}}
        assert updated.source.type == "sftp"
        assert updated.source_extensions == [".txt"]
        assert updated.source_folder == "/new"
        assert updated.schedule_cron == "*/5 * * * *"

    async def test_empty_update_only_touches_updated_at(self, jobs, job_docs, now):
        created = await jobs.create_job(job_request())
        job_docs.clear_calls()

        updated = await jobs.update_job(created.id, JobUpdateRequest())

        [(args, _)] = job_docs.calls_to("update_one")
        assert args[1] == {"$set": {"updated_at": now}}
        assert updated.name == created.name
        assert updated.updated_at == now

    async def test_update_job_returns_none_for_unknown_id(self, jobs, job_docs):
        assert await jobs.update_job(str(ObjectId()), JobUpdateRequest(name="x")) is None
        assert job_docs.docs == {}


class TestJobStorageSave:
    async def test_save_job_replaces_the_stored_document(self, jobs, job_docs, now):
        job = await jobs.create_job(job_request())
        job_docs.clear_calls()
        # The converter assigns enum members (not their values) while running.
        job.status = JobStatus.COMPLETED
        job.file_results = [
            FileConversionResult(
                source_path="/a.pdf",
                destination_path="/a.md",
                status=FileConversionStatus.COMPLETED,
                file_size=10,
                converted_size=4,
            ),
            FileConversionResult(
                source_path="/b.pdf", status=FileConversionStatus.FAILED, error_message="boom"
            ),
        ]
        job.update_stats()

        saved = await jobs.save_job(job)

        assert saved is job
        [(args, _)] = job_docs.calls_to("replace_one")
        query, replacement = args
        assert query == {"_id": ObjectId(job.id)}
        assert "_id" not in replacement and "id" not in replacement
        assert job_docs.calls_to("insert_one") == []
        assert list(job_docs.docs) == [ObjectId(job.id)]
        stored = job_docs.docs[ObjectId(job.id)]
        assert stored["status"] == "completed"
        assert [r["status"] for r in stored["file_results"]] == ["completed", "failed"]
        assert stored["stats"]["total_files"] == 2
        assert stored["stats"]["failed_files"] == 1
        assert stored["updated_at"] == now

    async def test_saved_job_can_be_read_back(self, jobs):
        job = await jobs.create_job(job_request())
        job.status = JobStatus.FAILED
        job.error_message = "source offline"
        job.file_results = [
            FileConversionResult(
                source_path="/a.pdf",
                status=FileConversionStatus.SKIPPED,
                started_at=T1,
                completed_at=T2,
            )
        ]
        job.update_stats()
        await jobs.save_job(job)

        reloaded = await jobs.get_job(job.id)

        assert reloaded.status == JobStatus.FAILED
        assert reloaded.error_message == "source offline"
        assert reloaded.stats == job.stats
        assert reloaded.file_results == job.file_results

    async def test_save_job_inserts_a_job_without_id(self, jobs, job_docs, now):
        job = conversion_job(name="fresh")

        saved = await jobs.save_job(job)

        assert saved is job
        [(oid, stored)] = job_docs.docs.items()
        assert job.id == str(oid)
        assert stored["name"] == "fresh"
        assert stored["updated_at"] == now
        assert "id" not in stored
        assert job_docs.calls_to("replace_one") == []
        [(args, _)] = job_docs.calls_to("insert_one")
        assert "_id" not in args[0]

    async def test_save_job_stamps_updated_at(self, jobs, now):
        job = conversion_job(updated_at=T1)

        await jobs.save_job(job)

        assert (await jobs.get_job(job.id)).updated_at == now


class TestJobStorageDelete:
    async def test_delete_job(self, jobs, job_docs):
        keep = await jobs.create_job(job_request(name="keep"))
        doomed = await jobs.create_job(job_request(name="doomed"))
        job_docs.clear_calls()

        assert await jobs.delete_job(doomed.id) is True

        assert job_docs.calls_to("delete_one") == [(({"_id": ObjectId(doomed.id)},), {})]
        assert list(job_docs.docs) == [ObjectId(keep.id)]
        assert await jobs.get_job(doomed.id) is None

    async def test_delete_job_reports_missing_job(self, jobs):
        job = await jobs.create_job(job_request())
        await jobs.delete_job(job.id)

        assert await jobs.delete_job(job.id) is False
        assert await jobs.delete_job(str(ObjectId())) is False


class TestJobStorageClaim:
    async def test_claim_marks_a_pending_job_as_running(self, jobs, job_docs, now):
        created = await jobs.create_job(job_request())
        job_docs.clear_calls()

        claimed = await jobs.claim_job_for_run(created.id)

        [(args, kwargs)] = job_docs.calls_to("find_one_and_update")
        assert args == (
            {"_id": ObjectId(created.id), "status": {"$ne": "running"}},
            {
                "$set": {
                    "status": "running",
                    "started_at": now,
                    "completed_at": None,
                    "error_message": None,
                    "updated_at": now,
                }
            },
        )
        assert kwargs == {"return_document": ReturnDocument.AFTER}
        # The returned job is the document *after* the update.
        assert claimed.id == created.id
        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at == now
        assert claimed.updated_at == now
        assert job_docs.docs[ObjectId(created.id)]["status"] == "running"

    @pytest.mark.parametrize(
        "previous", [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.PAUSED]
    )
    async def test_claim_resets_the_outcome_of_a_previous_run(self, jobs, job_docs, now, previous):
        job_id = job_docs.seed(
            _document(
                conversion_job(
                    status=previous, started_at=T1, completed_at=T2, error_message="old failure"
                )
            )
        )

        claimed = await jobs.claim_job_for_run(job_id)

        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at == now
        assert claimed.completed_at is None
        assert claimed.error_message is None

    async def test_running_job_cannot_be_claimed(self, jobs, job_docs, now):
        job_id = job_docs.seed(_document(conversion_job(status=JobStatus.RUNNING, started_at=T1)))
        before = dict(job_docs.docs[ObjectId(job_id)])

        assert await jobs.claim_job_for_run(job_id) is None

        assert job_docs.docs[ObjectId(job_id)] == before

    async def test_only_one_concurrent_claim_wins(self, jobs):
        created = await jobs.create_job(job_request())

        results = await asyncio.gather(
            *[jobs.claim_job_for_run(created.id) for _ in range(5)]
        )

        winners = [job for job in results if job is not None]
        assert len(winners) == 1
        assert winners[0].status == JobStatus.RUNNING

    async def test_job_can_be_claimed_again_once_finished(self, jobs):
        created = await jobs.create_job(job_request())
        assert await jobs.claim_job_for_run(created.id) is not None
        assert await jobs.claim_job_for_run(created.id) is None

        await jobs.update_job_status(created.id, JobStatus.COMPLETED)

        assert await jobs.claim_job_for_run(created.id) is not None

    async def test_claim_returns_none_for_unknown_id(self, jobs):
        assert await jobs.claim_job_for_run(str(ObjectId())) is None

    async def test_claim_returns_none_for_malformed_id(self, jobs, job_docs, errors):
        assert await jobs.claim_job_for_run("not-an-object-id") is None

        assert job_docs.calls == []
        assert len(errors()) == 1

    async def test_claim_returns_none_when_mongo_fails(self, jobs, job_docs, errors):
        created = await jobs.create_job(job_request())
        job_docs.errors["find_one_and_update"] = OperationFailure("write conflict")

        assert await jobs.claim_job_for_run(created.id) is None

        assert any("write conflict" in message for message in errors())
        assert job_docs.docs[ObjectId(created.id)]["status"] == "pending"


JOB_OPERATIONS: dict[str, Callable[[JobStorage], Any]] = {
    "create_job": lambda s: s.create_job(job_request()),
    "list_jobs": lambda s: s.list_jobs(),
    "update_job": lambda s: s.update_job(str(ObjectId()), JobUpdateRequest(name="x")),
    "save_job": lambda s: s.save_job(conversion_job()),
    "delete_job": lambda s: s.delete_job(str(ObjectId())),
    "update_job_status": lambda s: s.update_job_status(str(ObjectId()), JobStatus.FAILED),
    "claim_job_for_run": lambda s: s.claim_job_for_run(str(ObjectId())),
    "get_pending_jobs": lambda s: s.get_pending_jobs(),
    "get_running_jobs": lambda s: s.get_running_jobs(),
    "count_jobs": lambda s: s.count_jobs(),
}


@pytest.mark.parametrize("operation", JOB_OPERATIONS.values(), ids=JOB_OPERATIONS.keys())
async def test_job_storage_operations_require_a_connection(mongo, operation):
    with pytest.raises(RuntimeError, match="Not connected to MongoDB"):
        await operation(JobStorage(URI, DB))

    assert mongo.clients == []


JOB_WRITES_BY_ID: dict[str, Callable[[JobStorage, str], Any]] = {
    "update_job": lambda s, i: s.update_job(i, JobUpdateRequest(name="x")),
    "delete_job": lambda s, i: s.delete_job(i),
    "update_job_status": lambda s, i: s.update_job_status(i, JobStatus.FAILED),
}


@pytest.mark.parametrize("write", JOB_WRITES_BY_ID.values(), ids=JOB_WRITES_BY_ID.keys())
async def test_job_writes_with_malformed_id_change_nothing(jobs, job_docs, write):
    """Whether the id is rejected with InvalidId or reported as "not found",
    a malformed id must never reach MongoDB nor be reported as a success."""
    await jobs.create_job(job_request())
    before = {oid: dict(doc) for oid, doc in job_docs.docs.items()}
    job_docs.clear_calls()

    try:
        outcome = await write(jobs, "not-an-object-id")
    except InvalidId:
        outcome = None

    assert not outcome
    assert job_docs.calls == []
    assert job_docs.docs == before


# =============================================================================
# Behaviour shared by JobStorage and IngestionJobStorage
# =============================================================================

@pytest.fixture(params=["conversion", "ingestion"])
async def job_api(request, mongo) -> SimpleNamespace:
    """The operations both job storages offer, under common names."""
    if request.param == "conversion":
        storage = JobStorage(URI, DB)
        api = SimpleNamespace(
            storage=storage,
            make=conversion_job,
            get=storage.get_job,
            list=storage.list_jobs,
            update_status=storage.update_job_status,
            save=storage.save_job,
        )
    else:
        storage = IngestionJobStorage(URI, DB)
        api = SimpleNamespace(
            storage=storage,
            make=ingestion_job,
            get=storage.get,
            list=storage.list_jobs,
            update_status=storage.update_status,
            save=storage.save,
        )
    assert await storage.connect() is True
    api.docs = mongo.collection(DB, storage.COLLECTION_NAME)
    api.docs.clear_calls()
    return api


class TestStatusTransitions:
    @pytest.mark.parametrize(
        "status, stamped_field",
        [
            (JobStatus.RUNNING, "started_at"),
            (JobStatus.COMPLETED, "completed_at"),
            (JobStatus.FAILED, "completed_at"),
            (JobStatus.CANCELLED, "completed_at"),
            (JobStatus.PENDING, None),
            (JobStatus.PAUSED, None),
            ("running", "started_at"),
            ("failed", "completed_at"),
            ("paused", None),
        ],
    )
    async def test_status_update_stamps_the_matching_timestamp(
        self, job_api, now, status, stamped_field
    ):
        job_id = job_api.docs.seed(_document(job_api.make(created_at=T1, updated_at=T1)))

        assert await job_api.update_status(job_id, status) is True

        expected = {"status": JobStatus(status).value, "updated_at": now}
        if stamped_field:
            expected[stamped_field] = now
        assert job_api.docs.calls_to("update_one") == [
            (({"_id": ObjectId(job_id)}, {"$set": expected}), {})
        ]
        reloaded = await job_api.get(job_id)
        assert reloaded.status == JobStatus(status)
        assert reloaded.updated_at == now
        assert reloaded.started_at == (now if stamped_field == "started_at" else None)
        assert reloaded.completed_at == (now if stamped_field == "completed_at" else None)
        assert reloaded.created_at == T1
        assert reloaded.error_message is None

    async def test_error_message_is_recorded_with_the_status(self, job_api, now):
        job_id = job_api.docs.seed(_document(job_api.make()))

        assert await job_api.update_status(job_id, JobStatus.FAILED, "disk full") is True

        [(args, _)] = job_api.docs.calls_to("update_one")
        assert args[1] == {
            "$set": {
                "status": "failed",
                "updated_at": now,
                "completed_at": now,
                "error_message": "disk full",
            }
        }
        assert (await job_api.get(job_id)).error_message == "disk full"

    @pytest.mark.parametrize("message", [None, ""])
    async def test_error_message_is_left_alone_when_not_given(self, job_api, message):
        job_id = job_api.docs.seed(_document(job_api.make(error_message="earlier problem")))

        await job_api.update_status(job_id, JobStatus.CANCELLED, message)

        [(args, _)] = job_api.docs.calls_to("update_one")
        assert "error_message" not in args[1]["$set"]
        assert (await job_api.get(job_id)).error_message == "earlier problem"

    async def test_status_update_reports_unknown_job(self, job_api):
        assert await job_api.update_status(str(ObjectId()), JobStatus.RUNNING) is False
        assert job_api.docs.docs == {}

    async def test_status_update_leaves_other_jobs_alone(self, job_api):
        target = job_api.docs.seed(_document(job_api.make(name="target")))
        other = job_api.docs.seed(_document(job_api.make(name="other")))

        await job_api.update_status(target, JobStatus.RUNNING)

        assert (await job_api.get(other)).status == JobStatus.PENDING
        assert (await job_api.get(target)).status == JobStatus.RUNNING


class TestListingSharedBehaviour:
    async def test_listing_sorts_pages_and_filters(self, job_api):
        ids = [
            job_api.docs.seed(_document(job_api.make(name=name, status=status, created_at=created)))
            for name, status, created in [
                ("a", JobStatus.COMPLETED, T1),
                ("b", JobStatus.FAILED, T2),
                ("c", JobStatus.COMPLETED, T3),
                ("d", JobStatus.COMPLETED, T4),
            ]
        ]

        everything = await job_api.list()
        page = await job_api.list(status=JobStatus.COMPLETED, limit=2, offset=1)

        assert [job.id for job in everything] == list(reversed(ids))
        assert [job.name for job in page] == ["c", "a"]
        assert job_api.docs.calls[-4:] == [
            ("find", ({"status": "completed"},), {}),
            ("cursor.sort", ("created_at", -1), {}),
            ("cursor.skip", (1,), {}),
            ("cursor.limit", (2,), {}),
        ]

    async def test_save_then_list_round_trip(self, job_api, now):
        job = job_api.make(name="saved")

        await job_api.save(job)

        [listed] = await job_api.list()
        assert listed.id == job.id
        assert listed.name == "saved"
        assert listed.updated_at == now


# =============================================================================
# ConfigurationStorage
# =============================================================================

class TestConfigurationStorage:
    async def test_create_persists_the_configuration(self, configs, config_docs):
        request = config_request(
            name="Prod bucket",
            description="where reports land",
            type=IntegrationType.S3,
            config={"bucket": "reports", "nested": {"tags": ["a", "b"], "depth": 2}},
            is_source=False,
        )

        created = await configs.create(request)

        [(oid, stored)] = config_docs.docs.items()
        assert created.id == str(oid)
        assert isinstance(created, SavedConfiguration)
        assert stored == {
            "_id": oid,
            "name": "Prod bucket",
            "description": "where reports land",
            "type": "s3",
            "config": {"bucket": "reports", "nested": {"tags": ["a", "b"], "depth": 2}},
            "is_source": False,
            "created_at": _ms(created.created_at),
            "updated_at": _ms(created.updated_at),
            "last_used_at": None,
            "use_count": 0,
        }
        [(args, _)] = config_docs.calls_to("insert_one")
        assert "_id" not in args[0] and "id" not in args[0]

    async def test_get_round_trips_a_created_configuration(self, configs, config_docs):
        created = await configs.create(config_request(description="d"))
        config_docs.clear_calls()

        fetched = await configs.get(created.id)

        assert config_docs.calls_to("find_one") == [(({"_id": ObjectId(created.id)},), {})]
        timestamps = {"created_at", "updated_at"}
        assert fetched.model_dump(exclude=timestamps) == created.model_dump(exclude=timestamps)
        assert fetched.created_at == _ms(created.created_at)

    async def test_get_returns_none_for_unknown_or_malformed_id(self, configs, config_docs, errors):
        assert await configs.get(str(ObjectId())) is None
        assert errors() == []
        config_docs.clear_calls()

        assert await configs.get("not-an-object-id") is None

        assert config_docs.calls == []
        assert len(errors()) == 1

    async def test_get_returns_none_when_not_connected(self, errors):
        assert await ConfigurationStorage(URI, DB).get(str(ObjectId())) is None
        assert any("Not connected" in message for message in errors())

    @pytest.fixture
    def seeded(self, configs, config_docs) -> dict[str, str]:
        """Four configurations, oldest to newest."""
        ids = {}
        for name, is_source, type_, created in [
            ("src-local", True, "local", T1),
            ("src-s3", True, "s3", T2),
            ("dst-s3", False, "s3", T3),
            ("dst-local", False, "local", T4),
        ]:
            config = SavedConfiguration(
                name=name, type=type_, is_source=is_source, created_at=created
            )
            ids[name] = config_docs.seed(_document(config))
        return ids

    async def test_list_all_returns_newest_first(self, configs, config_docs, seeded):
        listed = await configs.list_all()

        assert [c.name for c in listed] == ["dst-local", "dst-s3", "src-s3", "src-local"]
        assert [c.id for c in listed] == [
            seeded["dst-local"], seeded["dst-s3"], seeded["src-s3"], seeded["src-local"]
        ]
        assert config_docs.calls == [
            ("find", ({},), {}),
            ("cursor.sort", ("created_at", -1), {}),
            ("cursor.skip", (0,), {}),
            ("cursor.limit", (100,), {}),
        ]

    @pytest.mark.parametrize(
        "filters, expected_query, expected_names",
        [
            ({"is_source": True}, {"is_source": True}, ["src-s3", "src-local"]),
            # False is a real filter value, not "no filter".
            ({"is_source": False}, {"is_source": False}, ["dst-local", "dst-s3"]),
            ({"integration_type": "s3"}, {"type": "s3"}, ["dst-s3", "src-s3"]),
            (
                {"is_source": False, "integration_type": "local"},
                {"is_source": False, "type": "local"},
                ["dst-local"],
            ),
            ({"integration_type": ""}, {}, ["dst-local", "dst-s3", "src-s3", "src-local"]),
            ({"integration_type": "sftp"}, {"type": "sftp"}, []),
        ],
    )
    async def test_list_all_filters(
        self, configs, config_docs, seeded, filters, expected_query, expected_names
    ):
        listed = await configs.list_all(**filters)

        assert [c.name for c in listed] == expected_names
        assert config_docs.calls_to("find") == [((expected_query,), {})]

    async def test_list_all_pages(self, configs, config_docs, seeded):
        listed = await configs.list_all(limit=2, offset=1)

        assert [c.name for c in listed] == ["dst-s3", "src-s3"]
        assert config_docs.calls_to("cursor.skip") == [((1,), {})]
        assert config_docs.calls_to("cursor.limit") == [((2,), {})]

    async def test_list_sources_and_destinations(self, configs, config_docs, seeded):
        assert [c.name for c in await configs.list_sources()] == ["src-s3", "src-local"]
        assert [c.name for c in await configs.list_sources("local")] == ["src-local"]
        assert [c.name for c in await configs.list_destinations()] == ["dst-local", "dst-s3"]
        assert [c.name for c in await configs.list_destinations("s3")] == ["dst-s3"]

        assert [args[0] for args, _ in config_docs.calls_to("find")] == [
            {"is_source": True},
            {"is_source": True, "type": "local"},
            {"is_source": False},
            {"is_source": False, "type": "s3"},
        ]

    async def test_update_sets_only_the_provided_fields(self, configs, config_docs, now):
        created = await configs.create(config_request(description="keep me"))
        config_docs.clear_calls()

        updated = await configs.update(
            created.id, SavedConfigurationUpdate(name="renamed", config={"bucket": "other"})
        )

        assert config_docs.calls_to("update_one") == [
            (
                (
                    {"_id": ObjectId(created.id)},
                    {"$set": {"name": "renamed", "config": {"bucket": "other"}, "updated_at": now}},
                ),
                {},
            )
        ]
        assert updated.id == created.id
        assert updated.name == "renamed"
        assert updated.config == {"bucket": "other"}
        assert updated.description == "keep me"
        assert updated.type == "s3"
        assert updated.updated_at == now

    async def test_update_can_empty_the_config(self, configs):
        created = await configs.create(config_request())

        updated = await configs.update(created.id, SavedConfigurationUpdate(config={}))

        assert updated.config == {}

    async def test_update_without_changes_writes_nothing(self, configs, config_docs):
        created = await configs.create(config_request())
        config_docs.clear_calls()

        unchanged = await configs.update(created.id, SavedConfigurationUpdate())

        assert config_docs.calls_to("update_one") == []
        assert unchanged.id == created.id
        assert unchanged.updated_at == _ms(created.updated_at)

    async def test_update_returns_none_for_unknown_id(self, configs, config_docs):
        unknown = str(ObjectId())

        assert await configs.update(unknown, SavedConfigurationUpdate(name="x")) is None
        assert await configs.update(unknown, SavedConfigurationUpdate()) is None
        assert config_docs.docs == {}

    async def test_delete(self, configs, config_docs):
        keep = await configs.create(config_request(name="keep"))
        doomed = await configs.create(config_request(name="doomed"))
        config_docs.clear_calls()

        assert await configs.delete(doomed.id) is True
        assert await configs.delete(doomed.id) is False

        assert config_docs.calls_to("delete_one") == [
            (({"_id": ObjectId(doomed.id)},), {}),
            (({"_id": ObjectId(doomed.id)},), {}),
        ]
        assert list(config_docs.docs) == [ObjectId(keep.id)]

    async def test_increment_use_count(self, configs, config_docs, now):
        created = await configs.create(config_request())
        config_docs.clear_calls()

        assert await configs.increment_use_count(created.id) is True
        assert await configs.increment_use_count(created.id) is True

        assert config_docs.calls_to("update_one")[0] == (
            (
                {"_id": ObjectId(created.id)},
                {"$inc": {"use_count": 1}, "$set": {"last_used_at": now}},
            ),
            {},
        )
        reloaded = await configs.get(created.id)
        assert reloaded.use_count == 2
        assert reloaded.last_used_at == now
        assert reloaded.updated_at == _ms(created.updated_at)

    async def test_increment_use_count_reports_unknown_id(self, configs):
        assert await configs.increment_use_count(str(ObjectId())) is False

    async def test_count(self, configs, config_docs, seeded):
        await configs.create(config_request(is_source=True))
        config_docs.clear_calls()

        assert await configs.count() == 5
        assert await configs.count(is_source=True) == 3
        assert await configs.count(is_source=False) == 2

        assert config_docs.calls_to("count_documents") == [
            (({},), {}),
            (({"is_source": True},), {}),
            (({"is_source": False},), {}),
        ]

    @pytest.mark.parametrize(
        "operation",
        [
            lambda s: s.create(config_request()),
            lambda s: s.list_all(),
            lambda s: s.list_sources(),
            lambda s: s.list_destinations(),
            lambda s: s.update(str(ObjectId()), SavedConfigurationUpdate(name="x")),
            lambda s: s.delete(str(ObjectId())),
            lambda s: s.increment_use_count(str(ObjectId())),
            lambda s: s.count(),
        ],
        ids=[
            "create", "list_all", "list_sources", "list_destinations",
            "update", "delete", "increment_use_count", "count",
        ],
    )
    async def test_operations_require_a_connection(self, mongo, operation):
        with pytest.raises(RuntimeError, match="Not connected to MongoDB"):
            await operation(ConfigurationStorage(URI, DB))

        assert mongo.clients == []


# =============================================================================
# ExecutionHistoryStorage
# =============================================================================

class TestExecutionHistoryStorage:
    async def test_connect_creates_indexes(self, mongo):
        storage = ExecutionHistoryStorage()

        await storage.connect(URI, DB)

        assert [client.uri for client in mongo.clients] == [URI]
        collection = mongo.collection(DB, "job_execution_history")
        assert collection.calls_to("create_index") == [(("job_id",), {}), (("started_at",), {})]

    async def test_connect_propagates_mongo_errors(self, mongo):
        collection = mongo.collection(DB, "job_execution_history")
        collection.errors["create_index"] = ServerSelectionTimeoutError("no servers available")

        with pytest.raises(ServerSelectionTimeoutError):
            await ExecutionHistoryStorage().connect(URI, DB)

    @pytest.mark.parametrize(
        "operation",
        [
            lambda s: s.save(JobExecutionHistory(job_id="j")),
            lambda s: s.update(str(ObjectId()), status="failed"),
            lambda s: s.list_by_job("j"),
            lambda s: s.list_recent(),
            lambda s: s.cleanup_old(),
        ],
        ids=["save", "update", "list_by_job", "list_recent", "cleanup_old"],
    )
    async def test_operations_require_a_connection(self, mongo, operation):
        with pytest.raises(RuntimeError, match="ExecutionHistoryStorage not connected"):
            await operation(ExecutionHistoryStorage())

        assert mongo.clients == []

    async def test_save_stores_the_record_and_returns_its_id(self, history, history_docs):
        record = JobExecutionHistory(
            job_id="64b7f0c2a1b2c3d4e5f60718",
            job_name="nightly",
            trigger="scheduled",
            status=JobStatus.RUNNING,
            started_at=T1,
        )

        record_id = await history.save(record)

        [(oid, stored)] = history_docs.docs.items()
        assert record_id == str(oid)
        assert stored == {
            "_id": oid,
            "job_id": "64b7f0c2a1b2c3d4e5f60718",
            "job_name": "nightly",
            "trigger": "scheduled",
            "status": "running",
            "started_at": T1,
            "completed_at": None,
            "total_files": 0,
            "completed_files": 0,
            "failed_files": 0,
            "error_message": None,
        }
        [(args, _)] = history_docs.calls_to("insert_one")
        assert "_id" not in args[0] and "id" not in args[0]

    async def test_update_sets_the_given_fields(self, history, history_docs):
        record_id = await history.save(JobExecutionHistory(job_id="j1", started_at=T1))
        history_docs.clear_calls()

        await history.update(
            record_id,
            status=JobStatus.COMPLETED,  # callers pass the enum member
            completed_at=T2,
            total_files=3,
            completed_files=2,
            failed_files=1,
            error_message=None,
        )

        assert history_docs.calls_to("update_one") == [
            (
                (
                    {"_id": ObjectId(record_id)},
                    {
                        "$set": {
                            "status": "completed",
                            "completed_at": T2,
                            "total_files": 3,
                            "completed_files": 2,
                            "failed_files": 1,
                            "error_message": None,
                        }
                    },
                ),
                {},
            )
        ]
        stored = history_docs.docs[ObjectId(record_id)]
        assert stored["status"] == "completed"
        assert stored["completed_at"] == T2
        assert (stored["total_files"], stored["completed_files"], stored["failed_files"]) == (3, 2, 1)
        assert stored["job_id"] == "j1" and stored["started_at"] == T1
        # What is stored still validates as a history record.
        record = JobExecutionHistory.model_validate({**stored, "_id": record_id})
        assert record.status == JobStatus.COMPLETED

    @pytest.fixture
    async def seeded(self, history, history_docs) -> dict[str, str]:
        ids = {}
        for label, job_id, started in [
            ("j1-old", "j1", T1),
            ("j2", "j2", T2),
            ("j1-mid", "j1", T3),
            ("j1-new", "j1", T4),
        ]:
            ids[label] = await history.save(
                JobExecutionHistory(job_id=job_id, job_name=label, started_at=started)
            )
        history_docs.clear_calls()
        return ids

    async def test_list_by_job_returns_that_jobs_runs_newest_first(
        self, history, history_docs, seeded
    ):
        runs = await history.list_by_job("j1")

        assert [run["job_name"] for run in runs] == ["j1-new", "j1-mid", "j1-old"]
        assert [run["_id"] for run in runs] == [seeded["j1-new"], seeded["j1-mid"], seeded["j1-old"]]
        assert all(isinstance(run, dict) for run in runs)
        assert history_docs.calls == [
            ("find", ({"job_id": "j1"},), {}),
            ("cursor.sort", ("started_at", -1), {}),
            ("cursor.limit", (50,), {}),
        ]

    async def test_list_by_job_honours_limit(self, history, history_docs, seeded):
        runs = await history.list_by_job("j1", limit=2)

        assert [run["job_name"] for run in runs] == ["j1-new", "j1-mid"]
        assert history_docs.calls_to("cursor.limit") == [((2,), {})]

    async def test_list_by_job_with_no_runs(self, history, seeded):
        assert await history.list_by_job("unknown") == []

    async def test_list_recent_returns_all_jobs_newest_first(self, history, history_docs, seeded):
        runs = await history.list_recent()

        assert [run["job_name"] for run in runs] == ["j1-new", "j1-mid", "j2", "j1-old"]
        assert all(isinstance(run["_id"], str) for run in runs)
        assert history_docs.calls == [
            ("find", (), {}),
            ("cursor.sort", ("started_at", -1), {}),
            ("cursor.limit", (100,), {}),
        ]

    async def test_list_recent_honours_limit(self, history, seeded):
        runs = await history.list_recent(limit=1)

        assert [run["job_name"] for run in runs] == ["j1-new"]

    async def test_listed_records_are_json_friendly_copies(self, history, history_docs, seeded):
        runs = await history.list_recent()
        runs[0]["job_name"] = "mutated"

        assert all(isinstance(doc["_id"], ObjectId) for doc in history_docs.docs.values())
        assert "mutated" not in [doc["job_name"] for doc in history_docs.docs.values()]

    async def test_cleanup_old_deletes_records_older_than_the_cutoff(
        self, history, history_docs, now
    ):
        cutoff = now - timedelta(days=30)
        for label, started in [
            ("ancient", cutoff - timedelta(days=300)),
            ("just-too-old", cutoff - timedelta(seconds=1)),
            ("exactly-at-cutoff", cutoff),
            ("recent", now - timedelta(days=1)),
        ]:
            await history.save(JobExecutionHistory(job_id="j", job_name=label, started_at=started))
        history_docs.clear_calls()

        deleted = await history.cleanup_old()

        assert deleted == 2
        assert history_docs.calls_to("delete_many") == [
            (({"started_at": {"$lt": cutoff}},), {})
        ]
        remaining = [doc["job_name"] for doc in history_docs.docs.values()]
        assert remaining == ["exactly-at-cutoff", "recent"]

    async def test_cleanup_old_with_custom_retention(self, history, history_docs, now):
        for days_ago in (1, 5, 10):
            await history.save(
                JobExecutionHistory(job_id="j", started_at=now - timedelta(days=days_ago))
            )
        history_docs.clear_calls()

        assert await history.cleanup_old(days=3) == 2

        assert history_docs.calls_to("delete_many") == [
            (({"started_at": {"$lt": now - timedelta(days=3)}},), {})
        ]
        assert len(history_docs.docs) == 1

    async def test_cleanup_old_with_nothing_to_delete(self, history, now):
        await history.save(JobExecutionHistory(job_id="j", started_at=now))

        assert await history.cleanup_old() == 0


# =============================================================================
# IngestionJobStorage
# =============================================================================

class TestIngestionJobStorage:
    def _full_request(self) -> IngestionJobCreateRequest:
        return ingestion_request(
            name="kb",
            description="knowledge base",
            vector_store=VectorStoreConfig(
                type="neo4j", config={"uri": "bolt://x"}, namespace="kb"
            ),
            source_folder="/docs",
            source_extensions=[".md", ".markdown"],
            embedding=EmbeddingSettings(
                provider="ollama", model="nomic", base_url="http://localhost:11434/v1", dimensions=768
            ),
            chunking=ChunkingSettings(strategy="recursive", chunk_size=500, chunk_overlap=50),
            graph=GraphSettings(
                enabled=True,
                mode="schema_guided",
                ontology=GraphOntology(
                    node_labels=["Person", "Company"],
                    relations=[
                        GraphOntologyRelation(type="WORKS_AT", source=["Person"], target=["Company"])
                    ],
                    node_properties={"Person": ["role"]},
                ),
                llm_model="gpt-x",
            ),
            batch_size=8,
            delete_orphans=True,
            schedule_cron="0 3 * * *",
            schedule_enabled=True,
        )

    async def test_create_persists_every_request_field(self, ingestions, ingestion_docs):
        request = self._full_request()

        job = await ingestions.create(request)

        [(oid, stored)] = ingestion_docs.docs.items()
        assert job.id == str(oid)
        assert isinstance(job, IngestionJob)
        for field, value in request.model_dump().items():
            assert stored[field] == value, field
            assert job.model_dump()[field] == value, field
        assert stored["vector_store"]["type"] == "neo4j"
        assert stored["batch_size"] == 8
        assert stored["delete_orphans"] is True
        assert stored["status"] == "pending"
        assert stored["file_results"] == []
        assert stored["stats"]["total_documents"] == 0
        assert "id" not in stored
        [(args, _)] = ingestion_docs.calls_to("insert_one")
        assert "_id" not in args[0] and "id" not in args[0]

    async def test_get_round_trips_a_created_job(self, ingestions, ingestion_docs):
        created = await ingestions.create(self._full_request())
        ingestion_docs.clear_calls()

        fetched = await ingestions.get(created.id)

        assert ingestion_docs.calls_to("find_one") == [(({"_id": ObjectId(created.id)},), {})]
        timestamps = {"created_at", "updated_at"}
        assert fetched.model_dump(exclude=timestamps) == created.model_dump(exclude=timestamps)
        assert fetched.created_at == _ms(created.created_at)

    async def test_get_returns_none_for_unknown_or_malformed_id(
        self, ingestions, ingestion_docs, errors
    ):
        assert await ingestions.get(str(ObjectId())) is None
        assert errors() == []
        ingestion_docs.clear_calls()

        assert await ingestions.get("not-an-object-id") is None

        assert ingestion_docs.calls == []
        assert len(errors()) == 1

    async def test_get_returns_none_when_not_connected(self, errors):
        assert await IngestionJobStorage(URI, DB).get(str(ObjectId())) is None
        assert any("not connected" in message for message in errors())

    @pytest.mark.parametrize("status", [JobStatus.RUNNING, "running"], ids=["enum", "string"])
    async def test_list_jobs_filters_by_status(self, ingestions, ingestion_docs, status):
        for name, job_status in [("a", JobStatus.RUNNING), ("b", JobStatus.PENDING)]:
            ingestion_docs.seed(_document(ingestion_job(name=name, status=job_status)))

        listed = await ingestions.list_jobs(status=status)

        assert [job.name for job in listed] == ["a"]
        assert all(isinstance(job, IngestionJob) for job in listed)
        assert ingestion_docs.calls_to("find") == [(({"status": "running"},), {})]

    async def test_list_jobs_defaults(self, ingestions, ingestion_docs):
        assert await ingestions.list_jobs() == []

        assert ingestion_docs.calls == [
            ("find", ({},), {}),
            ("cursor.sort", ("created_at", -1), {}),
            ("cursor.skip", (0,), {}),
            ("cursor.limit", (50,), {}),
        ]

    async def test_update_sets_only_the_provided_fields(self, ingestions, ingestion_docs, now):
        created = await ingestions.create(self._full_request())
        ingestion_docs.clear_calls()
        update = IngestionJobUpdateRequest(
            name="renamed",
            batch_size=2,
            delete_orphans=False,  # falsy but explicitly set: must be written
            schedule_enabled=False,
            embedding=EmbeddingSettings(provider="openai", model="text-embedding-3-large"),
        )

        updated = await ingestions.update(created.id, update)

        assert ingestion_docs.calls_to("update_one") == [
            (
                (
                    {"_id": ObjectId(created.id)},
                    {
                        "$set": {
                            "name": "renamed",
                            "batch_size": 2,
                            "delete_orphans": False,
                            "schedule_enabled": False,
                            "embedding": {"provider": "openai", "model": "text-embedding-3-large"},
                            "updated_at": now,
                        }
                    },
                ),
                {},
            )
        ]
        assert updated.id == created.id
        assert updated.name == "renamed"
        assert updated.batch_size == 2
        assert updated.delete_orphans is False
        assert updated.schedule_enabled is False
        assert updated.embedding == EmbeddingSettings(model="text-embedding-3-large")
        assert updated.updated_at == now
        # Everything not mentioned in the update is untouched.
        assert updated.description == "knowledge base"
        assert updated.chunking == created.chunking
        assert updated.graph == created.graph
        assert updated.vector_store == created.vector_store
        assert updated.schedule_cron == "0 3 * * *"

    async def test_update_can_replace_nested_settings(self, ingestions):
        created = await ingestions.create(ingestion_request())

        updated = await ingestions.update(
            created.id,
            IngestionJobUpdateRequest(
                vector_store=VectorStoreConfig(type="redis", config={"url": "redis://x"}),
                chunking=ChunkingSettings(strategy="fixed", chunk_size=100, chunk_overlap=0),
                graph=GraphSettings(enabled=True),
                source_extensions=[".mdx"],
            ),
        )

        assert updated.vector_store.type == "redis"
        assert updated.vector_store.namespace == "default"
        assert updated.chunking == ChunkingSettings(strategy="fixed", chunk_size=100, chunk_overlap=0)
        assert updated.graph.enabled is True
        assert updated.source_extensions == [".mdx"]

    async def test_empty_update_only_touches_updated_at(self, ingestions, ingestion_docs, now):
        created = await ingestions.create(ingestion_request())
        ingestion_docs.clear_calls()

        updated = await ingestions.update(created.id, IngestionJobUpdateRequest())

        [(args, _)] = ingestion_docs.calls_to("update_one")
        assert args[1] == {"$set": {"updated_at": now}}
        assert updated.name == created.name
        assert updated.updated_at == now

    async def test_update_returns_none_for_unknown_id(self, ingestions, ingestion_docs):
        update = IngestionJobUpdateRequest(name="x")

        assert await ingestions.update(str(ObjectId()), update) is None
        assert ingestion_docs.docs == {}

    async def test_save_replaces_the_stored_document(self, ingestions, ingestion_docs, now):
        job = await ingestions.create(ingestion_request())
        ingestion_docs.clear_calls()
        # The ingestion service assigns enum members while running.
        job.status = JobStatus.COMPLETED
        job.completed_at = T2
        job.file_results = [
            IngestionFileResult(
                source_path="/a.md",
                doc_id="a.md",
                status=IngestionFileStatus.COMPLETED,
                chunk_count=3,
                entity_count=2,
                relation_count=1,
            ),
            IngestionFileResult(
                source_path="/b.md", status=IngestionFileStatus.FAILED, error_message="boom"
            ),
        ]
        job.update_stats()

        saved = await ingestions.save(job)

        assert saved is job
        [(args, _)] = ingestion_docs.calls_to("replace_one")
        query, replacement = args
        assert query == {"_id": ObjectId(job.id)}
        assert "_id" not in replacement and "id" not in replacement
        assert ingestion_docs.calls_to("insert_one") == []
        stored = ingestion_docs.docs[ObjectId(job.id)]
        assert stored["status"] == "completed"
        assert [r["status"] for r in stored["file_results"]] == ["completed", "failed"]
        assert stored["stats"]["total_chunks"] == 3
        assert stored["updated_at"] == now
        reloaded = await ingestions.get(job.id)
        assert reloaded.status == JobStatus.COMPLETED
        assert reloaded.completed_at == T2
        assert reloaded.stats == job.stats
        assert reloaded.file_results == job.file_results

    async def test_save_inserts_a_job_without_id(self, ingestions, ingestion_docs, now):
        job = ingestion_job(name="fresh")

        saved = await ingestions.save(job)

        assert saved is job
        [(oid, stored)] = ingestion_docs.docs.items()
        assert job.id == str(oid)
        assert stored["name"] == "fresh"
        assert stored["updated_at"] == now
        assert ingestion_docs.calls_to("replace_one") == []

    async def test_delete(self, ingestions, ingestion_docs):
        keep = await ingestions.create(ingestion_request(name="keep"))
        doomed = await ingestions.create(ingestion_request(name="doomed"))
        ingestion_docs.clear_calls()

        assert await ingestions.delete(doomed.id) is True
        assert await ingestions.delete(doomed.id) is False
        assert await ingestions.delete(str(ObjectId())) is False

        assert ingestion_docs.calls_to("delete_one")[0] == (({"_id": ObjectId(doomed.id)},), {})
        assert list(ingestion_docs.docs) == [ObjectId(keep.id)]

    @pytest.mark.parametrize(
        "operation",
        [
            lambda s: s.create(ingestion_request()),
            lambda s: s.list_jobs(),
            lambda s: s.update(str(ObjectId()), IngestionJobUpdateRequest(name="x")),
            lambda s: s.save(ingestion_job()),
            lambda s: s.delete(str(ObjectId())),
            lambda s: s.update_status(str(ObjectId()), JobStatus.FAILED),
        ],
        ids=["create", "list_jobs", "update", "save", "delete", "update_status"],
    )
    async def test_operations_require_a_connection(self, mongo, operation):
        with pytest.raises(RuntimeError, match="IngestionJobStorage not connected"):
            await operation(IngestionJobStorage(URI, DB))

        assert mongo.clients == []

    @pytest.mark.parametrize(
        "write",
        [
            lambda s, i: s.update(i, IngestionJobUpdateRequest(name="x")),
            lambda s, i: s.delete(i),
            lambda s, i: s.update_status(i, JobStatus.FAILED),
        ],
        ids=["update", "delete", "update_status"],
    )
    async def test_writes_with_malformed_id_change_nothing(self, ingestions, ingestion_docs, write):
        await ingestions.create(ingestion_request())
        before = {oid: dict(doc) for oid, doc in ingestion_docs.docs.items()}
        ingestion_docs.clear_calls()

        try:
            outcome = await write(ingestions, "not-an-object-id")
        except InvalidId:
            outcome = None

        assert not outcome
        assert ingestion_docs.calls == []
        assert ingestion_docs.docs == before
