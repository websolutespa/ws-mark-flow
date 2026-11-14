"""
Fixtures for live tests against a real MongoDB (``pytest --live``).

Connection: ``MONGODB_URI`` / ``MONGODB_DATABASE`` from the shell environment,
else those two keys only from the repository ``.env`` (no other key is read),
else the Settings defaults.

Every test starts on a fresh ``{MONGODB_DATABASE}-tests-live`` database, which
is also dropped when the session ends: the configured database is never touched.
"""
from dataclasses import dataclass
from pathlib import Path

import pytest
from dotenv import dotenv_values
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src import main
from src.config import Settings
from src.storage import (
    ConfigurationStorage,
    ExecutionHistoryStorage,
    IngestionJobStorage,
    JobStorage,
)
from tests.conftest import LIVE_ENV

ENV_FILE = Path(__file__).resolve().parents[2] / ".env"
LIVE_DB_SUFFIX = "-tests-live"


@dataclass(frozen=True)
class LiveMongo:
    uri: str
    database: str


def _setting(key: str) -> str:
    value = LIVE_ENV.get(key) or dotenv_values(ENV_FILE).get(key)
    return value or Settings.model_fields[key.lower()].default


@pytest.fixture(scope="session")
def _live_mongo_client():
    uri = _setting("MONGODB_URI")
    database = _setting("MONGODB_DATABASE") + LIVE_DB_SUFFIX
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        client.close()
        pytest.fail(
            f"--live: MongoDB is not reachable ({type(exc).__name__}: {exc}). "
            "Start the container or fix MONGODB_URI.",
            pytrace=False,
        )
    yield client, LiveMongo(uri, database)
    client.drop_database(database)
    client.close()


@pytest.fixture(autouse=True)
def live_db(_live_mongo_client) -> LiveMongo:
    """A fresh, empty ``*-tests-live`` database for every test."""
    client, live = _live_mongo_client
    assert live.database.endswith(LIVE_DB_SUFFIX)
    client.drop_database(live.database)
    return live


@pytest.fixture
def live_raw_db(_live_mongo_client, live_db):
    """Synchronous pymongo handle on the live database, for direct assertions."""
    client, _ = _live_mongo_client
    return client[live_db.database]


async def _close_client(store) -> None:
    # The storages' disconnect() does not await AsyncMongoClient.close()
    # (BUG pinned in tests/test_storage.py), so close the client here.
    client = store._client
    store._client = store._db = None
    await client.close()


@pytest.fixture
async def jobs(live_db):
    store = JobStorage(live_db.uri, live_db.database)
    assert await store.connect()
    yield store
    await _close_client(store)


@pytest.fixture
async def configs(live_db):
    store = ConfigurationStorage(live_db.uri, live_db.database)
    assert await store.connect()
    yield store
    await _close_client(store)


@pytest.fixture
async def ingestions(live_db):
    store = IngestionJobStorage(live_db.uri, live_db.database)
    assert await store.connect()
    yield store
    await _close_client(store)


@pytest.fixture
async def history(live_db):
    store = ExecutionHistoryStorage()
    await store.connect(live_db.uri, live_db.database)
    yield store
    await store._db.client.close()


@pytest.fixture
def live_settings(live_db, main_state, monkeypatch):
    """Point the app at the live database (restored by ``main_state``)."""
    monkeypatch.setattr(main.settings, "mongodb_uri", live_db.uri)
    monkeypatch.setattr(main.settings, "mongodb_database", live_db.database)
    return main.settings


@pytest.fixture
def start_app(live_settings):
    """``with start_app() as client:`` runs the real lifespan (storages + scheduler)."""
    return lambda: TestClient(main.app)
