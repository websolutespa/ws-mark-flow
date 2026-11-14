"""Builders and fixtures shared by the ``test_main_*`` modules."""
from typing import Any
from unittest.mock import AsyncMock

import pytest
from apscheduler import AsyncScheduler
from fastapi.testclient import TestClient

from src import main
from src.models import (
    ConversionJob,
    IngestionJob,
    IntegrationConfig,
    SavedConfiguration,
    VectorStoreConfig,
)
from src.storage import (
    ConfigurationStorage,
    ExecutionHistoryStorage,
    IngestionJobStorage,
    JobStorage,
)

JOB_ID = "64b7f0c2a1b2c3d4e5f60718"
CONFIG_ID = "64b7f0c2a1b2c3d4e5f60719"
DEST_CONFIG_ID = "64b7f0c2a1b2c3d4e5f6071a"
INGESTION_ID = "64b7f0c2a1b2c3d4e5f6071b"

LOCAL_SOURCE = {"type": "local", "config": {"path": "/data/in"}}
LOCAL_DEST = {"type": "local", "config": {"path": "/data/out"}}


def make_job(**overrides: Any) -> ConversionJob:
    data: dict[str, Any] = {
        "_id": JOB_ID,
        "name": "nightly",
        "source": IntegrationConfig(**LOCAL_SOURCE),
        "destination": IntegrationConfig(**LOCAL_DEST),
    }
    data.update(overrides)
    return ConversionJob(**data)


def make_config(**overrides: Any) -> SavedConfiguration:
    data: dict[str, Any] = {
        "_id": CONFIG_ID,
        "name": "My Source",
        "type": "local",
        "config": {"path": "/data/in"},
        "is_source": True,
    }
    data.update(overrides)
    return SavedConfiguration(**data)


def make_ingestion_job(**overrides: Any) -> IngestionJob:
    data: dict[str, Any] = {
        "_id": INGESTION_ID,
        "name": "index-docs",
        "source": IntegrationConfig(**LOCAL_SOURCE),
        "vector_store": VectorStoreConfig(type="chroma", config={"path": "/data/chroma"}),
    }
    data.update(overrides)
    return IngestionJob(**data)


@pytest.fixture
def main_state(monkeypatch):
    """Reset ``src.main`` globals and restore runtime-mutable settings afterwards."""
    for name in ("storage", "config_storage", "history_storage", "ingestion_storage", "scheduler"):
        monkeypatch.setattr(main, name, None)
    snapshot = main.settings.model_dump()
    yield main
    for key, value in snapshot.items():
        setattr(main.settings, key, value)


@pytest.fixture
def client(main_state) -> TestClient:
    """HTTP client that does NOT run the lifespan (no MongoDB, no scheduler)."""
    return TestClient(main.app)


@pytest.fixture
def storage(main_state, monkeypatch) -> AsyncMock:
    mock = AsyncMock(spec=JobStorage)
    monkeypatch.setattr(main, "storage", mock)
    return mock


@pytest.fixture
def config_storage(main_state, monkeypatch) -> AsyncMock:
    mock = AsyncMock(spec=ConfigurationStorage)
    monkeypatch.setattr(main, "config_storage", mock)
    return mock


@pytest.fixture
def history_storage(main_state, monkeypatch) -> AsyncMock:
    mock = AsyncMock(spec=ExecutionHistoryStorage)
    monkeypatch.setattr(main, "history_storage", mock)
    return mock


@pytest.fixture
def ingestion_storage(main_state, monkeypatch) -> AsyncMock:
    mock = AsyncMock(spec=IngestionJobStorage)
    monkeypatch.setattr(main, "ingestion_storage", mock)
    return mock


@pytest.fixture
def scheduler(main_state, monkeypatch) -> AsyncMock:
    """A stand-in for APScheduler's AsyncScheduler (spec'd against the real class)."""
    mock = AsyncMock(spec=AsyncScheduler)
    monkeypatch.setattr(main, "scheduler", mock)
    return mock


@pytest.fixture
def integrations(main_state, monkeypatch):
    """Route the factory functions used by ``main`` to caller-supplied fakes."""

    class _Registry:
        source: Any = None
        destination: Any = None
        vector_store: Any = None
        calls: list[tuple[str, Any, dict]] = []

    registry = _Registry()
    registry.calls = []

    def _make(kind: str):
        def factory(integration_type, config):
            registry.calls.append((kind, integration_type, config))
            target = getattr(registry, kind)
            if isinstance(target, Exception):
                raise target
            return target
        return factory

    monkeypatch.setattr(main, "create_source", _make("source"))
    monkeypatch.setattr(main, "create_destination", _make("destination"))
    monkeypatch.setattr(main, "create_vector_store", _make("vector_store"))
    return registry
