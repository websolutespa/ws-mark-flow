"""
Shared pytest configuration.

The suite is hermetic: no network, no MongoDB, no LLM calls, no ML model
loading and no dependency on the developer's ``.env`` file. Everything
external is faked or mocked.

Opt-in exceptions are marked explicitly:
  * ``@pytest.mark.allow_network``  - test may open real (non-loopback) sockets
  * ``@pytest.mark.live``           - test talks to a real backing service; skipped
                                      unless ``pytest --live`` (see tests/live/)
"""
import ipaddress
import os
import shutil
import socket
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Never let HuggingFace-backed libraries reach the network during tests.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
os.environ.setdefault("ANONYMIZED_TELEMETRY", "False")  # chromadb

# --- Hermetic settings -------------------------------------------------------
# This block must run before ``src.main`` is imported, because that module
# builds its global ``settings`` (and the services that depend on it) at import
# time. Importing ``src.config`` is safe: it does not instantiate Settings.
from src.config import Settings, get_settings  # noqa: E402

_SESSION_TMP = Path(tempfile.mkdtemp(prefix="ws_mark_flow_tests_"))

# Live tests (``--live``) may target the MongoDB set in the shell: keep it
# before the scrub below. tests/live/conftest.py falls back to the .env file.
LIVE_ENV = {key: os.environ.get(key) for key in ("MONGODB_URI", "MONGODB_DATABASE")}

# Never read the developer's .env (it may hold real credentials) ...
Settings.model_config["env_file"] = None
# ... nor settings leaking in from the shell environment.
for _field in Settings.model_fields:
    os.environ.pop(_field.upper(), None)
    os.environ.pop(_field, None)
# Keep temp files created by module-level services out of the repository.
os.environ["TEMP_DIR"] = str(_SESSION_TMP / "tmp")
get_settings.cache_clear()

# --- No ML model loading -----------------------------------------------------
# ``Converter.__init__`` eagerly builds the Docling pipeline (loads model
# weights, contacts HuggingFace) and ``src.main`` creates a Converter at import
# time. Swap the factory for a stub before anything imports ``src.main``.
# The genuine factory stays reachable through the ``real_docling_factory``
# fixture so it can still be unit-tested with Docling's classes mocked out.
import src.converter as _converter_module  # noqa: E402

_REAL_DOCLING_FACTORY = _converter_module._docling_converter


def _stub_docling_factory() -> MagicMock:
    return MagicMock(name="docling_converter_stub")


_converter_module._docling_converter = _stub_docling_factory

from tests.helpers import FakeDestination, FakeSource, FakeVectorStore, make_file_info  # noqa: E402

# Fixtures for the FastAPI layer (client, mocked storages, scheduler, ...).
pytest_plugins = ["tests.main_support"]


def pytest_addoption(parser):
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="also run @pytest.mark.live tests against real services (e.g. the MongoDB in .env)",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "allow_network: test may open real network connections")
    config.addinivalue_line("markers", "live: test needs a real backing service (opt-in via --live)")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--live"):
        return
    skip_live = pytest.mark.skip(reason="live test: run with --live")
    for item in items:
        if item.get_closest_marker("live"):
            item.add_marker(skip_live)


# --- Network guard -----------------------------------------------------------

def _is_loopback(host) -> bool:
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode("ascii", "ignore")
    host = str(host).strip("[]").split("%", 1)[0]
    if host in ("", "localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class NetworkBlockedError(RuntimeError):
    """Raised when a test tries to reach a non-loopback host."""


@pytest.fixture(autouse=True)
def _block_network(request, monkeypatch):
    """Fail fast if a test attempts real network access (loopback is allowed)."""
    if request.node.get_closest_marker("allow_network") or request.node.get_closest_marker("live"):
        yield
        return

    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def _check(host):
        if not _is_loopback(host):
            raise NetworkBlockedError(
                f"Network access to {host!r} blocked: tests must be hermetic. "
                "Mock the client, or mark the test with @pytest.mark.allow_network."
            )

    def guarded_getaddrinfo(host, *args, **kwargs):
        _check(host)
        return real_getaddrinfo(host, *args, **kwargs)

    def guarded_connect(self, address):
        if isinstance(address, tuple):
            _check(address[0])
        return real_connect(self, address)

    def guarded_connect_ex(self, address):
        if isinstance(address, tuple):
            _check(address[0])
        return real_connect_ex(self, address)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    yield


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def _session_tmp_cleanup():
    """Remove the session-wide temp directory once the run is over."""
    yield
    shutil.rmtree(_SESSION_TMP, ignore_errors=True)


@pytest.fixture
def real_docling_factory():
    """The genuine ``src.converter._docling_converter`` (stubbed out by default).

    Calling it for real loads ML models: patch ``src.converter.DocumentConverter``
    first unless that is exactly what the test intends.
    """
    return _REAL_DOCLING_FACTORY


@pytest.fixture
def settings(tmp_path) -> Settings:
    """A fresh Settings instance with defaults and an isolated temp dir."""
    return Settings(temp_dir=str(tmp_path / "tmp"))


@pytest.fixture
def file_info():
    """Factory for FileInfo objects: ``file_info("/docs/a.pdf", size=3)``."""
    return make_file_info


@pytest.fixture
def fixed_now() -> datetime:
    """A deterministic timestamp for tests that need one."""
    return datetime(2026, 1, 15, 12, 30, 0)


@pytest.fixture
def fake_source():
    """Factory for in-memory sources: ``fake_source({"/a.pdf": b"..."})``."""
    return FakeSource


@pytest.fixture
def fake_destination():
    """Factory for in-memory destinations."""
    return FakeDestination


@pytest.fixture
def fake_vector_store():
    """Factory for in-memory vector stores."""
    return FakeVectorStore
