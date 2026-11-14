"""Guards that keep the suite hermetic. If these fail, other results are suspect."""
import socket
from unittest.mock import MagicMock

import pytest

from src import main
from src.converter import Converter
from tests.conftest import NetworkBlockedError, _is_loopback


def test_settings_ignore_developer_env_file():
    # Defaults, not whatever the developer has in .env / the shell.
    assert main.settings.auth_password == ""
    assert main.settings.auth_username == "admin"
    assert main.settings.llm_api_key == ""
    assert main.settings.mongodb_uri == "mongodb://localhost:27017"
    assert main.settings.model_config["env_file"] is None


def test_temp_dir_is_outside_the_repository(tmp_path_factory):
    assert "ws_mark_flow_tests_" in main.settings.temp_dir


def test_docling_models_are_never_loaded_by_default():
    assert isinstance(Converter._doc_converter, MagicMock)


def test_dns_lookup_of_external_host_is_blocked():
    with pytest.raises(NetworkBlockedError, match="example.com"):
        socket.getaddrinfo("example.com", 443)


def test_connect_to_external_address_is_blocked():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(NetworkBlockedError):
            sock.connect(("93.184.216.34", 443))
        with pytest.raises(NetworkBlockedError):
            sock.connect_ex(("93.184.216.34", 443))


def test_loopback_is_still_allowed():
    assert socket.getaddrinfo("localhost", 80)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(5)
        port = listener.getsockname()[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("127.0.0.1", port))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            assert sock.connect_ex(("127.0.0.1", port)) == 0


@pytest.mark.parametrize(
    "host, expected",
    [
        (None, True),
        ("", True),
        ("localhost", True),
        ("127.0.0.1", True),
        (b"127.0.0.1", True),
        ("::1", True),
        ("[::1]", True),
        ("fe80::1%eth0", False),
        ("10.0.0.5", False),
        ("example.com", False),
    ],
)
def test_is_loopback(host, expected):
    assert _is_loopback(host) is expected


@pytest.mark.allow_network
def test_allow_network_marker_disables_the_guard():
    # Only checks the guard is not installed; no connection is attempted.
    assert socket.getaddrinfo.__name__ == "getaddrinfo"
