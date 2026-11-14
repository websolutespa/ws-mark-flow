"""
Tests for ``src.integration.sftp`` (SFTP source + destination).

``paramiko.SSHClient`` is replaced by a spec'd MagicMock whose ``open_sftp()``
returns a spec'd ``SFTPClient`` mock. The SFTP calls are wired to a small
in-memory server that behaves like OpenSSH's sftp-server (POSIX paths,
duplicate slashes collapsed, relative paths resolved against the login
directory, ``IOError`` for missing entries), so the integration's path
handling is exercised for real. Private keys are real paramiko/cryptography
keys, so key parsing and passphrase handling are not mocked.
"""
import errno
import io
import logging
import posixpath
import re
import stat
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import paramiko
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from pydantic import ValidationError

from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration
from src.integration.sftp import SFTPConfig, SFTPDestination, SFTPSource
from tests.helpers import make_file_info

LOGGER = "src.integration.sftp"
HOME = "/home/mark"
MTIME = 1_700_000_000
PASSPHRASE = "s3cret-passphrase"
NOT_CONNECTED = "Not connected"


# --- in-memory SFTP server ---------------------------------------------------

class FakeSFTPServer:
    """Just enough of an sftp-server to back the SFTPClient mock."""

    def __init__(self):
        self.dirs: set[str] = {"/", "/home", HOME}
        self.files: dict[str, tuple[bytes, object]] = {}
        self.special: dict[str, int] = {}
        self.unreadable: set[str] = set()
        self.readonly: set[str] = set()
        self.racy_mkdir = False
        self.mkdir_calls: list[str] = []

    # helpers used by the tests
    @staticmethod
    def norm(path) -> str:
        path = str(path)
        if not path.startswith("/"):
            path = f"{HOME}/{path}"
        return posixpath.normpath(re.sub("/+", "/", path))

    def add_dir(self, path: str) -> None:
        path = self.norm(path)
        while path not in self.dirs:
            self.dirs.add(path)
            path = posixpath.dirname(path)

    def add_file(self, path: str, content: bytes = b"data", mtime=MTIME) -> None:
        path = self.norm(path)
        self.add_dir(posixpath.dirname(path))
        self.files[path] = (content, mtime)

    def add_special(self, path: str, mode: int) -> None:
        path = self.norm(path)
        self.add_dir(posixpath.dirname(path))
        self.special[path] = mode

    def _attrs(self, path: str) -> paramiko.SFTPAttributes:
        attrs = paramiko.SFTPAttributes()
        attrs.filename = posixpath.basename(path)
        if path in self.dirs:
            attrs.st_mode, attrs.st_size, attrs.st_mtime = stat.S_IFDIR | 0o755, 4096, MTIME
        elif path in self.files:
            content, mtime = self.files[path]
            attrs.st_mode, attrs.st_size, attrs.st_mtime = stat.S_IFREG | 0o644, len(content), mtime
        else:
            attrs.st_mode, attrs.st_size, attrs.st_mtime = self.special[path], 0, MTIME
        return attrs

    @staticmethod
    def _missing(path: str) -> FileNotFoundError:
        return FileNotFoundError(errno.ENOENT, "No such file", path)

    # SFTPClient API
    def listdir_attr(self, path="."):
        path = self.norm(path)
        if path not in self.dirs:
            raise self._missing(path)
        if path in self.unreadable:
            raise PermissionError(errno.EACCES, "Permission denied", path)
        children = [
            p for p in (*self.dirs, *self.files, *self.special)
            if p != "/" and posixpath.dirname(p) == path
        ]
        return [self._attrs(p) for p in sorted(children)]

    def stat(self, path):
        path = self.norm(path)
        if path not in self.dirs and path not in self.files and path not in self.special:
            raise self._missing(path)
        return self._attrs(path)

    def mkdir(self, path, mode=0o777):
        path = self.norm(path)
        self.mkdir_calls.append(path)
        if self.racy_mkdir:
            # another client created it between our stat() and mkdir()
            self.dirs.add(path)
            raise OSError("Failure")
        parent = posixpath.dirname(path)
        if parent in self.readonly:
            raise PermissionError(errno.EACCES, "Permission denied", path)
        if parent not in self.dirs:
            raise self._missing(path)
        if path in self.dirs or path in self.files:
            raise OSError("Failure")
        self.dirs.add(path)

    def get(self, remotepath, localpath, callback=None, prefetch=True, max_concurrent_prefetch_requests=None):
        path = self.norm(remotepath)
        if path not in self.files:
            raise self._missing(path)
        Path(localpath).write_bytes(self.files[path][0])

    def put(self, localpath, remotepath, callback=None, confirm=True):
        path = self.norm(remotepath)
        if posixpath.dirname(path) not in self.dirs:
            raise self._missing(path)
        self.files[path] = (Path(localpath).read_bytes(), MTIME)
        return self._attrs(path)


# --- fixtures ----------------------------------------------------------------

@pytest.fixture
def server() -> FakeSFTPServer:
    return FakeSFTPServer()


@pytest.fixture
def sftp_client(server) -> MagicMock:
    client = MagicMock(spec=paramiko.SFTPClient, name="SFTPClient")
    for name in ("listdir_attr", "stat", "mkdir", "get", "put"):
        getattr(client, name).side_effect = getattr(server, name)
    return client


@pytest.fixture
def ssh(monkeypatch, sftp_client) -> MagicMock:
    client = MagicMock(spec=paramiko.SSHClient, name="SSHClient")
    client.open_sftp.return_value = sftp_client
    monkeypatch.setattr(paramiko, "SSHClient", MagicMock(return_value=client))
    return client


@pytest.fixture
async def make_connected(ssh):
    created = []

    async def _make(cls=SFTPSource, **config):
        config.setdefault("host", "sftp.example.com")
        config.setdefault("username", "mark")
        config.setdefault("password", "pw")
        config.setdefault("basePath", "/data")
        integration = cls(config)
        assert await integration.connect() is True
        created.append(integration)
        return integration

    yield _make
    for integration in created:
        await integration.disconnect()


@pytest.fixture
def tree(server) -> FakeSFTPServer:
    """
    /data/
      a.pdf  B.PDF  notes.txt  README  link.pdf (symlink)
      docs/c.docx
      docs/deep/d.pdf
      empty/
    """
    server.add_file("/data/a.pdf", b"pdf")
    server.add_file("/data/B.PDF", b"PDF!")
    server.add_file("/data/notes.txt", b"notes")
    server.add_file("/data/README", b"readme")
    server.add_special("/data/link.pdf", stat.S_IFLNK | 0o777)
    server.add_file("/data/docs/c.docx", b"docx")
    server.add_file("/data/docs/deep/d.pdf", b"deep pdf")
    server.add_dir("/data/empty")
    return server


@pytest.fixture(scope="module")
def rsa_key() -> paramiko.RSAKey:
    return paramiko.RSAKey.generate(1024)


@pytest.fixture(scope="module")
def rsa_pem(rsa_key) -> str:
    buffer = io.StringIO()
    rsa_key.write_private_key(buffer, password=PASSPHRASE)
    return buffer.getvalue()


@pytest.fixture
def local_md(tmp_path) -> Path:
    path = tmp_path / "local" / "out.md"
    path.parent.mkdir()
    path.write_bytes("# Résumé ✓\n".encode("utf-8"))
    return path


def by_path(files) -> dict:
    return {f.path: f for f in files}


# --- config ------------------------------------------------------------------

class TestConfig:
    def test_camel_case_aliases_and_defaults(self):
        config = SFTPConfig.model_validate({
            "host": "h", "username": "u",
            "privateKey": "/keys/id_rsa", "privateKeyPassphrase": "pp", "basePath": "/srv",
            "unknown": "ignored",
        })

        assert (config.private_key, config.private_key_passphrase, config.base_path) == ("/keys/id_rsa", "pp", "/srv")
        assert config.port == 22
        assert config.password is None
        assert not hasattr(config, "unknown")

    def test_snake_case_names_and_defaults(self):
        config = SFTPConfig.model_validate({"host": "h", "username": "u", "port": "2222",
                                            "private_key": "k", "base_path": "/x"})

        assert (config.port, config.private_key, config.base_path) == (2222, "k", "/x")
        assert SFTPConfig(host="h", username="u").base_path == "/"

    @pytest.mark.parametrize("missing", ["host", "username"])
    def test_required_fields(self, missing):
        data = {"host": "h", "username": "u"}
        del data[missing]

        with pytest.raises(ValidationError, match=missing):
            SFTPConfig.model_validate(data)


@pytest.mark.parametrize("cls, base", [(SFTPSource, SourceIntegration), (SFTPDestination, DestinationIntegration)])
def test_integration_type_and_interfaces(cls, base):
    integration = cls({"host": "h", "username": "u"})

    assert integration.integration_type is IntegrationType.SFTP
    assert isinstance(integration, base)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    async def test_password_auth(self, ssh, sftp_client):
        source = SFTPSource({"host": "sftp.example.com", "port": 2222, "username": "mark", "password": "pw"})

        assert await source.connect() is True

        ssh.connect.assert_called_once_with(hostname="sftp.example.com", port=2222, username="mark", password="pw")
        policy = ssh.set_missing_host_key_policy.call_args.args[0]
        assert isinstance(policy, paramiko.AutoAddPolicy)
        ssh.open_sftp.assert_called_once_with()
        assert source._sftp is sftp_client

    async def test_inline_encrypted_private_key(self, ssh, rsa_key, rsa_pem):
        source = SFTPSource({"host": "h", "username": "u", "privateKey": rsa_pem,
                             "privateKeyPassphrase": PASSPHRASE})

        assert await source.connect() is True

        kwargs = ssh.connect.call_args.kwargs
        assert "password" not in kwargs
        assert isinstance(kwargs["pkey"], paramiko.RSAKey)
        assert kwargs["pkey"].get_base64() == rsa_key.get_base64()

    async def test_private_key_file_path_together_with_password(self, ssh, rsa_key, rsa_pem, tmp_path):
        key_file = tmp_path / "id_rsa"
        key_file.write_text(rsa_pem)
        source = SFTPSource({"host": "h", "username": "u", "password": "pw",
                             "private_key": str(key_file), "private_key_passphrase": PASSPHRASE})

        assert await source.connect() is True

        kwargs = ssh.connect.call_args.kwargs
        assert kwargs["password"] == "pw"
        assert kwargs["pkey"].get_base64() == rsa_key.get_base64()

    @pytest.mark.parametrize("passphrase", [None, "wrong"])
    async def test_bad_passphrase_fails_before_connecting(self, ssh, rsa_pem, passphrase, caplog):
        source = SFTPSource({"host": "h", "username": "u", "privateKey": rsa_pem,
                             "privateKeyPassphrase": passphrase})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        ssh.connect.assert_not_called()
        assert "Failed to connect to SFTP" in caplog.text

    async def test_missing_private_key_file(self, ssh, tmp_path, caplog):
        source = SFTPSource({"host": "h", "username": "u", "privateKey": str(tmp_path / "nope")})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        ssh.connect.assert_not_called()
        assert "Failed to connect to SFTP" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sftp.py:79-88 always parses private keys with paramiko.RSAKey, so "
        "Ed25519/ECDSA keys (the ssh-keygen default) can never authenticate",
    )
    async def test_ed25519_private_key(self, ssh):
        pem = ed25519.Ed25519PrivateKey.generate().private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, serialization.NoEncryption()
        ).decode("ascii")
        source = SFTPSource({"host": "h", "username": "u", "privateKey": pem})

        assert await source.connect() is True
        assert ssh.connect.call_args.kwargs["pkey"].get_name() == "ssh-ed25519"

    @pytest.mark.parametrize(
        "error",
        [
            paramiko.AuthenticationException("Authentication failed."),
            paramiko.SSHException("Error reading SSH protocol banner"),
            TimeoutError("timed out"),
            OSError(errno.ECONNREFUSED, "Connection refused"),
        ],
    )
    async def test_connection_errors_return_false(self, ssh, error, caplog):
        ssh.connect.side_effect = error
        source = SFTPSource({"host": "h", "username": "u", "password": "pw"})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        ssh.open_sftp.assert_not_called()
        assert "Failed to connect to SFTP" in caplog.text
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_sftp_subsystem_failure_returns_false(self, ssh):
        ssh.open_sftp.side_effect = paramiko.SSHException("subsystem request failed")
        source = SFTPSource({"host": "h", "username": "u", "password": "pw"})

        assert await source.connect() is False


class TestDisconnect:
    async def test_closes_sftp_then_ssh_and_is_idempotent(self, make_connected, ssh, sftp_client):
        source = await make_connected()

        await source.disconnect()
        await source.disconnect()

        sftp_client.close.assert_called_once_with()
        ssh.close.assert_called_once_with()
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_disconnect_without_connect_is_a_noop(self):
        await SFTPDestination({"host": "h", "username": "u"}).disconnect()

    async def test_works_as_async_context_manager(self, ssh, sftp_client, tree):
        async with SFTPSource({"host": "h", "username": "u", "password": "pw", "basePath": "/data"}) as source:
            assert "/a.pdf" in by_path(await source.list_files())

        sftp_client.close.assert_called_once_with()
        ssh.close.assert_called_once_with()


@pytest.mark.parametrize(
    "call",
    [
        lambda i, p: i.list_files(),
        lambda i, p: i.download_file(make_file_info("/a.pdf"), p / "a.pdf"),
        lambda i, p: i.upload_file(p / "a.md", "/a.md"),
    ],
    ids=["list_files", "download_file", "upload_file"],
)
async def test_operations_require_connect(call, tmp_path):
    integration = SFTPDestination({"host": "h", "username": "u"})

    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await call(integration, tmp_path)


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_walks_recursively_with_paths_relative_to_base(self, make_connected, tree):
        source = await make_connected()

        files = by_path(await source.list_files())

        assert set(files) == {"/a.pdf", "/B.PDF", "/notes.txt", "/README", "/docs/c.docx", "/docs/deep/d.pdf"}
        deep = files["/docs/deep/d.pdf"]
        assert deep.name == "d.pdf"
        assert deep.size == len(b"deep pdf")
        assert deep.modified_at == datetime.fromtimestamp(MTIME)
        assert deep.content_type is None

    async def test_symlinks_and_special_entries_are_skipped(self, make_connected, tree):
        tree.add_special("/data/docs/socket", stat.S_IFSOCK | 0o600)
        source = await make_connected()

        paths = set(by_path(await source.list_files()))

        assert "/link.pdf" not in paths
        assert "/docs/socket" not in paths

    @pytest.mark.parametrize(
        "extensions, expected",
        [
            ([".pdf"], {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}),
            ([".PDF"], {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}),
            ([".docx", ".TXT"], {"/notes.txt", "/docs/c.docx"}),
            ([".xlsx"], set()),
            ([], set()),
        ],
    )
    async def test_extension_filter_is_case_insensitive(self, make_connected, tree, extensions, expected):
        source = await make_connected()

        assert set(by_path(await source.list_files(extensions=extensions))) == expected

    @pytest.mark.parametrize("base_path", ["/", None])
    async def test_root_base_path(self, make_connected, tree, base_path):
        source = await make_connected(basePath=base_path)

        paths = set(by_path(await source.list_files(extensions=[".docx"])))

        assert paths == {"/data/docs/c.docx"}

    async def test_absolute_folder_path_overrides_the_start_directory(self, make_connected, tree):
        source = await make_connected(basePath="/")

        paths = set(by_path(await source.list_files(folder_path="/data/docs")))

        assert paths == {"/c.docx", "/deep/d.pdf"}

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sftp.py:118 uses folder_path as the walk root instead of resolving it "
        "under base_path, and paths are made relative to it, so download_file (which joins "
        "base_path) cannot fetch the listed files",
    )
    async def test_folder_path_is_resolved_under_base_path(self, make_connected, tree, tmp_path):
        source = await make_connected()

        files = await source.list_files(folder_path="docs")

        assert set(by_path(files)) == {"/docs/c.docx", "/docs/deep/d.pdf"}
        assert set(await source.download_files(files, tmp_path)) == {"/docs/c.docx", "/docs/deep/d.pdf"}

    async def test_unreadable_subdirectory_is_skipped_with_warning(self, make_connected, tree, caplog):
        tree.unreadable.add("/data/docs")
        source = await make_connected()

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            paths = set(by_path(await source.list_files()))

        assert paths == {"/a.pdf", "/B.PDF", "/notes.txt", "/README"}
        assert "Cannot list directory /data/docs" in caplog.text

    async def test_missing_base_path_yields_nothing(self, make_connected, server, caplog):
        source = await make_connected(basePath="/nowhere")

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            assert await source.list_files() == []

        assert "Cannot list directory /nowhere" in caplog.text

    @pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow:DeprecationWarning")
    async def test_unusable_mtime_falls_back_to_now(self, make_connected, server):
        server.add_file("/data/odd.pdf", b"x", mtime=None)
        source = await make_connected()

        before = datetime.utcnow()
        (info,) = await source.list_files()

        assert info.path == "/odd.pdf"
        assert before - timedelta(seconds=1) <= info.modified_at <= datetime.utcnow() + timedelta(seconds=1)


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    async def test_downloads_into_nested_local_path(self, make_connected, tree, sftp_client, file_info, tmp_path):
        source = await make_connected()
        target = tmp_path / "out" / "docs" / "c.docx"

        assert await source.download_file(file_info("/docs/c.docx"), target) is True

        assert target.read_bytes() == b"docx"
        assert sftp_client.get.call_args.args == ("/data/docs/c.docx", str(target))

    async def test_round_trip_with_download_files(self, make_connected, tree, tmp_path):
        source = await make_connected()

        files = await source.list_files(extensions=[".pdf"])
        result = await source.download_files(files, tmp_path)

        assert set(result) == {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}
        assert (tmp_path / "docs" / "deep" / "d.pdf").read_bytes() == b"deep pdf"

    async def test_missing_remote_file(self, make_connected, server, file_info, tmp_path, caplog):
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(file_info("/gone.pdf"), tmp_path / "gone.pdf") is False

        assert "Download error for /gone.pdf" in caplog.text
        assert not (tmp_path / "gone.pdf").exists()

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: with base_path=None list_files walks '/' (sftp.py:118) but "
        "download_file joins '' (sftp.py:191), fetching paths relative to the login directory",
    )
    async def test_null_base_path_round_trip(self, make_connected, tree, tmp_path):
        source = await make_connected(basePath=None)

        files = await source.list_files(extensions=[".docx"])

        assert await source.download_files(files, tmp_path) == {
            "/data/docs/c.docx": tmp_path / "data" / "docs" / "c.docx"
        }


# --- upload_file / create_folder ---------------------------------------------

class TestUploadFile:
    async def test_creates_missing_parent_directories(self, make_connected, server, local_md):
        server.add_dir("/data")
        dest = await make_connected(SFTPDestination)

        assert await dest.upload_file(local_md, "/out/docs/a.md") is True

        assert server.files["/data/out/docs/a.md"][0] == local_md.read_bytes()
        assert server.mkdir_calls == ["/data/out", "/data/out/docs"]

    async def test_existing_directories_are_not_recreated(self, make_connected, tree, local_md):
        dest = await make_connected(SFTPDestination)

        assert await dest.upload_file(local_md, "docs/deep/c.md") is True

        assert tree.mkdir_calls == []
        assert "/data/docs/deep/c.md" in tree.files

    async def test_upload_to_root_needs_no_directories(self, make_connected, server, sftp_client, local_md):
        dest = await make_connected(SFTPDestination, basePath="/")

        assert await dest.upload_file(local_md, "/a.md") is True

        assert "/a.md" in server.files
        sftp_client.stat.assert_not_called()
        sftp_client.mkdir.assert_not_called()

    async def test_directory_created_concurrently_is_tolerated(self, make_connected, server, local_md):
        server.racy_mkdir = True
        dest = await make_connected(SFTPDestination)

        assert await dest.upload_file(local_md, "/x/a.md") is True

        assert "/data/x/a.md" in server.files

    async def test_permission_denied_on_mkdir_fails_the_upload(self, make_connected, server, local_md, caplog):
        server.add_dir("/data")
        server.readonly.add("/data")
        dest = await make_connected(SFTPDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(local_md, "/x/a.md") is False

        assert server.mkdir_calls == ["/data/x"]
        assert "Upload error for /x/a.md" in caplog.text

    async def test_missing_local_file(self, make_connected, server, tmp_path, caplog):
        dest = await make_connected(SFTPDestination, basePath="/")

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(tmp_path / "missing.md", "/a.md") is False

        assert "Upload error for /a.md" in caplog.text

    async def test_upload_files_creates_folders_then_uploads(self, make_connected, server, local_md):
        server.add_dir("/data")
        dest = await make_connected(SFTPDestination)

        result = await dest.upload_files({local_md: "/reports/2026/out.md"})

        assert result == {"/reports/2026/out.md": True}
        assert "/data/reports/2026/out.md" in server.files

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sftp.py:_mkdir_p prefixes every path with '/', so a relative base_path "
        "creates directories at the filesystem root instead of under the login directory",
    )
    async def test_relative_base_path_is_resolved_against_login_directory(self, make_connected, server, local_md):
        dest = await make_connected(SFTPDestination, basePath="uploads")

        assert await dest.upload_file(local_md, "/docs/a.md") is True
        assert f"{HOME}/uploads/docs/a.md" in server.files


class TestCreateFolder:
    async def test_creates_nested_folders_and_is_idempotent(self, make_connected, server):
        server.add_dir("/data")
        dest = await make_connected(SFTPDestination)

        assert await dest.create_folder("/a/b/c") is True
        assert await dest.create_folder("a/b/c/") is True

        assert {"/data/a", "/data/a/b", "/data/a/b/c"} <= server.dirs
        assert server.mkdir_calls == ["/data/a", "/data/a/b", "/data/a/b/c"]

    async def test_root_is_a_noop(self, make_connected, sftp_client):
        dest = await make_connected(SFTPDestination, basePath="/")

        assert await dest.create_folder("/") is True

        sftp_client.stat.assert_not_called()

    async def test_not_connected_returns_false(self, caplog):
        dest = SFTPDestination({"host": "h", "username": "u"})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.create_folder("/a") is False

        assert "Failed to create folder /a" in caplog.text
