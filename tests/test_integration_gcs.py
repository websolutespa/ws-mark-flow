"""
Tests for ``src.integration.gcs`` (Google Cloud Storage source + destination).

The integration builds REAL ``google.cloud.storage`` objects: the ``Client``
(with real service-account credentials signed by a throwaway RSA key), its
``Bucket`` and ``Blob`` handles, and blob metadata parsed from JSON API
resources. Only the methods that would hit the wire (``Bucket.reload``,
``Client.list_blobs``, ``Blob.download_to_filename``,
``Blob.upload_from_filename``) are replaced, with autospecced fakes, so a
changed SDK signature fails a test instead of failing in production.
Application Default Credentials are faked, so nothing is read from the
developer's gcloud configuration and the metadata server is never contacted.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import ANY, create_autospec

import google.auth
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.api_core.exceptions import Forbidden, NotFound, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from google.cloud.storage import Blob, Bucket, Client
from google.oauth2 import service_account
from pydantic import ValidationError

from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration
from src.integration.gcs import GCSConfig, GCSDestination, GCSSource

LOGGER = "src.integration.gcs"
BUCKET = "mark-flow-docs"
PROJECT = "mark-flow-tests"
ADC_PROJECT = "adc-project"
SA_EMAIL = "mark-flow@mark-flow-tests.iam.gserviceaccount.com"
NOT_CONNECTED = "Not connected"
MTIME = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)


def utcnow_naive() -> datetime:
    """What ``datetime.utcnow()`` returns, without its deprecation warning."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture(scope="module")
def sa_info() -> dict:
    """A service-account key file as downloaded from the Google Cloud console."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")
    return {
        "type": "service_account",
        "project_id": PROJECT,
        "private_key_id": "0123456789abcdef0123456789abcdef01234567",
        "private_key": private_pem,
        "client_email": SA_EMAIL,
        "client_id": "123456789012345678901",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }


def resource(name, size=10, updated="2026-01-15T12:30:00.000Z", content_type="application/octet-stream"):
    """A JSON API object resource, as returned by ``objects.list``."""
    item = {"kind": "storage#object", "bucket": BUCKET, "name": name}
    if size is not None:
        item["size"] = str(size)
    if updated is not None:
        item["updated"] = updated
    if content_type is not None:
        item["contentType"] = content_type
    return item


class FakeGCS:
    """Autospecced stand-ins for every network-bound SDK method used by the integration."""

    def __init__(self, monkeypatch):
        self.items: list[dict] = []
        self.data = b""
        self.uploads: dict[str, bytes] = {}
        real_client = Client
        real_close = Client.close

        def make_client(*args, **kwargs):
            # Never resolve ADC for real (gcloud config, metadata server).
            kwargs.setdefault("credentials", AnonymousCredentials())
            return real_client(*args, **kwargs)

        self.adc = create_autospec(google.auth.default, return_value=(AnonymousCredentials(), ADC_PROJECT))
        monkeypatch.setattr(google.auth, "default", self.adc)
        self.client_cls = create_autospec(Client, side_effect=make_client)
        monkeypatch.setattr(storage, "Client", self.client_cls)

        self.reload = self._patch(monkeypatch, Bucket, "reload")
        self.list_blobs = self._patch(monkeypatch, Client, "list_blobs", side_effect=self._list_blobs)
        self.close = self._patch(monkeypatch, Client, "close", side_effect=real_close)
        self.download = self._patch(monkeypatch, Blob, "download_to_filename", side_effect=self._download)
        self.upload = self._patch(monkeypatch, Blob, "upload_from_filename", side_effect=self._upload)

    @staticmethod
    def _patch(monkeypatch, cls, name, **kwargs):
        fake = create_autospec(getattr(cls, name), **kwargs)
        monkeypatch.setattr(cls, name, fake)
        return fake

    def _list_blobs(self, client, bucket_or_name, **kwargs):
        bucket = client.bucket(bucket_or_name)
        for item in self.items:
            # Same construction as the SDK's own HTTPIterator item mapper.
            blob = Blob(item["name"], bucket=bucket)
            blob._set_properties(item)
            yield blob

    def _download(self, blob, filename, **kwargs):
        Path(filename).write_bytes(self.data)

    def _upload(self, blob, filename, **kwargs):
        self.uploads[blob.name] = Path(filename).read_bytes()


@pytest.fixture(autouse=True)
def _isolated_gcp_env(monkeypatch):
    for name in ("STORAGE_EMULATOR_HOST", "GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def gcs(monkeypatch) -> FakeGCS:
    return FakeGCS(monkeypatch)


@pytest.fixture
async def make_connected(gcs):
    created = []

    async def _make(cls=GCSSource, **config):
        config.setdefault("bucket", BUCKET)
        config.setdefault("project", PROJECT)
        integration = cls(config)
        assert await integration.connect() is True
        created.append(integration)
        return integration

    yield _make
    for integration in created:
        await integration.disconnect()


# --- Config ------------------------------------------------------------------

class TestConfig:
    def test_bucket_is_required(self):
        with pytest.raises(ValidationError) as exc:
            GCSConfig.model_validate({"project": PROJECT})
        assert exc.value.errors()[0]["loc"] == ("bucket",)

    def test_defaults(self):
        assert GCSConfig.model_validate({"bucket": BUCKET}).model_dump() == {
            "bucket": BUCKET,
            "prefix": None,
            "project": None,
            "service_account_json": None,
        }

    @pytest.mark.parametrize("key", ["serviceAccountJson", "service_account_json"])
    def test_service_account_aliases(self, key):
        assert GCSConfig.model_validate({"bucket": BUCKET, key: "/keys/sa.json"}).service_account_json == "/keys/sa.json"

    def test_unknown_keys_are_ignored(self):
        config = GCSConfig.model_validate({"bucket": BUCKET, "type": "gcs", "container": "x"})
        assert not hasattr(config, "container")

    @pytest.mark.parametrize("cls", [GCSSource, GCSDestination])
    def test_invalid_config_is_rejected_by_the_integration(self, cls):
        with pytest.raises(ValidationError):
            cls({"prefix": "docs"})


class TestIdentity:
    def test_source(self):
        source = GCSSource({"bucket": BUCKET})
        assert source.integration_type is IntegrationType.GCS
        assert isinstance(source, SourceIntegration)
        assert not isinstance(source, DestinationIntegration)

    def test_destination(self):
        dest = GCSDestination({"bucket": BUCKET})
        assert dest.integration_type is IntegrationType.GCS
        assert isinstance(dest, DestinationIntegration)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    @pytest.mark.parametrize("cls", [GCSSource, GCSDestination])
    async def test_application_default_credentials(self, gcs, cls, caplog):
        integration = cls({"bucket": BUCKET})

        with caplog.at_level(logging.INFO, logger=LOGGER):
            assert await integration.connect() is True

        gcs.client_cls.assert_called_once_with()
        client = integration._client
        assert isinstance(client, Client)
        assert client.project == ADC_PROJECT
        assert isinstance(integration._bucket, Bucket)
        assert (integration._bucket.name, integration._bucket.client) == (BUCKET, client)
        gcs.reload.assert_called_once_with(integration._bucket)
        assert f"Connected to GCS bucket: {BUCKET}" in caplog.text
        await integration.disconnect()

    async def test_explicit_project(self, gcs):
        source = GCSSource({"bucket": BUCKET, "project": PROJECT})

        assert await source.connect() is True

        gcs.client_cls.assert_called_once_with(project=PROJECT)
        assert source._client.project == PROJECT
        await source.disconnect()

    async def test_service_account_json_content(self, gcs, sa_info):
        source = GCSSource({"bucket": BUCKET, "project": PROJECT, "serviceAccountJson": json.dumps(sa_info)})

        assert await source.connect() is True

        gcs.client_cls.assert_called_once_with(project=PROJECT, credentials=ANY)
        credentials = gcs.client_cls.call_args.kwargs["credentials"]
        assert isinstance(credentials, service_account.Credentials)
        assert credentials.service_account_email == SA_EMAIL
        assert credentials.project_id == PROJECT
        await source.disconnect()

    async def test_service_account_json_file(self, gcs, sa_info, tmp_path):
        key_file = tmp_path / "keys" / "sa.json"
        key_file.parent.mkdir()
        key_file.write_text(json.dumps(sa_info, indent=2), encoding="utf-8")
        source = GCSSource({"bucket": BUCKET, "service_account_json": str(key_file)})

        assert await source.connect() is True

        gcs.client_cls.assert_called_once_with(credentials=ANY)
        assert gcs.client_cls.call_args.kwargs["credentials"].service_account_email == SA_EMAIL
        await source.disconnect()

    @pytest.mark.parametrize(
        "sa_json",
        [
            "{not json",
            '{"type": "service_account"}',
            "missing-key-file.json",
        ],
        ids=["malformed-json", "incomplete-key", "missing-file"],
    )
    async def test_bad_service_account_returns_false(self, gcs, caplog, sa_json, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        source = GCSSource({"bucket": BUCKET, "service_account_json": sa_json})

        assert await source.connect() is False

        gcs.client_cls.assert_not_called()
        assert (source._client, source._bucket) == (None, None)
        assert "Failed to connect to GCS" in caplog.text

    @pytest.mark.parametrize(
        "error",
        [NotFound("bucket not found"), Forbidden("caller lacks storage.buckets.get"), ServiceUnavailable("down")],
        ids=["missing-bucket", "forbidden", "unavailable"],
    )
    async def test_bucket_check_failure_returns_false(self, gcs, caplog, error):
        gcs.reload.side_effect = error
        source = GCSSource({"bucket": BUCKET, "project": PROJECT})

        assert await source.connect() is False

        assert (source._client, source._bucket) == (None, None)
        assert "Failed to connect to GCS" in caplog.text
        assert str(error) in caplog.text
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_client_creation_failure_returns_false(self, gcs, caplog):
        gcs.client_cls.side_effect = DefaultCredentialsError("no ADC")
        source = GCSSource({"bucket": BUCKET})

        assert await source.connect() is False

        assert (source._client, source._bucket) == (None, None)
        assert "no ADC" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: gcs.py:73-79 drops the storage Client without closing it when the bucket "
        "check fails, leaking its HTTP session; disconnect() can't help because _client is "
        "reset to None",
    )
    async def test_failed_connect_closes_the_client(self, gcs):
        gcs.reload.side_effect = Forbidden("denied")
        source = GCSSource({"bucket": BUCKET, "project": PROJECT})

        assert await source.connect() is False
        gcs.close.assert_called_once()


class TestDisconnect:
    async def test_closes_the_client(self, gcs, make_connected):
        source = await make_connected()
        client = source._client

        await source.disconnect()

        gcs.close.assert_called_once_with(client)
        assert (source._client, source._bucket) == (None, None)
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_is_idempotent(self, gcs, make_connected):
        source = await make_connected()
        await source.disconnect()
        await source.disconnect()
        gcs.close.assert_called_once()

    async def test_without_connect_is_a_noop(self, gcs):
        dest = GCSDestination({"bucket": BUCKET})
        await dest.disconnect()
        gcs.close.assert_not_called()
        assert (dest._client, dest._bucket) == (None, None)


class TestNotConnected:
    async def test_list_files(self):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await GCSSource({"bucket": BUCKET}).list_files()

    async def test_download_file(self, file_info, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await GCSSource({"bucket": BUCKET}).download_file(file_info("/a.pdf"), tmp_path / "a.pdf")

    async def test_upload_file(self, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await GCSDestination({"bucket": BUCKET}).upload_file(tmp_path / "a.md", "/a.md")


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_maps_blobs_to_file_info(self, gcs, make_connected):
        source = await make_connected()
        gcs.items = [
            resource("docs/report.pdf", size=1234, content_type="application/pdf"),
            resource("readme.md", size=7, content_type="text/markdown"),
        ]

        files = await source.list_files()

        assert [(f.name, f.path, f.size, f.modified_at, f.content_type) for f in files] == [
            ("report.pdf", "/docs/report.pdf", 1234, MTIME, "application/pdf"),
            ("readme.md", "/readme.md", 7, MTIME, "text/markdown"),
        ]

    async def test_skips_directory_markers(self, gcs, make_connected):
        source = await make_connected()
        gcs.items = [resource("docs/", size=0), resource("docs/a.pdf"), resource("docs/deep/", size=0)]

        assert [f.path for f in await source.list_files()] == ["/docs/a.pdf"]

    @pytest.mark.parametrize(
        ("extensions", "expected"),
        [
            (None, ["/a.pdf", "/B.PDF", "/c.docx", "/notes.txt", "/README"]),
            ([], ["/a.pdf", "/B.PDF", "/c.docx", "/notes.txt", "/README"]),
            ([".pdf"], ["/a.pdf", "/B.PDF"]),
            ([".PDF", ".Docx"], ["/a.pdf", "/B.PDF", "/c.docx"]),
            ([".xlsx"], []),
        ],
        ids=["none", "empty", "pdf", "mixed-case", "no-match"],
    )
    async def test_filters_by_extension_case_insensitively(self, gcs, make_connected, extensions, expected):
        source = await make_connected()
        gcs.items = [resource(n) for n in ("a.pdf", "B.PDF", "c.docx", "notes.txt", "README")]

        assert [f.path for f in await source.list_files(extensions=extensions)] == expected

    @pytest.mark.parametrize(
        ("prefix", "folder_path", "expected"),
        [
            (None, None, None),
            ("docs/", None, "docs/"),
            (None, "reports/2026/", "reports/2026/"),
            ("docs/", "reports/", "reports/"),
            ("", "", None),
        ],
        ids=["none", "config-prefix", "folder-path", "folder-path-wins", "empty"],
    )
    async def test_prefix_resolution(self, gcs, make_connected, prefix, folder_path, expected):
        source = await make_connected(prefix=prefix)

        assert await source.list_files(folder_path=folder_path) == []

        gcs.list_blobs.assert_called_once_with(source._client, BUCKET, prefix=expected)

    async def test_missing_metadata_falls_back(self, gcs, make_connected):
        source = await make_connected()
        gcs.items = [resource("bare.pdf", size=None, updated=None, content_type=None)]

        before = utcnow_naive()
        [info] = await source.list_files()
        after = utcnow_naive()

        assert (info.size, info.content_type) == (None, None)
        assert before - timedelta(seconds=1) <= info.modified_at <= after + timedelta(seconds=1)

    async def test_names_with_a_leading_slash_are_not_doubled(self, gcs, make_connected):
        source = await make_connected()
        gcs.items = [resource("/abs/a.pdf")]

        [info] = await source.list_files()

        assert (info.name, info.path) == ("a.pdf", "/abs/a.pdf")

    async def test_listing_errors_propagate(self, gcs, make_connected):
        source = await make_connected()
        gcs.list_blobs.side_effect = Forbidden("caller lacks storage.objects.list")

        with pytest.raises(Forbidden, match="storage.objects.list"):
            await source.list_files()

    async def test_destination_lists_too(self, gcs, make_connected):
        dest = await make_connected(GCSDestination, prefix="out/")
        gcs.items = [resource("out/a.md"), resource("out/a.pdf")]

        assert [f.path for f in await dest.list_files(extensions=[".md"])] == ["/out/a.md"]
        gcs.list_blobs.assert_called_once_with(dest._client, BUCKET, prefix="out/")


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    @pytest.mark.parametrize("cls", [GCSSource, GCSDestination])
    async def test_downloads_to_the_local_path(self, gcs, make_connected, file_info, tmp_path, cls):
        integration = await make_connected(cls)
        gcs.data = b"%PDF-1.7 content"
        target = tmp_path / "nested" / "dir" / "report.pdf"

        assert await integration.download_file(file_info("/docs/report.pdf"), target) is True

        assert target.read_bytes() == b"%PDF-1.7 content"
        gcs.download.assert_called_once_with(ANY, str(target))
        blob = gcs.download.call_args.args[0]
        assert (blob.bucket.name, blob.name) == (BUCKET, "docs/report.pdf")

    async def test_missing_object_returns_false(self, gcs, make_connected, file_info, tmp_path, caplog):
        source = await make_connected()
        gcs.download.side_effect = NotFound("No such object: mark-flow-docs/gone.pdf")
        target = tmp_path / "gone.pdf"

        assert await source.download_file(file_info("/gone.pdf"), target) is False

        assert not target.exists()
        assert "Failed to download /gone.pdf" in caplog.text


# --- upload_file / create_folder ---------------------------------------------

class TestUploadFile:
    @pytest.mark.parametrize(
        ("prefix", "remote_path", "expected_key"),
        [
            (None, "/docs/a.md", "docs/a.md"),
            (None, "a.md", "a.md"),
            ("out", "/docs/a.md", "out/docs/a.md"),
            ("/out/md/", "docs/a.md", "out/md/docs/a.md"),
        ],
    )
    async def test_uploads_under_the_prefixed_key(
        self, gcs, make_connected, tmp_path, prefix, remote_path, expected_key
    ):
        dest = await make_connected(GCSDestination, prefix=prefix)
        local = tmp_path / "a.md"
        local.write_bytes(b"# Title\n")

        assert await dest.upload_file(local, remote_path) is True

        assert gcs.uploads == {expected_key: b"# Title\n"}
        gcs.upload.assert_called_once_with(ANY, str(local))
        assert gcs.upload.call_args.args[0].bucket.name == BUCKET

    async def test_service_error_returns_false(self, gcs, make_connected, tmp_path, caplog):
        dest = await make_connected(GCSDestination)
        local = tmp_path / "a.md"
        local.write_bytes(b"x")
        gcs.upload.side_effect = Forbidden("caller lacks storage.objects.create")

        assert await dest.upload_file(local, "/a.md") is False
        assert "Failed to upload /a.md" in caplog.text

    async def test_missing_local_file_returns_false(self, gcs, make_connected, tmp_path):
        dest = await make_connected(GCSDestination)

        assert await dest.upload_file(tmp_path / "missing.md", "/missing.md") is False
        assert gcs.uploads == {}

    async def test_create_folder_is_a_noop(self, gcs, make_connected):
        dest = await make_connected(GCSDestination)
        assert await dest.create_folder("/any/folder") is True
        gcs.upload.assert_not_called()
