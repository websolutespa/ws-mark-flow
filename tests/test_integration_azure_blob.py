"""
Tests for ``src.integration.azure_blob`` (Azure Blob Storage source + destination).

The integration builds REAL ``azure.storage.blob.aio`` clients (connection
string parsing, account URL + shared-key credential, blob client derivation)
and pages through a REAL ``azure.core`` ``AsyncItemPaged`` of real
``BlobProperties``. Only the methods that would hit the wire are replaced, with
autospecced fakes, so a changed SDK signature fails a test instead of failing
in production.
"""
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

import pytest
from azure.core.async_paging import AsyncItemPaged
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobProperties
from azure.storage.blob.aio import BlobClient, ContainerClient, StorageStreamDownloader
from pydantic import ValidationError

from src.integration.azure_blob import AzureBlobConfig, AzureBlobDestination, AzureBlobSource
from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration

LOGGER = "src.integration.azure_blob"
ACCOUNT = "markflowtests"
ACCOUNT_KEY = "c2VjcmV0LWtleQ=="  # base64, like a real storage account key
CONTAINER = "documents"
CONN_STR = (
    f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT};AccountKey={ACCOUNT_KEY};"
    "EndpointSuffix=core.windows.net"
)
AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=a2V5;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
NOT_CONNECTED = "Not connected"
MTIME = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)


def utcnow_naive() -> datetime:
    """What ``datetime.utcnow()`` returns, without its deprecation warning."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def blob(name, size=10, modified=MTIME, content_type="application/octet-stream"):
    """A real ``BlobProperties`` as deserialized from a List Blobs response."""
    return BlobProperties(
        name=name,
        **{"Content-Length": size, "Last-Modified": modified, "Content-Type": content_type},
    )


def paged(*pages):
    """A real ``AsyncItemPaged`` serving ``pages`` through continuation tokens."""

    async def get_next(token):
        return int(token or 0)

    async def extract_data(index):
        next_token = str(index + 1) if index + 1 < len(pages) else None
        return next_token, list(pages[index])

    return AsyncItemPaged(get_next, extract_data)


class FakeAzure:
    """Autospecced stand-ins for every network-bound SDK method used by the integration."""

    def __init__(self, monkeypatch):
        self.pages: list[list[BlobProperties]] = [[]]
        self.data = b""
        self.real_close = ContainerClient.close

        self.get_container_properties = self._patch(monkeypatch, ContainerClient, "get_container_properties")
        self.list_blobs = self._patch(
            monkeypatch, ContainerClient, "list_blobs", side_effect=lambda *a, **kw: paged(*self.pages)
        )
        self.close = self._patch(monkeypatch, ContainerClient, "close", side_effect=self.real_close)
        self.downloader = create_autospec(StorageStreamDownloader, instance=True)
        self.downloader.readall.side_effect = lambda: self.data
        self.download_blob = self._patch(monkeypatch, BlobClient, "download_blob", return_value=self.downloader)
        self.upload_blob = self._patch(monkeypatch, BlobClient, "upload_blob", return_value={"etag": '"0x1"'})

    @staticmethod
    def _patch(monkeypatch, cls, name, **kwargs):
        fake = create_autospec(getattr(cls, name), **kwargs)
        monkeypatch.setattr(cls, name, fake)
        return fake


@pytest.fixture
def azure(monkeypatch) -> FakeAzure:
    return FakeAzure(monkeypatch)


@pytest.fixture
async def make_connected(azure):
    created = []

    async def _make(cls=AzureBlobSource, **config):
        config.setdefault("connection_string", CONN_STR)
        config.setdefault("container", CONTAINER)
        integration = cls(config)
        assert await integration.connect() is True
        created.append(integration)
        return integration

    yield _make
    for integration in created:
        await integration.disconnect()


# --- Config ------------------------------------------------------------------

class TestConfig:
    def test_container_is_required(self):
        with pytest.raises(ValidationError) as exc:
            AzureBlobConfig.model_validate({"connection_string": CONN_STR})
        assert exc.value.errors()[0]["loc"] == ("container",)

    def test_defaults(self):
        assert AzureBlobConfig.model_validate({"container": CONTAINER}).model_dump() == {
            "connection_string": None,
            "account_name": None,
            "account_key": None,
            "container": CONTAINER,
            "prefix": None,
        }

    @pytest.mark.parametrize(
        "raw",
        [
            {"connectionString": "cs", "accountName": "acc", "accountKey": "key"},
            {"connection_string": "cs", "account_name": "acc", "account_key": "key"},
        ],
        ids=["camelCase", "snake_case"],
    )
    def test_aliases(self, raw):
        config = AzureBlobConfig.model_validate({"container": CONTAINER, **raw})
        assert (config.connection_string, config.account_name, config.account_key) == ("cs", "acc", "key")

    def test_unknown_keys_are_ignored(self):
        config = AzureBlobConfig.model_validate({"container": CONTAINER, "bucket": "x", "type": "azure_blob"})
        assert not hasattr(config, "bucket")

    @pytest.mark.parametrize("cls", [AzureBlobSource, AzureBlobDestination])
    def test_invalid_config_is_rejected_by_the_integration(self, cls):
        with pytest.raises(ValidationError):
            cls({"connection_string": CONN_STR})


class TestIdentity:
    def test_source(self):
        source = AzureBlobSource({"container": CONTAINER})
        assert source.integration_type is IntegrationType.AZURE_BLOB
        assert isinstance(source, SourceIntegration)
        assert not isinstance(source, DestinationIntegration)

    def test_destination(self):
        dest = AzureBlobDestination({"container": CONTAINER})
        assert dest.integration_type is IntegrationType.AZURE_BLOB
        assert isinstance(dest, DestinationIntegration)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    @pytest.mark.parametrize("cls", [AzureBlobSource, AzureBlobDestination])
    async def test_connection_string(self, azure, cls, caplog):
        integration = cls({"connectionString": CONN_STR, "container": CONTAINER})

        with caplog.at_level(logging.INFO, logger=LOGGER):
            assert await integration.connect() is True

        client = integration._container_client
        assert isinstance(client, ContainerClient)
        assert client.url == f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}"
        assert (client.account_name, client.container_name) == (ACCOUNT, CONTAINER)
        assert client.credential.account_key == ACCOUNT_KEY
        azure.get_container_properties.assert_awaited_once_with(client)
        assert f"Connected to Azure Blob container: {CONTAINER}" in caplog.text
        await integration.disconnect()

    async def test_azurite_connection_string(self, azure):
        source = AzureBlobSource({"connection_string": AZURITE_CONN_STR, "container": CONTAINER})

        assert await source.connect() is True

        assert source._container_client.url == f"http://127.0.0.1:10000/devstoreaccount1/{CONTAINER}"
        await source.disconnect()

    async def test_connection_string_wins_over_account_key(self, azure):
        source = AzureBlobSource(
            {"connection_string": CONN_STR, "account_name": "other", "account_key": "b3RoZXI=", "container": CONTAINER}
        )
        assert await source.connect() is True
        assert source._container_client.account_name == ACCOUNT
        await source.disconnect()

    async def test_account_name_and_key(self, azure):
        source = AzureBlobSource({"accountName": ACCOUNT, "accountKey": ACCOUNT_KEY, "container": CONTAINER})

        assert await source.connect() is True

        client = source._container_client
        assert client.url == f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}"
        assert client.credential.account_name == ACCOUNT
        assert client.credential.account_key == ACCOUNT_KEY
        azure.get_container_properties.assert_awaited_once_with(client)
        await source.disconnect()

    @pytest.mark.parametrize(
        "credentials",
        [{}, {"account_name": ACCOUNT}, {"account_key": ACCOUNT_KEY}, {"connection_string": ""}],
        ids=["nothing", "name-only", "key-only", "empty-connection-string"],
    )
    async def test_missing_credentials_return_false(self, azure, caplog, credentials):
        source = AzureBlobSource({"container": CONTAINER, **credentials})

        assert await source.connect() is False

        assert source._container_client is None
        azure.get_container_properties.assert_not_awaited()
        assert "provide connection_string or account_name+account_key" in caplog.text

    async def test_malformed_connection_string_returns_false(self, azure, caplog):
        source = AzureBlobSource({"connection_string": "not-a-connection-string", "container": CONTAINER})

        assert await source.connect() is False

        assert source._container_client is None
        assert "Failed to connect to Azure Blob Storage" in caplog.text

    @pytest.mark.parametrize(
        "error",
        [
            ResourceNotFoundError("The specified container does not exist."),
            ClientAuthenticationError("Server failed to authenticate the request."),
            HttpResponseError("Service unavailable"),
        ],
        ids=["missing-container", "bad-key", "server-error"],
    )
    async def test_verification_failure_returns_false(self, azure, caplog, error):
        azure.get_container_properties.side_effect = error
        source = AzureBlobSource({"connection_string": CONN_STR, "container": CONTAINER})

        assert await source.connect() is False

        assert source._container_client is None
        assert "Failed to connect to Azure Blob Storage" in caplog.text
        assert str(error) in caplog.text
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: azure_blob.py:72-77 drops the ContainerClient without closing it when the "
        "container check fails, leaking its aiohttp session; disconnect() can't help because "
        "_container_client is reset to None",
    )
    async def test_failed_connect_closes_the_client(self, azure):
        azure.get_container_properties.side_effect = ResourceNotFoundError("no container")
        source = AzureBlobSource({"connection_string": CONN_STR, "container": CONTAINER})

        assert await source.connect() is False
        azure.close.assert_awaited_once()


class TestDisconnect:
    async def test_closes_the_client(self, azure, make_connected):
        source = await make_connected()
        client = source._container_client

        await source.disconnect()

        azure.close.assert_awaited_once_with(client)
        assert source._container_client is None
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_is_idempotent(self, azure, make_connected):
        source = await make_connected()
        await source.disconnect()
        await source.disconnect()
        azure.close.assert_awaited_once()

    async def test_without_connect_is_a_noop(self, azure):
        await AzureBlobDestination({"container": CONTAINER}).disconnect()
        azure.close.assert_not_awaited()


class TestNotConnected:
    async def test_list_files(self):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await AzureBlobSource({"container": CONTAINER}).list_files()

    async def test_download_file(self, file_info, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await AzureBlobSource({"container": CONTAINER}).download_file(file_info("/a.pdf"), tmp_path / "a.pdf")

    async def test_upload_file(self, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await AzureBlobDestination({"container": CONTAINER}).upload_file(tmp_path / "a.md", "/a.md")


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_maps_blobs_to_file_info(self, azure, make_connected):
        source = await make_connected()
        azure.pages = [[blob("docs/report.pdf", size=1234, content_type="application/pdf"), blob("readme.md", size=7)]]

        files = await source.list_files()

        assert [(f.name, f.path, f.size, f.modified_at, f.content_type) for f in files] == [
            ("report.pdf", "/docs/report.pdf", 1234, MTIME, "application/pdf"),
            ("readme.md", "/readme.md", 7, MTIME, "application/octet-stream"),
        ]

    async def test_reads_every_page(self, azure, make_connected):
        source = await make_connected()
        azure.pages = [[blob("a.pdf"), blob("b.pdf")], [blob("c.pdf")], [], [blob("d.pdf")]]

        assert [f.path for f in await source.list_files()] == ["/a.pdf", "/b.pdf", "/c.pdf", "/d.pdf"]

    async def test_skips_directory_markers(self, azure, make_connected):
        source = await make_connected()
        azure.pages = [[blob("docs/", size=0), blob("docs/a.pdf"), blob("docs/deep/", size=0)]]

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
    async def test_filters_by_extension_case_insensitively(self, azure, make_connected, extensions, expected):
        source = await make_connected()
        azure.pages = [[blob("a.pdf"), blob("B.PDF"), blob("c.docx"), blob("notes.txt"), blob("README")]]

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
    async def test_prefix_resolution(self, azure, make_connected, prefix, folder_path, expected):
        source = await make_connected(prefix=prefix)

        assert await source.list_files(folder_path=folder_path) == []

        azure.list_blobs.assert_called_once_with(source._container_client, name_starts_with=expected)

    async def test_missing_metadata_falls_back(self, azure, make_connected):
        source = await make_connected()
        bare = BlobProperties(name="bare.pdf")
        no_settings = blob("no-settings.pdf")
        no_settings.content_settings = None
        azure.pages = [[bare, no_settings]]

        before = utcnow_naive()
        files = await source.list_files()
        after = utcnow_naive()

        assert [(f.size, f.content_type) for f in files] == [(None, None), (10, None)]
        assert before - timedelta(seconds=1) <= files[0].modified_at <= after + timedelta(seconds=1)

    async def test_names_with_a_leading_slash_are_not_doubled(self, azure, make_connected):
        source = await make_connected()
        azure.pages = [[blob("/abs/a.pdf")]]

        [info] = await source.list_files()

        assert (info.name, info.path) == ("a.pdf", "/abs/a.pdf")

    async def test_listing_errors_propagate(self, azure, make_connected):
        source = await make_connected()
        azure.list_blobs.side_effect = HttpResponseError("AuthorizationFailure")

        with pytest.raises(HttpResponseError, match="AuthorizationFailure"):
            await source.list_files()

    async def test_destination_lists_too(self, azure, make_connected):
        dest = await make_connected(AzureBlobDestination, prefix="out/")
        azure.pages = [[blob("out/a.md"), blob("out/a.pdf")]]

        assert [f.path for f in await dest.list_files(extensions=[".md"])] == ["/out/a.md"]
        azure.list_blobs.assert_called_once_with(dest._container_client, name_starts_with="out/")


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    @pytest.mark.parametrize("cls", [AzureBlobSource, AzureBlobDestination])
    async def test_writes_the_blob_content(self, azure, make_connected, file_info, tmp_path, cls):
        integration = await make_connected(cls)
        azure.data = b"%PDF-1.7 content"
        target = tmp_path / "nested" / "dir" / "report.pdf"

        assert await integration.download_file(file_info("/docs/report.pdf"), target) is True

        assert target.read_bytes() == b"%PDF-1.7 content"
        azure.download_blob.assert_awaited_once()
        blob_client = azure.download_blob.call_args.args[0]
        assert (blob_client.container_name, blob_client.blob_name) == (CONTAINER, "docs/report.pdf")
        azure.downloader.readall.assert_awaited_once_with()

    async def test_missing_blob_returns_false(self, azure, make_connected, file_info, tmp_path, caplog):
        source = await make_connected()
        azure.download_blob.side_effect = ResourceNotFoundError("The specified blob does not exist.")
        target = tmp_path / "gone.pdf"

        assert await source.download_file(file_info("/gone.pdf"), target) is False

        assert not target.exists()
        assert "Failed to download blob /gone.pdf" in caplog.text

    async def test_read_error_returns_false(self, azure, make_connected, file_info, tmp_path):
        source = await make_connected()
        azure.downloader.readall.side_effect = HttpResponseError("connection reset")
        target = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/a.pdf"), target) is False
        assert not target.exists()


# --- upload_file / create_folder ---------------------------------------------

class TestUploadFile:
    @pytest.mark.parametrize(
        ("prefix", "remote_path", "expected_blob"),
        [
            (None, "/docs/a.md", "docs/a.md"),
            (None, "a.md", "a.md"),
            ("out", "/docs/a.md", "out/docs/a.md"),
            ("/out/md/", "docs/a.md", "out/md/docs/a.md"),
        ],
    )
    async def test_uploads_under_the_prefixed_name(
        self, azure, make_connected, tmp_path, prefix, remote_path, expected_blob
    ):
        dest = await make_connected(AzureBlobDestination, prefix=prefix)
        local = tmp_path / "a.md"
        local.write_bytes(b"# Title\n")

        assert await dest.upload_file(local, remote_path) is True

        azure.upload_blob.assert_awaited_once()
        blob_client = azure.upload_blob.call_args.args[0]
        assert (blob_client.container_name, blob_client.blob_name) == (CONTAINER, expected_blob)
        azure.upload_blob.assert_awaited_once_with(blob_client, b"# Title\n", overwrite=True)

    async def test_service_error_returns_false(self, azure, make_connected, tmp_path, caplog):
        dest = await make_connected(AzureBlobDestination)
        local = tmp_path / "a.md"
        local.write_bytes(b"x")
        azure.upload_blob.side_effect = HttpResponseError("This request is not authorized")

        assert await dest.upload_file(local, "/a.md") is False
        assert "Failed to upload blob /a.md" in caplog.text

    async def test_missing_local_file_returns_false(self, azure, make_connected, tmp_path):
        dest = await make_connected(AzureBlobDestination)

        assert await dest.upload_file(tmp_path / "missing.md", "/missing.md") is False
        azure.upload_blob.assert_not_awaited()

    async def test_create_folder_is_a_noop(self, azure, make_connected):
        dest = await make_connected(AzureBlobDestination)
        assert await dest.create_folder("/any/folder") is True
        azure.upload_blob.assert_not_awaited()
