"""
Tests for ``src.integration.s3`` (AWS S3 source + destination).

The integration drives a REAL aiobotocore session and S3 client: client
creation, endpoint resolution, parameter validation against the S3 service
model and pagination all run for real. Only the wire is replaced, with
``aiobotocore.stub.AioStubber``, so an incompatible SDK upgrade (renamed
operation, changed parameter, different paginator) fails a test instead of
failing in production. AWS environment/config files are isolated so the
developer's credentials are never read.
"""
import logging
from datetime import datetime, timedelta, timezone

import pytest
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession
from aiobotocore.stub import AioStubber
from pydantic import ValidationError

from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration
from src.integration.s3 import S3Config, S3Destination, S3Source

LOGGER = "src.integration.s3"
BUCKET = "mark-flow-docs"
NOT_CONNECTED = "Not connected"
MTIME = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)

AWS_ENV_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
]


@pytest.fixture(autouse=True)
def _isolated_aws_env(monkeypatch, tmp_path):
    """Never read the developer's AWS credentials/config, never query IMDS."""
    for name in AWS_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config-missing"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials-missing"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


class StubbedS3:
    """Attaches an ``AioStubber`` to every real S3 client the integration creates.

    Responses queued with :meth:`add_response` / :meth:`add_client_error` before
    ``connect()`` are replayed by the stubber once the client exists.
    """

    def __init__(self):
        self.create_kwargs: list[dict] = []
        self.clients: list[AioBaseClient] = []
        self.stubber: AioStubber | None = None
        self._pending: list[tuple[str, tuple, dict]] = []

    def add_response(self, method, response, expected_params=None):
        self._queue("add_response", (method, response, expected_params), {})

    def add_client_error(self, method, code, status, expected_params=None):
        self._queue(
            "add_client_error",
            (method,),
            {
                "service_error_code": code,
                "service_message": f"{code} (stubbed)",
                "http_status_code": status,
                "expected_params": expected_params,
            },
        )

    def _queue(self, kind, args, kwargs):
        if self.stubber is None:
            self._pending.append((kind, args, kwargs))
        else:
            getattr(self.stubber, kind)(*args, **kwargs)

    def attach(self, client):
        self.clients.append(client)
        self.stubber = AioStubber(client)
        for kind, args, kwargs in self._pending:
            getattr(self.stubber, kind)(*args, **kwargs)
        self._pending.clear()
        self.stubber.activate()

    def assert_all_consumed(self):
        assert not self._pending, "responses queued but no client was created"
        if self.stubber is not None:
            self.stubber.assert_no_pending_responses()


@pytest.fixture
def s3(monkeypatch):
    harness = StubbedS3()
    real_create_client = AioSession.create_client

    class _StubbingContext:
        def __init__(self, inner):
            self._inner = inner

        async def __aenter__(self):
            client = await self._inner.__aenter__()
            harness.attach(client)
            return client

        async def __aexit__(self, *exc_info):
            return await self._inner.__aexit__(*exc_info)

    def create_client(self, *args, **kwargs):
        harness.create_kwargs.append(kwargs)
        return _StubbingContext(real_create_client(self, *args, **kwargs))

    monkeypatch.setattr(AioSession, "create_client", create_client)
    return harness


@pytest.fixture
def close_spy(monkeypatch):
    """Records every real S3 client whose context is exited (connections closed)."""
    closed = []
    real_aexit = AioBaseClient.__aexit__

    async def aexit(self, *exc_info):
        closed.append(self)
        return await real_aexit(self, *exc_info)

    monkeypatch.setattr(AioBaseClient, "__aexit__", aexit)
    return closed


@pytest.fixture
async def make_connected(s3):
    """Factory for integrations connected to the stubbed bucket."""
    created = []

    async def _make(cls=S3Source, **config):
        config.setdefault("bucket", BUCKET)
        config.setdefault("region", "eu-west-1")
        config.setdefault("access_key_id", "AKIATESTTESTTEST")
        config.setdefault("secret_access_key", "secret")
        s3.add_response("head_bucket", {}, {"Bucket": config["bucket"]})
        integration = cls(config)
        assert await integration.connect() is True
        created.append(integration)
        return integration

    yield _make
    for integration in created:
        await integration.disconnect()
    s3.assert_all_consumed()


def obj(key, size=10, modified=MTIME):
    entry = {"Key": key, "Size": size}
    if modified is not None:
        entry["LastModified"] = modified
    return entry


def page(*objects, next_token=None):
    response = {"Contents": list(objects), "IsTruncated": next_token is not None, "KeyCount": len(objects)}
    if next_token:
        response["NextContinuationToken"] = next_token
    return response


def utcnow_naive() -> datetime:
    """What ``datetime.utcnow()`` returns, without its deprecation warning."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class FakeBody:
    """Async streaming body as returned by aiobotocore's ``get_object``."""

    def __init__(self, data: bytes):
        self._data = data

    async def read(self) -> bytes:
        return self._data


# --- Config ------------------------------------------------------------------

class TestConfig:
    def test_bucket_is_required(self):
        with pytest.raises(ValidationError) as exc:
            S3Config.model_validate({"region": "eu-west-1"})
        assert exc.value.errors()[0]["loc"] == ("bucket",)

    def test_defaults(self):
        config = S3Config.model_validate({"bucket": BUCKET})
        assert config.model_dump() == {
            "bucket": BUCKET,
            "prefix": None,
            "region": None,
            "access_key_id": None,
            "secret_access_key": None,
            "session_token": None,
            "endpoint_url": None,
        }

    @pytest.mark.parametrize(
        "raw",
        [
            {
                "region": "eu-south-1",
                "accessKeyId": "AK",
                "secretAccessKey": "SK",
                "sessionToken": "ST",
                "endpointUrl": "http://localhost:9000",
            },
            {
                "region_name": "eu-south-1",
                "aws_access_key_id": "AK",
                "aws_secret_access_key": "SK",
                "aws_session_token": "ST",
                "endpoint_url": "http://localhost:9000",
            },
            {
                "region": "eu-south-1",
                "access_key_id": "AK",
                "secret_access_key": "SK",
                "session_token": "ST",
                "endpoint_url": "http://localhost:9000",
            },
        ],
        ids=["camelCase", "boto-style", "snake_case"],
    )
    def test_aliases(self, raw):
        config = S3Config.model_validate({"bucket": BUCKET, **raw})
        assert (
            config.region,
            config.access_key_id,
            config.secret_access_key,
            config.session_token,
            config.endpoint_url,
        ) == ("eu-south-1", "AK", "SK", "ST", "http://localhost:9000")

    def test_unknown_keys_are_ignored(self):
        config = S3Config.model_validate({"bucket": BUCKET, "type": "s3", "container": "x"})
        assert not hasattr(config, "container")

    @pytest.mark.parametrize("cls", [S3Source, S3Destination])
    def test_invalid_config_is_rejected_by_the_integration(self, cls):
        with pytest.raises(ValidationError):
            cls({"prefix": "docs"})


class TestIdentity:
    def test_source(self):
        source = S3Source({"bucket": BUCKET})
        assert source.integration_type is IntegrationType.S3
        assert isinstance(source, SourceIntegration)
        assert not isinstance(source, DestinationIntegration)

    def test_destination(self):
        dest = S3Destination({"bucket": BUCKET})
        assert dest.integration_type is IntegrationType.S3
        assert isinstance(dest, DestinationIntegration)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    @pytest.mark.parametrize(
        ("config", "expected_kwargs"),
        [
            ({}, {}),
            ({"region": "eu-west-1"}, {"region_name": "eu-west-1"}),
            (
                {
                    "region": "eu-west-1",
                    "access_key_id": "AKIATESTTESTTEST",
                    "secret_access_key": "secret",
                    "session_token": "token",
                    "endpoint_url": "http://localhost:9000",
                },
                {
                    "region_name": "eu-west-1",
                    "aws_access_key_id": "AKIATESTTESTTEST",
                    "aws_secret_access_key": "secret",
                    "aws_session_token": "token",
                    "endpoint_url": "http://localhost:9000",
                },
            ),
        ],
        ids=["defaults", "region-only", "all-options"],
    )
    @pytest.mark.parametrize("cls", [S3Source, S3Destination])
    async def test_success_forwards_only_configured_options(self, s3, cls, config, expected_kwargs):
        s3.add_response("head_bucket", {}, {"Bucket": BUCKET})
        integration = cls({"bucket": BUCKET, **config})

        assert await integration.connect() is True

        assert s3.create_kwargs == [expected_kwargs]
        client = s3.clients[0]
        assert client.meta.service_model.service_name == "s3"
        if "region_name" in expected_kwargs:
            assert client.meta.region_name == "eu-west-1"
        if "endpoint_url" in expected_kwargs:
            assert client.meta.endpoint_url == "http://localhost:9000"
        await integration.disconnect()
        s3.assert_all_consumed()

    async def test_logs_the_bucket_on_success(self, s3, caplog):
        s3.add_response("head_bucket", {}, {"Bucket": BUCKET})
        source = S3Source({"bucket": BUCKET, "region": "eu-west-1"})
        with caplog.at_level(logging.INFO, logger=LOGGER):
            assert await source.connect() is True
        assert f"Connected to S3 bucket: {BUCKET}" in caplog.text
        await source.disconnect()

    @pytest.mark.parametrize(
        ("code", "status"),
        [("404", 404), ("403", 403), ("NoSuchBucket", 404), ("InternalError", 500)],
    )
    async def test_head_bucket_failure_returns_false(self, s3, caplog, code, status):
        s3.add_client_error("head_bucket", code, status, {"Bucket": BUCKET})
        source = S3Source({"bucket": BUCKET, "region": "eu-west-1"})

        assert await source.connect() is False

        assert source._client is None
        assert "Failed to connect to S3" in caplog.text
        s3.assert_all_consumed()
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()
        await s3.clients[0].__aexit__(None, None, None)

    async def test_invalid_endpoint_returns_false(self, s3, caplog):
        source = S3Source({"bucket": BUCKET, "endpoint_url": "not a url"})

        assert await source.connect() is False

        assert source._client is None
        assert s3.clients == []
        assert "Failed to connect to S3" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: s3.py:81-89 enters the client context but never exits it when head_bucket "
        "fails, so the client (and its HTTP connection pool) leaks; disconnect() can't help "
        "because _client is reset to None",
    )
    async def test_failed_connect_closes_the_client(self, s3, close_spy):
        s3.add_client_error("head_bucket", "403", 403, {"Bucket": BUCKET})
        source = S3Source({"bucket": BUCKET, "region": "eu-west-1"})
        try:
            assert await source.connect() is False
            assert close_spy == s3.clients
        finally:
            for client in s3.clients:
                if client not in close_spy:
                    await AioBaseClient.__aexit__(client, None, None, None)


class TestDisconnect:
    async def test_closes_the_client(self, make_connected, close_spy):
        source = await make_connected()
        client = source._client

        await source.disconnect()

        assert close_spy == [client]
        assert source._client is None
        assert source._session is None
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_is_idempotent(self, make_connected, close_spy):
        source = await make_connected()
        await source.disconnect()
        await source.disconnect()
        assert len(close_spy) == 1

    async def test_without_connect_is_a_noop(self, close_spy):
        dest = S3Destination({"bucket": BUCKET})
        await dest.disconnect()
        assert close_spy == []
        assert dest._client is None


class TestNotConnected:
    async def test_list_files(self):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await S3Source({"bucket": BUCKET}).list_files()

    async def test_download_file(self, file_info, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await S3Source({"bucket": BUCKET}).download_file(file_info("/a.pdf"), tmp_path / "a.pdf")

    async def test_upload_file(self, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await S3Destination({"bucket": BUCKET}).upload_file(tmp_path / "a.md", "/a.md")


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_maps_objects_to_file_info(self, s3, make_connected):
        source = await make_connected()
        s3.add_response(
            "list_objects_v2",
            page(obj("docs/report.pdf", size=1234), obj("readme.md", size=7)),
            {"Bucket": BUCKET},
        )

        files = await source.list_files()

        assert [(f.name, f.path, f.size, f.modified_at, f.content_type) for f in files] == [
            ("report.pdf", "/docs/report.pdf", 1234, MTIME, None),
            ("readme.md", "/readme.md", 7, MTIME, None),
        ]

    async def test_follows_continuation_tokens(self, s3, make_connected):
        source = await make_connected()
        s3.add_response("list_objects_v2", page(obj("a.pdf"), obj("b.pdf"), next_token="tok-1"), {"Bucket": BUCKET})
        s3.add_response(
            "list_objects_v2",
            page(obj("c.pdf"), next_token="tok-2"),
            {"Bucket": BUCKET, "ContinuationToken": "tok-1"},
        )
        s3.add_response("list_objects_v2", page(obj("d.pdf")), {"Bucket": BUCKET, "ContinuationToken": "tok-2"})

        files = await source.list_files()

        assert [f.path for f in files] == ["/a.pdf", "/b.pdf", "/c.pdf", "/d.pdf"]

    async def test_skips_directory_markers(self, s3, make_connected):
        source = await make_connected()
        s3.add_response(
            "list_objects_v2",
            page(obj("docs/", size=0), obj("docs/a.pdf"), obj("docs/deep/", size=0)),
            {"Bucket": BUCKET},
        )
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
    async def test_filters_by_extension_case_insensitively(self, s3, make_connected, extensions, expected):
        source = await make_connected()
        s3.add_response(
            "list_objects_v2",
            page(obj("a.pdf"), obj("B.PDF"), obj("c.docx"), obj("notes.txt"), obj("README")),
            {"Bucket": BUCKET},
        )
        assert [f.path for f in await source.list_files(extensions=extensions)] == expected

    @pytest.mark.parametrize(
        ("prefix", "folder_path", "expected_prefix"),
        [
            (None, None, None),
            ("docs/", None, "docs/"),
            (None, "reports/2026/", "reports/2026/"),
            ("docs/", "reports/", "reports/"),
            ("", "", None),
        ],
        ids=["none", "config-prefix", "folder-path", "folder-path-wins", "empty"],
    )
    async def test_prefix_resolution(self, s3, make_connected, prefix, folder_path, expected_prefix):
        source = await make_connected(prefix=prefix)
        params = {"Bucket": BUCKET}
        if expected_prefix:
            params["Prefix"] = expected_prefix
        s3.add_response("list_objects_v2", page(), params)

        assert await source.list_files(folder_path=folder_path) == []

    async def test_empty_bucket(self, s3, make_connected):
        source = await make_connected()
        s3.add_response("list_objects_v2", {"IsTruncated": False, "KeyCount": 0}, {"Bucket": BUCKET})
        assert await source.list_files() == []

    async def test_missing_metadata_falls_back(self, s3, make_connected):
        source = await make_connected()
        s3.add_response("list_objects_v2", page({"Key": "bare.pdf"}), {"Bucket": BUCKET})

        before = utcnow_naive()
        [info] = await source.list_files()
        after = utcnow_naive()

        assert info.size is None
        assert before - timedelta(seconds=1) <= info.modified_at <= after + timedelta(seconds=1)

    async def test_keys_with_a_leading_slash_are_not_doubled(self, s3, make_connected):
        source = await make_connected()
        s3.add_response("list_objects_v2", page(obj("/abs/a.pdf")), {"Bucket": BUCKET})
        [info] = await source.list_files()
        assert (info.name, info.path) == ("a.pdf", "/abs/a.pdf")

    async def test_listing_errors_propagate(self, s3, make_connected):
        from botocore.exceptions import ClientError

        source = await make_connected()
        s3.add_client_error("list_objects_v2", "AccessDenied", 403, {"Bucket": BUCKET})
        with pytest.raises(ClientError, match="AccessDenied"):
            await source.list_files()

    async def test_destination_lists_too(self, s3, make_connected):
        dest = await make_connected(S3Destination, prefix="out/")
        s3.add_response(
            "list_objects_v2",
            page(obj("out/a.md"), obj("out/a.pdf")),
            {"Bucket": BUCKET, "Prefix": "out/"},
        )
        assert [f.path for f in await dest.list_files(extensions=[".md"])] == ["/out/a.md"]


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    @pytest.mark.parametrize("cls", [S3Source, S3Destination])
    async def test_writes_the_object_body(self, s3, make_connected, file_info, tmp_path, cls):
        integration = await make_connected(cls)
        s3.add_response(
            "get_object",
            {"Body": FakeBody(b"%PDF-1.7 content"), "ContentLength": 16},
            {"Bucket": BUCKET, "Key": "docs/report.pdf"},
        )
        target = tmp_path / "nested" / "dir" / "report.pdf"

        assert await integration.download_file(file_info("/docs/report.pdf"), target) is True

        assert target.read_bytes() == b"%PDF-1.7 content"

    async def test_missing_object_returns_false(self, s3, make_connected, file_info, tmp_path, caplog):
        source = await make_connected()
        s3.add_client_error("get_object", "NoSuchKey", 404, {"Bucket": BUCKET, "Key": "gone.pdf"})
        target = tmp_path / "out" / "gone.pdf"

        assert await source.download_file(file_info("/gone.pdf"), target) is False

        assert not target.exists()
        assert "Failed to download /gone.pdf" in caplog.text

    async def test_body_read_error_returns_false(self, s3, make_connected, file_info, tmp_path):
        class BrokenBody(FakeBody):
            async def read(self):
                raise ConnectionResetError("peer reset")

        source = await make_connected()
        s3.add_response("get_object", {"Body": BrokenBody(b"")}, {"Bucket": BUCKET, "Key": "a.pdf"})
        target = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/a.pdf"), target) is False
        assert not target.exists()


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
    async def test_puts_the_file_under_the_prefixed_key(
        self, s3, make_connected, tmp_path, prefix, remote_path, expected_key
    ):
        dest = await make_connected(S3Destination, prefix=prefix)
        local = tmp_path / "a.md"
        local.write_bytes(b"# Title\n")
        s3.add_response(
            "put_object",
            {"ETag": '"etag"'},
            {"Bucket": BUCKET, "Key": expected_key, "Body": b"# Title\n"},
        )

        assert await dest.upload_file(local, remote_path) is True

    async def test_service_error_returns_false(self, s3, make_connected, tmp_path, caplog):
        dest = await make_connected(S3Destination)
        local = tmp_path / "a.md"
        local.write_bytes(b"x")
        s3.add_client_error("put_object", "AccessDenied", 403, {"Bucket": BUCKET, "Key": "a.md", "Body": b"x"})

        assert await dest.upload_file(local, "/a.md") is False
        assert "Failed to upload /a.md" in caplog.text

    async def test_missing_local_file_returns_false(self, make_connected, tmp_path):
        dest = await make_connected(S3Destination)
        assert await dest.upload_file(tmp_path / "missing.md", "/missing.md") is False

    async def test_create_folder_is_a_noop(self, make_connected):
        dest = await make_connected(S3Destination)
        assert await dest.create_folder("/any/folder") is True
