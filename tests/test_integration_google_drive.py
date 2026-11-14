"""
Tests for ``src.integration.google_drive``.

HTTP is mocked at the httpx transport layer with respx; everything else is the
real thing: pydantic validation, PyJWT + cryptography signing of the
service-account assertion, httpx request building (query encoding, form
bodies, headers) and local file I/O.
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import ValidationError

from src.integration import google_drive as gd
from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration
from src.integration.google_drive import GoogleDriveConfig, GoogleDriveDestination, GoogleDriveSource

TOKEN_URL = "https://oauth2.googleapis.com/token"
FILES_URL = "https://www.googleapis.com/drive/v3/files"
UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
LIST_FIELDS = "nextPageToken, files(id, name, mimeType, modifiedTime, size)"

ACCESS_TOKEN = "ya29.a0-user-access-token"
SA_TOKEN = "ya29.c.service-account-token"
SA_EMAIL = "mark-flow@mark-flow-tests.iam.gserviceaccount.com"

HTTP_ERRORS = [401, 403, 404, 429, 500]
TRANSPORT_ERRORS = [httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout]

# Remote paths are built with ``pathlib.Path`` in upload_file(), which turns
# "/docs/sub" into "\\docs\\sub" on Windows. Linux (the deployment target) is fine.
windows_remote_path_bug = pytest.mark.xfail(
    sys.platform == "win32",
    strict=True,
    reason="BUG: google_drive.py:381 builds the remote parent path with pathlib.Path "
    "(backslashes on Windows), so folders are never split on '/'",
)


# --- helpers -----------------------------------------------------------------

def _xfail_expected(request) -> bool:
    marker = request.node.get_closest_marker("xfail")
    if marker is None:
        return False
    condition = marker.args[0] if marker.args else marker.kwargs.get("condition", True)
    return bool(condition)


@pytest.fixture
def mock_api(request, caplog):
    """respx router: every request must be mocked and every route must be hit.

    The integration swallows most exceptions and returns ``False``, so a request
    that was not mocked would otherwise go unnoticed in the error-path tests.
    The teardown check turns that into a hard failure.
    """
    strict = not _xfail_expected(request)
    with respx.mock(assert_all_called=strict, assert_all_mocked=True) as router:
        yield router
    if strict:
        leaked = [r.getMessage() for r in caplog.get_records("call") if "not mocked" in r.getMessage()]
        assert not leaked, f"request(s) without a mocked route: {leaked}"


@pytest.fixture
async def make_connected(mock_api):
    """Factory for integrations connected with a plain access token (no HTTP)."""
    created = []

    async def _make(cls=GoogleDriveSource, **config):
        config.setdefault("access_token", ACCESS_TOKEN)
        integration = cls(config)
        assert await integration.connect() is True
        created.append(integration)
        return integration

    yield _make
    for integration in created:
        await integration.disconnect()


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def public_pem(rsa_key) -> bytes:
    return rsa_key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )


@pytest.fixture(scope="module")
def sa_info(rsa_key) -> dict:
    """A service-account key file as downloaded from the Google Cloud console."""
    private_pem = rsa_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")
    return {
        "type": "service_account",
        "project_id": "mark-flow-tests",
        "private_key_id": "0123456789abcdef0123456789abcdef01234567",
        "private_key": private_pem,
        "client_email": SA_EMAIL,
        "client_id": "104857600000000000000",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": TOKEN_URL,
    }


def token_response(token: str = SA_TOKEN) -> dict:
    return {"access_token": token, "expires_in": 3599, "token_type": "Bearer"}


def google_error(status: int) -> dict:
    return {"error": {"code": status, "message": "simulated failure", "status": "FAILED"}}


def form(request: httpx.Request) -> dict[str, str]:
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    parsed = parse_qs(request.content.decode("ascii"), keep_blank_values=True, strict_parsing=True)
    return {key: values[0] for key, values in parsed.items()}


def drive_file(name, file_id=None, *, mime="application/pdf",
               modified="2024-03-10T08:15:30.000Z", size="2048") -> dict:
    item = {"id": file_id or f"id-{name}", "name": name, "mimeType": mime}
    if modified is not None:
        item["modifiedTime"] = modified
    if size is not None:
        item["size"] = size  # Drive returns int64 values as JSON strings
    return item


def drive_folder(name, folder_id) -> dict:
    return {"id": folder_id, "name": name, "mimeType": FOLDER_MIME,
            "modifiedTime": "2024-01-01T00:00:00.000Z"}


def children_q(folder_id: str) -> str:
    return f"'{folder_id}' in parents and trashed = false"


def file_q(name: str, parent_id: str) -> str:
    return f"name = '{name}' and '{parent_id}' in parents and trashed = false"


def folder_q(name: str, parent_id: str) -> str:
    return (f"name = '{name}' and '{parent_id}' in parents "
            f"and mimeType = '{FOLDER_MIME}' and trashed = false")


def mock_children(router, folder_id, items, *, page_token=None, next_page_token=None):
    """Route for one page of a folder listing; matches the *exact* query string."""
    params = {"q": children_q(folder_id), "fields": LIST_FIELDS, "pageSize": "1000"}
    if page_token:
        params["pageToken"] = page_token
    body = {"files": items}
    if next_page_token:
        body["nextPageToken"] = next_page_token
    return router.get(FILES_URL, params__eq=params).respond(200, json=body)


def mock_file_lookup(router, name, parent_id, file_id=None):
    files = [{"id": file_id}] if file_id else []
    return router.get(
        FILES_URL, params__eq={"q": file_q(name, parent_id), "fields": "files(id)"}
    ).respond(200, json={"files": files})


def mock_folder_lookup(router, name, parent_id, folder_id=None):
    files = [{"id": folder_id}] if folder_id else []
    return router.get(
        FILES_URL, params__eq={"q": folder_q(name, parent_id), "fields": "files(id)"}
    ).respond(200, json={"files": files})


def mock_folder_create(router, new_id):
    return router.post(FILES_URL).respond(
        200, json={"kind": "drive#file", "id": new_id, "mimeType": FOLDER_MIME}
    )


def mock_media(router, file_id, content=b"", status=200):
    return router.get(f"{FILES_URL}/{file_id}", params__eq={"alt": "media"}).respond(status, content=content)


def parse_multipart_related(request: httpx.Request) -> list[tuple[dict[str, str], bytes]]:
    """Strictly split a ``multipart/related`` body into (headers, payload) parts."""
    content_type = request.headers["content-type"]
    prefix = "multipart/related; boundary="
    assert content_type.startswith(prefix)
    delimiter = b"--" + content_type[len(prefix):].encode("ascii")
    body = request.content
    assert body.startswith(delimiter + b"\r\n")
    assert body.endswith(b"\r\n" + delimiter + b"--")
    inner = body[len(delimiter) + 2: -(len(delimiter) + 4)]
    parts = []
    for raw in inner.split(b"\r\n" + delimiter + b"\r\n"):
        head, separator, payload = raw.partition(b"\r\n\r\n")
        assert separator, "part without header/body separator"
        headers = dict(line.split(": ", 1) for line in head.decode("ascii").split("\r\n"))
        parts.append((headers, payload))
    return parts


def bearer_tokens(router) -> set:
    return {call.request.headers.get("authorization") for call in router.calls}


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


# --- config model ------------------------------------------------------------

class TestConfig:
    def test_defaults_are_all_none(self):
        config = GoogleDriveConfig.model_validate({})
        assert config.model_dump() == {
            "service_account_json": None,
            "access_token": None,
            "refresh_token": None,
            "client_id": None,
            "client_secret": None,
            "folder_id": None,
        }

    @pytest.mark.parametrize(
        "field, camel",
        [
            ("service_account_json", "serviceAccountJson"),
            ("access_token", "accessToken"),
            ("refresh_token", "refreshToken"),
            ("client_id", "clientId"),
            ("client_secret", "clientSecret"),
            ("folder_id", "folderId"),
        ],
    )
    def test_accepts_camel_case_and_snake_case(self, field, camel):
        assert getattr(GoogleDriveConfig.model_validate({camel: "value"}), field) == "value"
        assert getattr(GoogleDriveConfig.model_validate({field: "value"}), field) == "value"

    def test_camel_case_alias_wins_when_both_are_given(self):
        config = GoogleDriveConfig.model_validate({"folderId": "camel", "folder_id": "snake"})
        assert config.folder_id == "camel"

    def test_unknown_keys_are_ignored(self):
        config = GoogleDriveConfig.model_validate({"accessToken": "t", "type": "google_drive", "x": 1})
        assert config.access_token == "t"
        assert not hasattr(config, "x")

    def test_service_account_json_must_be_a_string(self, sa_info):
        # The field is typed ``str``: an already-parsed JSON object is rejected
        # up front rather than failing later inside connect().
        with pytest.raises(ValidationError) as excinfo:
            GoogleDriveSource({"serviceAccountJson": sa_info})
        assert excinfo.value.errors()[0]["type"] == "string_type"

    def test_non_string_token_is_rejected(self):
        with pytest.raises(ValidationError):
            GoogleDriveConfig.model_validate({"accessToken": 12345})


class TestIdentity:
    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    def test_integration_type(self, cls):
        integration = cls({"accessToken": ACCESS_TOKEN})
        assert integration.integration_type is IntegrationType.GOOGLE_DRIVE
        assert integration.integration_type.value == "google_drive"

    def test_class_hierarchy(self):
        assert issubclass(GoogleDriveSource, SourceIntegration)
        assert not issubclass(GoogleDriveSource, DestinationIntegration)
        assert issubclass(GoogleDriveDestination, DestinationIntegration)

    def test_oauth_scopes(self):
        assert GoogleDriveSource._oauth_scope == "https://www.googleapis.com/auth/drive.readonly"
        assert GoogleDriveDestination._oauth_scope == "https://www.googleapis.com/auth/drive"


# --- connect: service account ------------------------------------------------

class TestServiceAccountAuth:
    async def test_inline_json_signs_a_verifiable_jwt_assertion(self, mock_api, sa_info, public_pem):
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response())
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})
        before = int(datetime.now(timezone.utc).timestamp())

        assert await source.connect() is True

        after = int(datetime.now(timezone.utc).timestamp())
        assert token_route.call_count == 1
        body = form(token_route.calls.last.request)
        assert set(body) == {"grant_type", "assertion"}
        assert body["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"

        # Full validation with the real public key: signature, audience, expiry.
        claims = jwt.decode(body["assertion"], public_pem, algorithms=["RS256"], audience=TOKEN_URL)
        assert claims["iss"] == SA_EMAIL
        assert claims["scope"] == "https://www.googleapis.com/auth/drive.readonly"
        assert claims["aud"] == TOKEN_URL
        assert before <= claims["iat"] <= after
        assert claims["exp"] - claims["iat"] == 3600
        assert set(claims) == {"iss", "scope", "aud", "iat", "exp"}

        header = jwt.get_unverified_header(body["assertion"])
        assert header["alg"] == "RS256"
        assert header["typ"] == "JWT"
        await source.disconnect()

    async def test_assertion_is_bound_to_the_service_account_key(self, mock_api, sa_info):
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response())
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})
        assert await source.connect() is True
        assertion = form(token_route.calls.last.request)["assertion"]

        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()
        with pytest.raises(jwt.InvalidSignatureError):
            jwt.decode(assertion, other_key, algorithms=["RS256"], audience=TOKEN_URL)
        await source.disconnect()

    async def test_claims_use_the_current_time_and_destination_scope(
        self, mock_api, sa_info, public_pem, monkeypatch
    ):
        monkeypatch.setattr(gd, "time", SimpleNamespace(time=lambda: 1_750_000_000.75))
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response())
        destination = GoogleDriveDestination({"service_account_json": json.dumps(sa_info)})

        assert await destination.connect() is True

        assertion = form(token_route.calls.last.request)["assertion"]
        claims = jwt.decode(
            assertion, public_pem, algorithms=["RS256"], audience=TOKEN_URL,
            options={"verify_exp": False, "verify_iat": False},
        )
        assert claims == {
            "iss": SA_EMAIL,
            "scope": "https://www.googleapis.com/auth/drive",
            "aud": TOKEN_URL,
            "iat": 1_750_000_000,
            "exp": 1_750_003_600,
        }
        await destination.disconnect()

    async def test_key_file_path(self, mock_api, sa_info, public_pem, tmp_path):
        key_file = tmp_path / "keys" / "service-account.json"
        key_file.parent.mkdir()
        key_file.write_text(json.dumps(sa_info, indent=2), encoding="utf-8")
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response())
        source = GoogleDriveSource({"serviceAccountJson": str(key_file)})

        assert await source.connect() is True

        claims = jwt.decode(
            form(token_route.calls.last.request)["assertion"],
            public_pem, algorithms=["RS256"], audience=TOKEN_URL,
        )
        assert claims["iss"] == SA_EMAIL
        await source.disconnect()

    async def test_token_is_fetched_once_and_reused_as_bearer(self, mock_api, sa_info, tmp_path):
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response())
        mock_children(mock_api, "root", [drive_file("a.pdf", "file-a")])
        mock_file_lookup(mock_api, "a.pdf", "root", "file-a")
        mock_media(mock_api, "file-a", b"%PDF-1.7")
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})

        assert await source.connect() is True
        files = await source.list_files()
        await source.list_files()
        assert await source.download_file(files[0], tmp_path / "a.pdf") is True

        assert token_route.call_count == 1
        api_calls = [c for c in mock_api.calls if c.request.url.host == "www.googleapis.com"]
        assert len(api_calls) == 4
        assert {c.request.headers["authorization"] for c in api_calls} == {f"Bearer {SA_TOKEN}"}
        # The token request itself is unauthenticated.
        assert "authorization" not in token_route.calls.last.request.headers
        await source.disconnect()

    async def test_missing_key_file(self, mock_api, tmp_path):
        source = GoogleDriveSource({"serviceAccountJson": str(tmp_path / "nope.json")})
        assert await source.connect() is False
        assert mock_api.calls.call_count == 0
        await source.disconnect()

    @pytest.mark.parametrize(
        "raw",
        ['{"client_email": ', "{}", '{"client_email": "a@b.iam.gserviceaccount.com"}'],
        ids=["truncated-json", "empty-object", "no-private-key"],
    )
    async def test_unusable_inline_json(self, mock_api, raw):
        source = GoogleDriveSource({"serviceAccountJson": raw})
        assert await source.connect() is False
        assert mock_api.calls.call_count == 0
        await source.disconnect()

    async def test_invalid_private_key(self, mock_api, sa_info):
        broken = {**sa_info, "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----\n"}
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(broken)})
        assert await source.connect() is False
        assert mock_api.calls.call_count == 0
        await source.disconnect()

    @pytest.mark.parametrize("status", [400, 401, 403, 429, 500])
    async def test_token_endpoint_rejects(self, mock_api, sa_info, status, caplog):
        mock_api.post(TOKEN_URL).respond(
            status, json={"error": "invalid_grant", "error_description": "Invalid JWT Signature."}
        )
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})

        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await source.connect() is False

        assert "invalid_grant" in caplog.text
        with pytest.raises(RuntimeError, match="Not connected"):
            await source.list_files()
        await source.disconnect()

    async def test_token_response_without_access_token(self, mock_api, sa_info):
        mock_api.post(TOKEN_URL).respond(200, json={"token_type": "Bearer"})
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})
        assert await source.connect() is False
        await source.disconnect()

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_token_endpoint_unreachable(self, mock_api, sa_info, error):
        mock_api.post(TOKEN_URL).mock(side_effect=error("simulated"))
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info)})
        assert await source.connect() is False
        await source.disconnect()


# --- connect: OAuth tokens ---------------------------------------------------

class TestOAuthTokenAuth:
    async def test_plain_access_token_needs_no_request(self, mock_api):
        mock_children(mock_api, "root", [])
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN})

        assert await source.connect() is True
        assert mock_api.calls.call_count == 0

        await source.list_files()
        assert bearer_tokens(mock_api) == {f"Bearer {ACCESS_TOKEN}"}
        await source.disconnect()

    async def test_refresh_token_flow_replaces_the_access_token(self, mock_api):
        token_route = mock_api.post(TOKEN_URL).respond(
            200, json={**token_response("ya29.refreshed"), "scope": "https://www.googleapis.com/auth/drive"}
        )
        list_route = mock_children(mock_api, "root", [])
        source = GoogleDriveSource({
            "accessToken": "ya29.stale",
            "refreshToken": "1//refresh-token",
            "clientId": "client-id.apps.googleusercontent.com",
            "clientSecret": "GOCSPX-secret",
        })

        assert await source.connect() is True
        await source.list_files()

        assert token_route.call_count == 1
        assert form(token_route.calls.last.request) == {
            "grant_type": "refresh_token",
            "refresh_token": "1//refresh-token",
            "client_id": "client-id.apps.googleusercontent.com",
            "client_secret": "GOCSPX-secret",
        }
        assert list_route.calls.last.request.headers["authorization"] == "Bearer ya29.refreshed"
        await source.disconnect()

    async def test_refresh_without_client_secret_sends_an_empty_one(self, mock_api):
        token_route = mock_api.post(TOKEN_URL).respond(200, json=token_response("ya29.refreshed"))
        source = GoogleDriveSource({"accessToken": "ya29.stale", "refreshToken": "r", "clientId": "cid"})

        assert await source.connect() is True

        assert form(token_route.calls.last.request)["client_secret"] == ""
        await source.disconnect()

    async def test_refresh_is_skipped_without_client_id(self, mock_api):
        list_route = mock_children(mock_api, "root", [])
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN, "refreshToken": "r"})

        assert await source.connect() is True
        await source.list_files()

        assert mock_api.calls.call_count == 1  # only the listing, no token request
        assert list_route.calls.last.request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        await source.disconnect()

    @pytest.mark.parametrize("status", [400, 401, 500])
    async def test_failed_refresh_keeps_the_configured_access_token(self, mock_api, status):
        mock_api.post(TOKEN_URL).respond(status, json={"error": "invalid_grant"})
        list_route = mock_children(mock_api, "root", [])
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN, "refreshToken": "r", "clientId": "cid"})

        assert await source.connect() is True
        await source.list_files()

        assert list_route.calls.last.request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        await source.disconnect()

    async def test_refresh_response_without_access_token_fails_connect(self, mock_api):
        mock_api.post(TOKEN_URL).respond(200, json={"token_type": "Bearer"})
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN, "refreshToken": "r", "clientId": "cid"})
        assert await source.connect() is False
        await source.disconnect()

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_refresh_transport_error_fails_connect(self, mock_api, error):
        mock_api.post(TOKEN_URL).mock(side_effect=error("simulated"))
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN, "refreshToken": "r", "clientId": "cid"})
        assert await source.connect() is False
        await source.disconnect()

    async def test_refresh_helper_is_a_noop_without_refresh_token(self, make_connected, mock_api):
        source = await make_connected()
        assert await source._refresh_token() is False
        assert mock_api.calls.call_count == 0

    async def test_no_credentials(self, mock_api, caplog):
        source = GoogleDriveSource({"folderId": "some-folder"})
        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await source.connect() is False
        assert "No credentials provided" in caplog.text
        await source.disconnect()

    async def test_service_account_takes_precedence_over_access_token(self, mock_api, sa_info):
        mock_api.post(TOKEN_URL).respond(200, json=token_response())
        list_route = mock_children(mock_api, "root", [])
        source = GoogleDriveSource({"serviceAccountJson": json.dumps(sa_info), "accessToken": ACCESS_TOKEN})

        assert await source.connect() is True
        await source.list_files()

        assert list_route.calls.last.request.headers["authorization"] == f"Bearer {SA_TOKEN}"
        await source.disconnect()


# --- connection lifecycle ----------------------------------------------------

class TestLifecycle:
    async def test_client_uses_a_120s_timeout(self, make_connected):
        source = await make_connected()
        assert source._client.timeout == httpx.Timeout(120.0)

    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    async def test_disconnect_without_connect(self, cls):
        integration = cls({"accessToken": ACCESS_TOKEN})
        await integration.disconnect()
        await integration.disconnect()

    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    async def test_disconnect_closes_the_client_and_forgets_the_token(self, cls, tmp_path, file_info):
        integration = cls({"accessToken": ACCESS_TOKEN})
        assert await integration.connect() is True
        client = integration._client

        await integration.disconnect()

        assert client.is_closed
        with pytest.raises(RuntimeError, match="Not connected"):
            await integration.list_files()
        with pytest.raises(RuntimeError, match="Not connected"):
            await integration.download_file(file_info("/a.pdf"), tmp_path / "a.pdf")
        await integration.disconnect()  # idempotent

    async def test_reconnect_after_disconnect(self, mock_api):
        mock_children(mock_api, "root", [drive_file("a.pdf")])
        source = GoogleDriveSource({"accessToken": ACCESS_TOKEN})
        assert await source.connect() is True
        await source.disconnect()
        assert await source.connect() is True
        assert [f.name for f in await source.list_files()] == ["a.pdf"]
        await source.disconnect()

    async def test_async_context_manager(self, mock_api):
        mock_children(mock_api, "root", [])
        async with GoogleDriveSource({"accessToken": ACCESS_TOKEN}) as source:
            client = source._client
            assert await source.list_files() == []
        assert client.is_closed

    async def test_async_context_manager_raises_when_connect_fails(self):
        with pytest.raises(RuntimeError, match="Failed to connect to google_drive"):
            async with GoogleDriveSource({}):
                pytest.fail("body must not run")  # pragma: no cover

    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    async def test_operations_require_connect(self, cls, tmp_path, file_info):
        integration = cls({"accessToken": ACCESS_TOKEN})
        with pytest.raises(RuntimeError, match="Not connected"):
            await integration.list_files()
        with pytest.raises(RuntimeError, match="Not connected"):
            await integration.download_file(file_info("/a.pdf"), tmp_path / "a.pdf")

    async def test_upload_requires_connect(self, tmp_path):
        local = tmp_path / "a.md"
        local.write_text("# a", encoding="utf-8")
        destination = GoogleDriveDestination({"accessToken": ACCESS_TOKEN})
        with pytest.raises(RuntimeError, match="Not connected"):
            await destination.upload_file(local, "a.md")

    async def test_create_folder_fails_when_not_connected(self):
        destination = GoogleDriveDestination({"accessToken": ACCESS_TOKEN})
        assert await destination.create_folder("/docs") is False


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_lists_root_folder_with_expected_request(self, make_connected, mock_api):
        route = mock_children(mock_api, "root", [
            drive_file("report.pdf", "file-1", size="2048", modified="2024-03-10T08:15:30.000Z"),
        ])
        source = await make_connected()

        files = await source.list_files()

        request = route.calls.last.request
        assert request.method == "GET"
        assert request.url.copy_with(query=None) == httpx.URL(FILES_URL)
        assert dict(request.url.params) == {
            "q": "'root' in parents and trashed = false",
            "fields": LIST_FIELDS,
            "pageSize": "1000",
        }
        assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"

        assert len(files) == 1
        info = files[0]
        assert info.name == "report.pdf"
        assert info.path == "/report.pdf"
        assert info.modified_at == datetime(2024, 3, 10, 8, 15, 30, tzinfo=timezone.utc)
        assert info.size == 2048
        assert info.content_type == "application/pdf"

    async def test_configured_folder_id_is_the_listing_root(self, make_connected, mock_api):
        route = mock_children(mock_api, "1AbCdEfGhIjK", [drive_file("a.docx")])
        source = await make_connected(folderId="1AbCdEfGhIjK")

        assert [f.path for f in await source.list_files()] == ["/a.docx"]
        assert route.call_count == 1

    async def test_recurses_into_subfolders(self, make_connected, mock_api):
        mock_children(mock_api, "root", [
            drive_file("top.pdf"),
            drive_folder("docs", "folder-docs"),
            drive_folder("empty", "folder-empty"),
        ])
        mock_children(mock_api, "folder-docs", [
            drive_folder("2024", "folder-2024"),
            drive_file("guide.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ])
        mock_children(mock_api, "folder-2024", [drive_file("q1 report.xlsx")])
        mock_children(mock_api, "folder-empty", [])
        source = await make_connected()

        files = await source.list_files()

        assert [f.path for f in files] == [
            "/top.pdf",
            "/docs/2024/q1 report.xlsx",
            "/docs/guide.docx",
        ]
        assert [f.name for f in files] == ["top.pdf", "q1 report.xlsx", "guide.docx"]
        assert all("\\" not in f.path for f in files)
        assert bearer_tokens(mock_api) == {f"Bearer {ACCESS_TOKEN}"}

    async def test_follows_next_page_token(self, make_connected, mock_api):
        page1 = mock_children(mock_api, "root", [drive_file("a.pdf")], next_page_token="tok-2")
        page2 = mock_children(mock_api, "root", [drive_file("b.pdf")], page_token="tok-2", next_page_token="tok-3")
        page3 = mock_children(mock_api, "root", [drive_file("c.pdf")], page_token="tok-3")
        source = await make_connected()

        files = await source.list_files()

        assert [f.name for f in files] == ["a.pdf", "b.pdf", "c.pdf"]
        assert (page1.call_count, page2.call_count, page3.call_count) == (1, 1, 1)
        assert "pageToken" not in page1.calls.last.request.url.params
        assert page3.calls.last.request.url.params["pageToken"] == "tok-3"

    async def test_extension_filter_is_case_insensitive(self, make_connected, mock_api):
        mock_children(mock_api, "root", [
            drive_file("a.pdf"), drive_file("B.PDF"), drive_file("c.docx"),
            drive_file("d.txt"), drive_file("no-extension"),
        ])
        source = await make_connected()

        files = await source.list_files(extensions=[".PDF", ".docx"])

        assert [f.name for f in files] == ["a.pdf", "B.PDF", "c.docx"]

    async def test_extension_filter_applies_inside_subfolders(self, make_connected, mock_api):
        mock_children(mock_api, "root", [drive_folder("docs.pdf", "folder-1"), drive_file("x.txt")])
        mock_children(mock_api, "folder-1", [drive_file("in.pdf"), drive_file("in.txt")])
        source = await make_connected()

        files = await source.list_files(extensions=[".pdf"])

        # A folder is traversed whatever its name looks like, and never listed itself.
        assert [f.path for f in files] == ["/docs.pdf/in.pdf"]

    async def test_size_and_content_type_mapping(self, make_connected, mock_api):
        mock_children(mock_api, "root", [
            drive_file("big.bin", size="5368709120", mime="application/octet-stream"),
            drive_file("empty.txt", size="0", mime="text/plain"),
            # Google-native documents have no ``size``.
            drive_file("Notes", size=None, mime="application/vnd.google-apps.document"),
        ])
        source = await make_connected()

        files = {f.name: f for f in await source.list_files()}

        assert files["big.bin"].size == 5368709120
        assert files["empty.txt"].size == 0
        assert files["Notes"].size is None
        assert files["Notes"].content_type == "application/vnd.google-apps.document"

    @pytest.mark.parametrize(
        "modified, expected",
        [
            ("2024-03-10T08:15:30.000Z", datetime(2024, 3, 10, 8, 15, 30, tzinfo=timezone.utc)),
            ("2024-03-10T08:15:30.123Z", datetime(2024, 3, 10, 8, 15, 30, 123000, tzinfo=timezone.utc)),
            ("2024-03-10T08:15:30Z", datetime(2024, 3, 10, 8, 15, 30, tzinfo=timezone.utc)),
            ("2024-03-10T10:15:30+02:00", datetime(2024, 3, 10, 8, 15, 30, tzinfo=timezone.utc)),
        ],
    )
    async def test_modified_time_parsing(self, make_connected, mock_api, modified, expected):
        mock_children(mock_api, "root", [drive_file("a.pdf", modified=modified)])
        source = await make_connected()

        info = (await source.list_files())[0]

        assert info.modified_at == expected
        assert info.modified_at.utcoffset() is not None

    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    @pytest.mark.parametrize("modified", [None, "", "yesterday"])
    async def test_unparseable_modified_time_falls_back_to_now(self, make_connected, mock_api, cls, modified):
        mock_children(mock_api, "root", [drive_file("a.pdf", modified=modified)])
        integration = await make_connected(cls)
        before = datetime.now(timezone.utc) - timedelta(seconds=5)

        info = (await integration.list_files())[0]

        after = datetime.now(timezone.utc) + timedelta(seconds=5)
        assert before <= as_utc(info.modified_at) <= after

    @pytest.mark.parametrize("cls", [GoogleDriveSource, GoogleDriveDestination])
    @pytest.mark.parametrize("status", HTTP_ERRORS)
    async def test_http_error_yields_no_files_and_a_warning(self, make_connected, mock_api, caplog, cls, status):
        route = mock_api.get(FILES_URL).respond(status, json=google_error(status))
        integration = await make_connected(cls)

        with caplog.at_level(logging.WARNING, logger="src.integration.google_drive"):
            files = await integration.list_files()

        assert files == []
        assert route.call_count == 1  # no retry loop
        assert f"Failed to list folder: {status}" in caplog.text

    async def test_http_error_on_a_later_page_keeps_earlier_pages(self, make_connected, mock_api, caplog):
        mock_children(mock_api, "root", [drive_file("a.pdf")], next_page_token="tok-2")
        failing = mock_api.get(FILES_URL, params={"pageToken": "tok-2"}).respond(500, json=google_error(500))
        source = await make_connected()

        with caplog.at_level(logging.WARNING, logger="src.integration.google_drive"):
            files = await source.list_files()

        assert [f.name for f in files] == ["a.pdf"]
        assert failing.call_count == 1
        assert "Failed to list folder: 500" in caplog.text

    async def test_http_error_in_a_subfolder_keeps_the_rest(self, make_connected, mock_api):
        mock_children(mock_api, "root", [
            drive_folder("private", "folder-private"), drive_file("public.pdf"),
        ])
        mock_api.get(FILES_URL, params={"q": children_q("folder-private")}).respond(403, json=google_error(403))
        source = await make_connected()

        assert [f.path for f in await source.list_files()] == ["/public.pdf"]

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_propagates(self, make_connected, mock_api, error):
        mock_api.get(FILES_URL).mock(side_effect=error("simulated"))
        source = await make_connected()

        with pytest.raises(error):
            await source.list_files()

    async def test_destination_lists_like_the_source(self, make_connected, mock_api):
        mock_children(mock_api, "root", [
            drive_folder("out", "folder-out"), drive_file("README.md", mime="text/markdown"),
        ])
        mock_children(mock_api, "folder-out", [
            drive_file("a.md", mime="text/markdown", size="12"), drive_file("a.pdf"),
        ])
        destination = await make_connected(GoogleDriveDestination)

        files = await destination.list_files(extensions=[".md"])

        assert [(f.path, f.size, f.content_type) for f in files] == [
            ("/out/a.md", 12, "text/markdown"),
            ("/README.md", 2048, "text/markdown"),
        ]
        assert files[0].modified_at == datetime(2024, 3, 10, 8, 15, 30, tzinfo=timezone.utc)

    async def test_destination_listing_primes_the_folder_cache(self, make_connected, mock_api):
        mock_children(mock_api, "root", [drive_folder("out", "folder-out")])
        mock_children(mock_api, "folder-out", [drive_folder("2024", "folder-2024")])
        mock_children(mock_api, "folder-2024", [])
        destination = await make_connected(GoogleDriveDestination)
        await destination.list_files()
        calls_after_listing = mock_api.calls.call_count

        # Folders seen while listing are not looked up (or created) again.
        assert await destination.create_folder("/out/2024") is True
        assert mock_api.calls.call_count == calls_after_listing

    async def test_disconnect_clears_the_destination_folder_cache(self, mock_api):
        mock_children(mock_api, "root", [drive_folder("out", "folder-out")])
        mock_children(mock_api, "folder-out", [])
        lookup = mock_folder_lookup(mock_api, "out", "root", "folder-out")
        destination = GoogleDriveDestination({"accessToken": ACCESS_TOKEN})
        assert await destination.connect() is True
        await destination.list_files()
        await destination.disconnect()

        assert await destination.connect() is True
        assert await destination.create_folder("/out") is True

        assert lookup.call_count == 1  # cache was dropped, so the folder is resolved again
        await destination.disconnect()


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    async def test_downloads_a_root_level_file(self, make_connected, mock_api, file_info, tmp_path):
        payload = b"%PDF-1.7\n\x00\xff\xfe binary \r\n payload"
        lookup = mock_file_lookup(mock_api, "report.pdf", "root", "file-1")
        media = mock_media(mock_api, "file-1", payload)
        source = await make_connected()
        local = tmp_path / "nested" / "dir" / "report.pdf"

        assert await source.download_file(file_info("/report.pdf"), local) is True

        assert local.read_bytes() == payload
        assert dict(lookup.calls.last.request.url.params) == {
            "q": "name = 'report.pdf' and 'root' in parents and trashed = false",
            "fields": "files(id)",
        }
        request = media.calls.last.request
        assert request.method == "GET"
        assert str(request.url) == f"{FILES_URL}/file-1?alt=media"
        assert bearer_tokens(mock_api) == {f"Bearer {ACCESS_TOKEN}"}

    async def test_resolves_nested_paths_folder_by_folder(self, make_connected, mock_api, file_info, tmp_path):
        docs = mock_folder_lookup(mock_api, "docs", "root", "folder-docs")
        year = mock_folder_lookup(mock_api, "2024", "folder-docs", "folder-2024")
        lookup = mock_file_lookup(mock_api, "q1 report.pdf", "folder-2024", "file-9")
        mock_media(mock_api, "file-9", b"data")
        source = await make_connected()
        local = tmp_path / "q1.pdf"

        assert await source.download_file(file_info("/docs/2024/q1 report.pdf"), local) is True

        assert local.read_bytes() == b"data"
        assert [c.request.url.params.get("q") for c in mock_api.calls][:3] == [
            folder_q("docs", "root"),
            folder_q("2024", "folder-docs"),
            file_q("q1 report.pdf", "folder-2024"),
        ]
        assert (docs.call_count, year.call_count, lookup.call_count) == (1, 1, 1)

    async def test_configured_folder_id_is_the_lookup_root(self, make_connected, mock_api, file_info, tmp_path):
        mock_file_lookup(mock_api, "a.pdf", "1AbCdEfGhIjK", "file-1")
        mock_media(mock_api, "file-1", b"x")
        source = await make_connected(folder_id="1AbCdEfGhIjK")

        assert await source.download_file(file_info("/a.pdf"), tmp_path / "a.pdf") is True

    async def test_destination_can_download_too(self, make_connected, mock_api, file_info, tmp_path):
        mock_file_lookup(mock_api, "a.md", "root", "file-1")
        mock_media(mock_api, "file-1", b"# title\n")
        destination = await make_connected(GoogleDriveDestination)
        local = tmp_path / "a.md"

        assert await destination.download_file(file_info("/a.md"), local) is True
        assert local.read_bytes() == b"# title\n"

    async def test_overwrites_an_existing_local_file(self, make_connected, mock_api, file_info, tmp_path):
        mock_file_lookup(mock_api, "a.pdf", "root", "file-1")
        mock_media(mock_api, "file-1", b"new")
        source = await make_connected()
        local = tmp_path / "a.pdf"
        local.write_bytes(b"old content that is longer")

        assert await source.download_file(file_info("/a.pdf"), local) is True
        assert local.read_bytes() == b"new"

    async def test_unknown_folder(self, make_connected, mock_api, file_info, tmp_path, caplog):
        mock_folder_lookup(mock_api, "docs", "root", None)
        source = await make_connected()
        local = tmp_path / "a.pdf"

        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await source.download_file(file_info("/docs/a.pdf"), local) is False

        assert not local.exists()
        assert "Could not resolve file ID for /docs/a.pdf" in caplog.text
        assert mock_api.calls.call_count == 1

    async def test_unknown_file(self, make_connected, mock_api, file_info, tmp_path):
        mock_file_lookup(mock_api, "a.pdf", "root", None)
        source = await make_connected()
        local = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/a.pdf"), local) is False
        assert not local.exists()
        assert mock_api.calls.call_count == 1

    async def test_empty_path_resolves_to_nothing(self, make_connected, mock_api, file_info, tmp_path):
        source = await make_connected()
        assert await source.download_file(file_info("/", name="x"), tmp_path / "x") is False
        assert mock_api.calls.call_count == 0

    @pytest.mark.parametrize("status", HTTP_ERRORS)
    async def test_lookup_http_error(self, make_connected, mock_api, file_info, tmp_path, status):
        lookup = mock_api.get(FILES_URL).respond(status, json=google_error(status))
        source = await make_connected()
        local = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/docs/a.pdf"), local) is False
        assert not local.exists()
        assert lookup.call_count == 1

    @pytest.mark.parametrize("status", HTTP_ERRORS)
    async def test_media_http_error(self, make_connected, mock_api, file_info, tmp_path, caplog, status):
        mock_file_lookup(mock_api, "a.pdf", "root", "file-1")
        media = mock_api.get(f"{FILES_URL}/file-1").respond(status, json=google_error(status))
        source = await make_connected()
        local = tmp_path / "a.pdf"

        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await source.download_file(file_info("/a.pdf"), local) is False

        assert not local.exists()
        assert media.call_count == 1
        assert f"Download failed for /a.pdf: {status}" in caplog.text

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_during_lookup(self, make_connected, mock_api, file_info, tmp_path, error):
        lookup = mock_api.get(FILES_URL).mock(side_effect=error("simulated"))
        source = await make_connected()
        local = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/a.pdf"), local) is False
        assert not local.exists()
        assert lookup.call_count == 1

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_during_media_download(self, make_connected, mock_api, file_info, tmp_path, error):
        mock_file_lookup(mock_api, "a.pdf", "root", "file-1")
        media = mock_api.get(f"{FILES_URL}/file-1").mock(side_effect=error("simulated"))
        source = await make_connected()
        local = tmp_path / "a.pdf"

        assert await source.download_file(file_info("/a.pdf"), local) is False
        assert not local.exists()
        assert media.call_count == 1

    async def test_local_write_failure(self, make_connected, mock_api, file_info, tmp_path):
        mock_file_lookup(mock_api, "a.pdf", "root", "file-1")
        mock_media(mock_api, "file-1", b"x")
        source = await make_connected()
        blocker = tmp_path / "blocker"
        blocker.write_text("i am a file, not a directory", encoding="utf-8")

        assert await source.download_file(file_info("/a.pdf"), blocker / "a.pdf") is False

    async def test_download_files_preserves_folder_structure(self, make_connected, mock_api, file_info, tmp_path):
        mock_folder_lookup(mock_api, "docs", "root", "folder-docs")
        mock_file_lookup(mock_api, "a.pdf", "folder-docs", "file-a")
        mock_file_lookup(mock_api, "missing.pdf", "root", None)
        mock_media(mock_api, "file-a", b"A")
        source = await make_connected()

        result = await source.download_files(
            [file_info("/docs/a.pdf"), file_info("/missing.pdf")], tmp_path
        )

        assert result == {"/docs/a.pdf": tmp_path / "docs" / "a.pdf"}
        assert (tmp_path / "docs" / "a.pdf").read_bytes() == b"A"

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: google_drive.py:242/260 interpolate names into the Drive query without "
        "escaping apostrophes, so files/folders named like O'Brien can never be resolved",
    )
    async def test_names_with_apostrophes_are_escaped_in_the_query(
        self, make_connected, mock_api, file_info, tmp_path
    ):
        lookup = mock_api.get(FILES_URL).respond(200, json={"files": []})
        source = await make_connected()

        await source.download_file(file_info("/O'Brien contract.pdf"), tmp_path / "c.pdf")

        # https://developers.google.com/drive/api/guides/search-files: escape ' as \'
        assert lookup.calls.last.request.url.params["q"] == (
            r"name = 'O\'Brien contract.pdf' and 'root' in parents and trashed = false"
        )


# --- upload_file -------------------------------------------------------------

@pytest.fixture
def local_md(tmp_path) -> Path:
    path = tmp_path / "local" / "out.md"
    path.parent.mkdir()
    path.write_bytes("# Résumé\r\n\r\nbody — with unicode ✓\n".encode("utf-8"))
    return path


def assert_multipart_create(request: httpx.Request, *, name: str, parent_id: str, content: bytes) -> None:
    assert request.method == "POST"
    assert str(request.url) == f"{UPLOAD_URL}?uploadType=multipart"
    assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert request.headers["content-type"] == "multipart/related; boundary====multipart_boundary==="
    assert int(request.headers["content-length"]) == len(request.content)
    metadata_part, media_part = parse_multipart_related(request)
    assert metadata_part[0] == {"Content-Type": "application/json; charset=UTF-8"}
    assert json.loads(metadata_part[1]) == {"name": name, "parents": [parent_id]}
    assert media_part[0] == {"Content-Type": "application/octet-stream"}
    assert media_part[1] == content


class TestUploadFile:
    async def test_creates_a_new_file_in_the_root_folder(self, make_connected, mock_api, local_md):
        lookup = mock_file_lookup(mock_api, "out.md", "root", None)
        create = mock_api.post(f"{UPLOAD_URL}", params__eq={"uploadType": "multipart"}).respond(
            200, json={"kind": "drive#file", "id": "new-1", "name": "out.md", "mimeType": "text/markdown"}
        )
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is True

        assert lookup.call_count == 1
        assert create.call_count == 1
        assert_multipart_create(
            create.calls.last.request, name="out.md", parent_id="root", content=local_md.read_bytes()
        )

    async def test_new_file_goes_into_the_configured_folder(self, make_connected, mock_api, local_md):
        mock_file_lookup(mock_api, "out.md", "1AbCdEfGhIjK", None)
        create = mock_api.post(UPLOAD_URL).respond(201, json={"id": "new-1"})
        destination = await make_connected(GoogleDriveDestination, folderId="1AbCdEfGhIjK")

        assert await destination.upload_file(local_md, "out.md") is True

        assert_multipart_create(
            create.calls.last.request, name="out.md", parent_id="1AbCdEfGhIjK", content=local_md.read_bytes()
        )

    async def test_binary_content_survives_the_multipart_body(self, make_connected, mock_api, tmp_path):
        content = bytes(range(256)) * 8 + b"\r\n--===multipart_boundary"
        local = tmp_path / "blob.bin"
        local.write_bytes(content)
        mock_file_lookup(mock_api, "blob.bin", "root", None)
        create = mock_api.post(UPLOAD_URL).respond(200, json={"id": "new-1"})
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local, "blob.bin") is True

        assert_multipart_create(create.calls.last.request, name="blob.bin", parent_id="root", content=content)

    async def test_existing_file_is_updated_in_place(self, make_connected, mock_api, local_md):
        mock_file_lookup(mock_api, "out.md", "root", "existing-7")
        update = mock_api.patch(f"{UPLOAD_URL}/existing-7", params__eq={"uploadType": "media"}).respond(
            200, json={"kind": "drive#file", "id": "existing-7", "name": "out.md"}
        )
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is True

        request = update.calls.last.request
        assert request.method == "PATCH"
        assert str(request.url) == f"{UPLOAD_URL}/existing-7?uploadType=media"
        assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert request.headers["content-type"] == "application/octet-stream"
        assert request.content == local_md.read_bytes()
        assert mock_api.calls.call_count == 2  # lookup + update, nothing created

    @pytest.mark.parametrize("status", [400, *HTTP_ERRORS])
    async def test_create_http_error(self, make_connected, mock_api, local_md, status):
        mock_file_lookup(mock_api, "out.md", "root", None)
        create = mock_api.post(UPLOAD_URL).respond(status, json=google_error(status))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is False
        assert create.call_count == 1

    @pytest.mark.parametrize("status", [201, 400, *HTTP_ERRORS])
    async def test_update_must_return_200(self, make_connected, mock_api, local_md, status):
        mock_file_lookup(mock_api, "out.md", "root", "existing-7")
        update = mock_api.patch(f"{UPLOAD_URL}/existing-7").respond(status, json=google_error(status))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is False
        assert update.call_count == 1

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_during_lookup(self, make_connected, mock_api, local_md, error):
        lookup = mock_api.get(FILES_URL).mock(side_effect=error("simulated"))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is False
        assert lookup.call_count == 1

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_during_create(self, make_connected, mock_api, local_md, error):
        mock_file_lookup(mock_api, "out.md", "root", None)
        create = mock_api.post(UPLOAD_URL).mock(side_effect=error("simulated"))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is False
        assert create.call_count == 1

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error_during_update(self, make_connected, mock_api, local_md, error):
        mock_file_lookup(mock_api, "out.md", "root", "existing-7")
        update = mock_api.patch(f"{UPLOAD_URL}/existing-7").mock(side_effect=error("simulated"))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "out.md") is False
        assert update.call_count == 1

    async def test_missing_local_file(self, make_connected, mock_api, tmp_path):
        mock_file_lookup(mock_api, "out.md", "root", None)
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(tmp_path / "does-not-exist.md", "out.md") is False
        assert mock_api.calls.call_count == 1  # nothing was created or updated

    @windows_remote_path_bug
    async def test_leading_slash_at_root_level_uploads_to_the_root_folder(self, make_connected, mock_api, local_md):
        mock_file_lookup(mock_api, "out.md", "root", None)
        create = mock_api.post(UPLOAD_URL).respond(200, json={"id": "new-1"})
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "/out.md") is True

        assert mock_api.calls.call_count == 2
        assert_multipart_create(
            create.calls.last.request, name="out.md", parent_id="root", content=local_md.read_bytes()
        )

    @windows_remote_path_bug
    async def test_nested_path_reuses_existing_and_creates_missing_folders(
        self, make_connected, mock_api, local_md
    ):
        docs = mock_folder_lookup(mock_api, "docs", "root", "folder-docs")
        sub = mock_folder_lookup(mock_api, "sub", "folder-docs", None)
        mkdir = mock_folder_create(mock_api, "folder-sub")
        lookup = mock_file_lookup(mock_api, "out.md", "folder-sub", None)
        create = mock_api.post(UPLOAD_URL).respond(200, json={"id": "new-1"})
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "/docs/sub/out.md") is True

        assert (docs.call_count, sub.call_count, mkdir.call_count, lookup.call_count) == (1, 1, 1, 1)
        mkdir_request = mkdir.calls.last.request
        assert mkdir_request.headers["content-type"] == "application/json"
        assert mkdir_request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert json.loads(mkdir_request.content) == {
            "name": "sub", "mimeType": FOLDER_MIME, "parents": ["folder-docs"],
        }
        assert_multipart_create(
            create.calls.last.request, name="out.md", parent_id="folder-sub", content=local_md.read_bytes()
        )

    @windows_remote_path_bug
    async def test_second_upload_to_the_same_folder_uses_the_folder_cache(
        self, make_connected, mock_api, local_md
    ):
        docs = mock_folder_lookup(mock_api, "docs", "root", "folder-docs")
        mock_file_lookup(mock_api, "a.md", "folder-docs", None)
        mock_file_lookup(mock_api, "b.md", "folder-docs", "existing-b")
        create = mock_api.post(UPLOAD_URL).respond(200, json={"id": "new-a"})
        update = mock_api.patch(f"{UPLOAD_URL}/existing-b").respond(200, json={"id": "existing-b"})
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.upload_file(local_md, "docs/a.md") is True
        assert await destination.upload_file(local_md, "/docs/b.md") is True

        assert docs.call_count == 1
        assert (create.call_count, update.call_count) == (1, 1)

    @windows_remote_path_bug
    async def test_folder_creation_failure_aborts_the_upload(self, make_connected, mock_api, local_md, caplog):
        mock_folder_lookup(mock_api, "docs", "root", None)
        mkdir = mock_api.post(FILES_URL).respond(403, json=google_error(403))
        destination = await make_connected(GoogleDriveDestination)

        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await destination.upload_file(local_md, "/docs/out.md") is False

        assert json.loads(mkdir.calls.last.request.content)["name"] == "docs"
        assert "Failed to create folder docs" in caplog.text
        assert mock_api.calls.call_count == 2  # no file lookup, no upload


# --- create_folder -----------------------------------------------------------

class TestCreateFolder:
    async def test_creates_every_missing_level(self, make_connected, mock_api):
        lookup_a = mock_folder_lookup(mock_api, "a", "root", None)
        lookup_b = mock_folder_lookup(mock_api, "b", "new-a", None)
        mkdir = mock_api.post(FILES_URL).mock(side_effect=[
            httpx.Response(200, json={"id": "new-a"}),
            httpx.Response(201, json={"id": "new-b"}),
        ])
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.create_folder("/a/b") is True

        assert (lookup_a.call_count, lookup_b.call_count, mkdir.call_count) == (1, 1, 2)
        bodies = [json.loads(call.request.content) for call in mkdir.calls]
        assert bodies == [
            {"name": "a", "mimeType": FOLDER_MIME, "parents": ["root"]},
            {"name": "b", "mimeType": FOLDER_MIME, "parents": ["new-a"]},
        ]
        for call in mkdir.calls:
            assert call.request.method == "POST"
            assert str(call.request.url) == FILES_URL
            assert call.request.headers["content-type"] == "application/json"
            assert call.request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"

    async def test_existing_folders_are_not_recreated(self, make_connected, mock_api):
        mock_folder_lookup(mock_api, "a", "root", "folder-a")
        mock_folder_lookup(mock_api, "b", "folder-a", "folder-b")
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.create_folder("/a/b") is True
        assert {call.request.method for call in mock_api.calls} == {"GET"}

    async def test_configured_folder_id_is_the_parent_of_the_first_level(self, make_connected, mock_api):
        mock_folder_lookup(mock_api, "a", "1AbCdEfGhIjK", None)
        mkdir = mock_folder_create(mock_api, "new-a")
        destination = await make_connected(GoogleDriveDestination, folderId="1AbCdEfGhIjK")

        assert await destination.create_folder("a") is True
        assert json.loads(mkdir.calls.last.request.content)["parents"] == ["1AbCdEfGhIjK"]

    async def test_repeated_calls_are_served_from_the_cache(self, make_connected, mock_api):
        lookup = mock_folder_lookup(mock_api, "a", "root", None)
        mkdir = mock_folder_create(mock_api, "new-a")
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.create_folder("/a") is True
        assert await destination.create_folder("/a") is True
        assert await destination.create_folder("a/") is True  # same folder, different spelling

        assert (lookup.call_count, mkdir.call_count) == (1, 1)

    async def test_cached_parent_is_reused_for_a_deeper_path(self, make_connected, mock_api):
        mock_folder_lookup(mock_api, "a", "root", "folder-a")
        lookup_b = mock_folder_lookup(mock_api, "b", "folder-a", None)
        mkdir = mock_folder_create(mock_api, "new-b")
        destination = await make_connected(GoogleDriveDestination)
        assert await destination.create_folder("/a") is True

        assert await destination.create_folder("/a/b") is True

        assert mock_api.calls.call_count == 3  # "a" lookup, then only "b" lookup + create
        assert lookup_b.call_count == 1
        assert json.loads(mkdir.calls.last.request.content)["parents"] == ["folder-a"]

    @pytest.mark.parametrize("status", [400, *HTTP_ERRORS])
    async def test_create_http_error(self, make_connected, mock_api, caplog, status):
        mock_folder_lookup(mock_api, "a", "root", None)
        mkdir = mock_api.post(FILES_URL).respond(status, json=google_error(status))
        destination = await make_connected(GoogleDriveDestination)

        with caplog.at_level(logging.ERROR, logger="src.integration.google_drive"):
            assert await destination.create_folder("/a/b") is False

        assert mkdir.call_count == 1  # stops at the first failing level
        assert "Failed to create folder" in caplog.text

    async def test_failed_level_is_not_cached(self, make_connected, mock_api):
        lookup = mock_folder_lookup(mock_api, "a", "root", None)
        mkdir = mock_api.post(FILES_URL).mock(side_effect=[
            httpx.Response(500, json=google_error(500)),
            httpx.Response(200, json={"id": "new-a"}),
        ])
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.create_folder("/a") is False
        assert await destination.create_folder("/a") is True

        assert (lookup.call_count, mkdir.call_count) == (2, 2)

    @pytest.mark.parametrize("error", TRANSPORT_ERRORS)
    async def test_transport_error(self, make_connected, mock_api, error):
        lookup = mock_api.get(FILES_URL).mock(side_effect=error("simulated"))
        destination = await make_connected(GoogleDriveDestination)

        assert await destination.create_folder("/a") is False
        assert lookup.call_count == 1

    async def test_root_path_needs_no_request(self, make_connected, mock_api):
        destination = await make_connected(GoogleDriveDestination)
        assert await destination.create_folder("/") is True
        assert mock_api.calls.call_count == 0
