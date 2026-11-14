"""
Tests for ``src.integration.sharepoint`` (Microsoft Graph source + destination).

HTTP is mocked at the httpx transport layer with respx, so the real request
building (OAuth form body, bearer headers, Graph URLs, JSON bodies, chunk
ranges, redirects) and response parsing are exercised. Everything else -
pydantic validation and local file I/O - is the real thing.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import respx
from pydantic import ValidationError

from src.integration.base import DestinationIntegration, IntegrationType, SourceIntegration
from src.integration.sharepoint import SharePointConfig, SharePointDestination, SharePointSource
from tests.helpers import make_file_info

LOGGER = "src.integration.sharepoint"
NOT_CONNECTED = "Not connected"

TENANT = "9f3c1d2e-0000-4a5b-8c7d-1234567890ab"
SITE = "contoso.sharepoint.com,1a2b3c4d-1111-2222-3333-444455556666,7e8f9a0b-7777-8888-9999-aaaabbbbcccc"
DRIVE = "b!AbCdEf0123456789"
FOLDER = "01ABCDEFFOLDERID"
TOKEN = "eyJ0eXAiOiJKV1Qi.graph-access-token"

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"
DRIVE_URL = f"https://graph.microsoft.com/v1.0/sites/{SITE}/drives/{DRIVE}"
UPLOAD_SESSION_URL = "https://contoso.sharepoint.com/_api/v2.0/drives/b!x/items/01UP/uploadSession?guid=abc"
EXPAND = "listItem($expand=fields)"

BASE_CONFIG = {
    "clientId": "app-client-id",
    "clientSecret": "app-client-secret",
    "tenantId": TENANT,
    "siteId": SITE,
    "driveId": DRIVE,
}


# --- helpers -----------------------------------------------------------------

def _xfail_expected(request) -> bool:
    return request.node.get_closest_marker("xfail") is not None


@pytest.fixture
def mock_api(request, caplog):
    """respx router: every request must be mocked and every route must be hit.

    The integration swallows most exceptions and returns ``False``, so an
    unmocked request would otherwise go unnoticed in the error-path tests.
    """
    strict = not _xfail_expected(request)
    with respx.mock(assert_all_called=strict, assert_all_mocked=True) as router:
        yield router
    if strict:
        leaked = [r.getMessage() for r in caplog.get_records("call") if "not mocked" in r.getMessage()]
        assert not leaked, f"request(s) without a mocked route: {leaked}"


@pytest.fixture
async def make_connected(mock_api):
    """Factory for integrations connected through a mocked client-credentials grant."""
    created = []
    token_route = mock_api.post(TOKEN_URL).respond(
        200, json={"token_type": "Bearer", "expires_in": 3599, "access_token": TOKEN}
    )

    async def _make(cls=SharePointSource, **overrides):
        integration = cls({**BASE_CONFIG, **overrides})
        assert await integration.connect() is True
        created.append(integration)
        return integration

    _make.token_route = token_route
    yield _make
    for integration in created:
        await integration.disconnect()


def file_item(name, *, modified="2026-01-15T12:30:00Z", size=3, mime="application/pdf", fields=None):
    item = {"id": f"id-{name}", "name": name, "size": size, "file": {}}
    if mime is not None:
        item["file"]["mimeType"] = mime
    if modified is not None:
        item["lastModifiedDateTime"] = modified
    if fields is not None:
        item["listItem"] = {"fields": fields}
    return item


def folder_item(name, item_id):
    return {"id": item_id, "name": name, "folder": {"childCount": 1}}


def children(*items, next_link=None):
    body = {"value": list(items)}
    if next_link:
        body["@odata.nextLink"] = next_link
    return body


def by_path(files) -> dict:
    return {f.path: f for f in files}


def assert_bearer(route):
    for call in route.calls:
        assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"


@pytest.fixture
def local_md(tmp_path) -> Path:
    path = tmp_path / "local" / "out.md"
    path.parent.mkdir()
    path.write_bytes("# Résumé ✓\n".encode("utf-8"))
    return path


# --- config ------------------------------------------------------------------

class TestConfig:
    def test_camel_case_aliases_and_defaults(self):
        config = SharePointConfig.model_validate({**BASE_CONFIG, "unknown": 1})

        assert (config.client_id, config.client_secret, config.tenant_id, config.site_id, config.drive_id) == (
            "app-client-id", "app-client-secret", TENANT, SITE, DRIVE)
        assert config.folder_id is None
        assert config.tags_to_include == [] and config.tags_to_exclude == []

    def test_snake_case_names(self):
        config = SharePointConfig.model_validate({
            "client_id": "c", "client_secret": "s", "tenant_id": "t", "site_id": "si", "drive_id": "d",
            "folder_id": "f", "tags_to_include": [{"columnName": "A", "columnValues": ["x"]}],
        })

        assert config.folder_id == "f"
        assert config.tags_to_include == [{"columnName": "A", "columnValues": ["x"]}]

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ('[{"columnName": "Status", "columnValues": ["Approved"]}]',
             [{"columnName": "Status", "columnValues": ["Approved"]}]),
            ("", []),
            ("   ", []),
            ("{not json", []),
            ([{"columnName": "A", "columnValues": []}], [{"columnName": "A", "columnValues": []}]),
        ],
    )
    def test_tag_filters_accept_json_strings_from_the_ui(self, raw, expected):
        config = SharePointConfig.model_validate({**BASE_CONFIG, "tagsToInclude": raw, "tagsToExclude": raw})

        assert config.tags_to_include == expected
        assert config.tags_to_exclude == expected

    @pytest.mark.parametrize("missing", ["clientId", "clientSecret", "tenantId", "siteId", "driveId"])
    def test_required_fields(self, missing):
        data = dict(BASE_CONFIG)
        del data[missing]

        with pytest.raises(ValidationError):
            SharePointConfig.model_validate(data)


@pytest.mark.parametrize(
    "cls, base", [(SharePointSource, SourceIntegration), (SharePointDestination, DestinationIntegration)]
)
def test_integration_type_and_interfaces(cls, base):
    integration = cls(BASE_CONFIG)

    assert integration.integration_type is IntegrationType.SHAREPOINT
    assert isinstance(integration, base)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    async def test_client_credentials_grant(self, mock_api):
        route = mock_api.post(TOKEN_URL).respond(200, json={"access_token": TOKEN})
        source = SharePointSource(BASE_CONFIG)

        assert await source.connect() is True

        request = route.calls.last.request
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert parse_qs(request.content.decode()) == {
            "grant_type": ["client_credentials"],
            "client_id": ["app-client-id"],
            "client_secret": ["app-client-secret"],
            "scope": ["https://graph.microsoft.com/.default"],
        }
        await source.disconnect()

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    async def test_auth_error_status(self, mock_api, status, caplog):
        mock_api.post(TOKEN_URL).respond(status, json={"error": "invalid_client"})
        source = SharePointSource(BASE_CONFIG)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        assert f"Auth error: {status}" in caplog.text
        assert "invalid_client" in caplog.text
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()
        await source.disconnect()

    async def test_token_response_without_access_token(self, mock_api):
        mock_api.post(TOKEN_URL).respond(200, json={"token_type": "Bearer"})
        source = SharePointSource(BASE_CONFIG)

        assert await source.connect() is False
        await source.disconnect()

    @pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ReadTimeout])
    async def test_transport_errors(self, mock_api, error, caplog):
        mock_api.post(TOKEN_URL).mock(side_effect=error("boom"))
        source = SharePointSource(BASE_CONFIG)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        assert "Failed to connect to SharePoint" in caplog.text
        await source.disconnect()


class TestDisconnect:
    async def test_closes_client_and_forgets_token(self, make_connected):
        source = await make_connected()
        client = source._client

        await source.disconnect()
        await source.disconnect()

        assert client.is_closed
        assert source._access_token is None
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    async def test_disconnect_without_connect_is_a_noop(self):
        await SharePointDestination(BASE_CONFIG).disconnect()


@pytest.mark.parametrize(
    "call",
    [
        lambda i, p: i.list_files(),
        lambda i, p: i.download_file(make_file_info("/a.pdf"), p / "a.pdf"),
        lambda i, p: i.upload_file(p / "a.md", "/a.md"),
        lambda i, p: i.create_folder("/a"),
    ],
    ids=["list_files", "download_file", "upload_file", "create_folder"],
)
async def test_operations_require_connect(call, tmp_path):
    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await call(SharePointDestination(BASE_CONFIG), tmp_path)


# --- list_files --------------------------------------------------------------

@pytest.fixture
def drive_tree(mock_api):
    """
    root: a.pdf, B.PDF, notes.txt (no mimeType), OneNote package, docs/
    docs: c.docx, deep/
    deep: d.pdf
    """
    routes = {
        "root": mock_api.get(f"{DRIVE_URL}/root/children").respond(200, json=children(
            file_item("a.pdf", size=11),
            file_item("B.PDF"),
            file_item("notes.txt", mime=None),
            {"id": "nb", "name": "Notebook", "package": {"type": "oneNote"}},
            folder_item("docs", "F-docs"),
        )),
        "docs": mock_api.get(f"{DRIVE_URL}/items/F-docs/children").respond(200, json=children(
            file_item("c.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            folder_item("deep", "F-deep"),
        )),
        "deep": mock_api.get(f"{DRIVE_URL}/items/F-deep/children").respond(200, json=children(
            file_item("d.pdf", modified="2025-12-31T23:59:59.123+01:00", size=None),
        )),
    }
    return routes


class TestListFiles:
    async def test_walks_folders_recursively(self, make_connected, drive_tree):
        source = await make_connected()

        files = by_path(await source.list_files())

        assert set(files) == {"/a.pdf", "/B.PDF", "/notes.txt", "/docs/c.docx", "/docs/deep/d.pdf"}
        a = files["/a.pdf"]
        assert (a.name, a.size, a.content_type) == ("a.pdf", 11, "application/pdf")
        assert a.modified_at == datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)
        assert files["/notes.txt"].content_type is None
        deep = files["/docs/deep/d.pdf"]
        assert deep.size is None
        assert deep.modified_at == datetime(2025, 12, 31, 23, 59, 59, 123000, tzinfo=timezone(timedelta(hours=1)))
        for route in drive_tree.values():
            assert_bearer(route)
            assert "$expand" not in route.calls.last.request.url.params

    @pytest.mark.parametrize(
        "extensions, expected",
        [
            ([".pdf"], {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}),
            ([".PDF", ".TXT"], {"/a.pdf", "/B.PDF", "/notes.txt", "/docs/deep/d.pdf"}),
            ([".docx"], {"/docs/c.docx"}),
            ([".xlsx"], set()),
        ],
    )
    async def test_extension_filter_is_case_insensitive(self, make_connected, drive_tree, extensions, expected):
        source = await make_connected()

        assert set(by_path(await source.list_files(extensions=extensions))) == expected

    async def test_configured_folder_is_the_starting_point(self, make_connected, mock_api):
        route = mock_api.get(f"{DRIVE_URL}/items/{FOLDER}/children").respond(
            200, json=children(file_item("x.pdf")))
        source = await make_connected(folderId=FOLDER)

        assert set(by_path(await source.list_files())) == {"/x.pdf"}
        assert route.call_count == 1

    async def test_follows_odata_next_link(self, make_connected, mock_api):
        next_link = f"{DRIVE_URL}/root/children?$skiptoken=page2"
        route = mock_api.get(f"{DRIVE_URL}/root/children").mock(side_effect=[
            httpx.Response(200, json=children(file_item("p1.pdf"), next_link=next_link)),
            httpx.Response(200, json=children(file_item("p2.pdf"))),
        ])
        source = await make_connected()

        assert set(by_path(await source.list_files())) == {"/p1.pdf", "/p2.pdf"}
        assert route.calls[1].request.url == httpx.URL(next_link)
        assert_bearer(route)

    async def test_failed_page_keeps_what_was_listed(self, make_connected, mock_api, caplog):
        mock_api.get(f"{DRIVE_URL}/root/children").mock(side_effect=[
            httpx.Response(200, json=children(
                file_item("p1.pdf"), folder_item("locked", "F-locked"),
                next_link=f"{DRIVE_URL}/root/children?$skiptoken=2")),
            httpx.Response(503),
        ])
        mock_api.get(f"{DRIVE_URL}/items/F-locked/children").respond(403)
        source = await make_connected()

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            assert set(by_path(await source.list_files())) == {"/p1.pdf"}

        assert "Failed to get children: 403" in caplog.text
        assert "Failed to get children: 503" in caplog.text

    @pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow:DeprecationWarning")
    @pytest.mark.parametrize("modified", [None, "not-a-date"])
    async def test_unparseable_modified_time_falls_back_to_now(self, make_connected, mock_api, modified):
        mock_api.get(f"{DRIVE_URL}/root/children").respond(200, json=children(file_item("a.pdf", modified=modified)))
        source = await make_connected()

        before = datetime.utcnow()
        (info,) = await source.list_files()

        assert before - timedelta(seconds=1) <= info.modified_at <= datetime.utcnow() + timedelta(seconds=1)


class TestTagFilters:
    INCLUDE = [{"columnName": "Status", "columnValues": ["Approved", "Published"]}]
    EXCLUDE = [{"columnName": "Confidential", "columnValues": ["Yes"]}]

    @pytest.fixture
    def tagged(self, mock_api):
        return mock_api.get(f"{DRIVE_URL}/root/children").respond(200, json=children(
            file_item("approved.pdf", fields={"Status": "Approved"}),
            file_item("published-secret.pdf", fields={"Status": "Published", "Confidential": "Yes"}),
            file_item("draft.pdf", fields={"Status": "Draft"}),
            file_item("published-public.pdf", fields={"Status": "Published", "Confidential": "No"}),
            file_item("untagged.pdf"),
        ))

    async def test_include_and_exclude(self, make_connected, tagged):
        source = await make_connected(tagsToInclude=json.dumps(self.INCLUDE), tagsToExclude=self.EXCLUDE)

        paths = set(by_path(await source.list_files()))

        assert paths == {"/approved.pdf", "/published-public.pdf"}
        assert tagged.calls.last.request.url.params["$expand"] == EXPAND

    async def test_exclude_only_keeps_untagged_files(self, make_connected, tagged):
        source = await make_connected(tagsToExclude=self.EXCLUDE)

        paths = set(by_path(await source.list_files()))

        assert paths == {"/approved.pdf", "/draft.pdf", "/published-public.pdf", "/untagged.pdf"}

    async def test_expand_is_kept_on_subfolders(self, make_connected, mock_api):
        sub = mock_api.get(f"{DRIVE_URL}/items/F-sub/children").respond(
            200, json=children(file_item("x.pdf", fields={"Status": "Approved"})))
        mock_api.get(f"{DRIVE_URL}/items/{FOLDER}/children").respond(200, json=children(folder_item("sub", "F-sub")))
        source = await make_connected(folderId=FOLDER, tagsToInclude=self.INCLUDE)

        assert set(by_path(await source.list_files())) == {"/sub/x.pdf"}
        assert sub.calls.last.request.url.params["$expand"] == EXPAND


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    async def test_resolves_item_and_follows_redirect_to_content(self, make_connected, mock_api, tmp_path):
        lookup = mock_api.get(f"{DRIVE_URL}/root:/docs/a.pdf").respond(200, json={"id": "ITEM-A"})
        content = mock_api.get(f"{DRIVE_URL}/items/ITEM-A/content").respond(
            302, headers={"Location": "https://contoso.sharepoint.com/download.aspx?tempauth=xyz"})
        cdn = mock_api.get("https://contoso.sharepoint.com/download.aspx").respond(200, content=b"%PDF-1.7")
        source = await make_connected()
        target = tmp_path / "out" / "docs" / "a.pdf"

        assert await source.download_file(make_file_info("/docs/a.pdf"), target) is True

        assert target.read_bytes() == b"%PDF-1.7"
        assert_bearer(lookup)
        assert_bearer(content)
        # httpx drops the bearer token on the cross-origin redirect to the pre-authenticated URL
        assert "Authorization" not in cdn.calls.last.request.headers

    async def test_paths_are_resolved_under_the_configured_folder(self, make_connected, mock_api, tmp_path):
        mock_api.get(f"{DRIVE_URL}/items/{FOLDER}:/docs/a.pdf").respond(200, json={"id": "ITEM-A"})
        mock_api.get(f"{DRIVE_URL}/items/ITEM-A/content").respond(200, content=b"data")
        source = await make_connected(folderId=FOLDER)

        assert await source.download_file(make_file_info("/docs/a.pdf"), tmp_path / "a.pdf") is True
        assert (tmp_path / "a.pdf").read_bytes() == b"data"

    async def test_unknown_item(self, make_connected, mock_api, tmp_path, caplog):
        mock_api.get(f"{DRIVE_URL}/root:/gone.pdf").respond(404, json={"error": {"code": "itemNotFound"}})
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(make_file_info("/gone.pdf"), tmp_path / "gone.pdf") is False

        assert "Could not find item: /gone.pdf" in caplog.text
        assert not (tmp_path / "gone.pdf").exists()

    @pytest.mark.parametrize("status", [403, 404, 500])
    async def test_content_error(self, make_connected, mock_api, tmp_path, status, caplog):
        mock_api.get(f"{DRIVE_URL}/root:/a.pdf").respond(200, json={"id": "ITEM-A"})
        mock_api.get(f"{DRIVE_URL}/items/ITEM-A/content").respond(status)
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(make_file_info("/a.pdf"), tmp_path / "a.pdf") is False

        assert f"Download failed: {status} - /a.pdf" in caplog.text
        assert not (tmp_path / "a.pdf").exists()

    async def test_transport_error(self, make_connected, mock_api, tmp_path, caplog):
        mock_api.get(f"{DRIVE_URL}/root:/a.pdf").mock(side_effect=httpx.ReadTimeout("slow"))
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(make_file_info("/a.pdf"), tmp_path / "a.pdf") is False

        assert "Download error for /a.pdf" in caplog.text


# --- upload_file -------------------------------------------------------------

def folder_meta(name, parent_path):
    return {"id": FOLDER, "name": name, "folder": {}, "parentReference": {"path": parent_path} if parent_path else {}}


class TestSimpleUpload:
    @pytest.mark.parametrize("status", [200, 201])
    async def test_puts_content(self, make_connected, mock_api, local_md, status):
        route = mock_api.put(f"{DRIVE_URL}/root:/out/docs/a.md:/content").respond(status, json={"id": "N"})
        dest = await make_connected(SharePointDestination)

        assert await dest.upload_file(local_md, "/out/docs/a.md") is True

        request = route.calls.last.request
        assert request.content == local_md.read_bytes()
        assert request.headers["Content-Type"] == "application/octet-stream"
        assert_bearer(route)

    @pytest.mark.parametrize(
        "parent_path, expected",
        [
            ("/drives/b!x/root:/Shared Documents/Team", "Shared Documents/Team/Converted/out/a.md"),
            ("/drive/root:", "Converted/out/a.md"),
            (None, "Converted/out/a.md"),
        ],
    )
    async def test_configured_folder_prefixes_the_path(self, make_connected, mock_api, local_md, parent_path, expected):
        meta = mock_api.get(f"{DRIVE_URL}/items/{FOLDER}").respond(200, json=folder_meta("Converted", parent_path))
        route = mock_api.put(f"{DRIVE_URL}/root:/{expected}:/content").respond(201)
        dest = await make_connected(SharePointDestination, folderId=FOLDER)

        assert await dest.upload_file(local_md, "out/a.md") is True

        assert route.call_count == 1
        assert_bearer(meta)

    @pytest.mark.parametrize("status", [400, 409, 507])
    async def test_rejected_upload(self, make_connected, mock_api, local_md, status, caplog):
        mock_api.put(f"{DRIVE_URL}/root:/a.md:/content").respond(status, text="nameAlreadyExists")
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(local_md, "/a.md") is False

        assert f"Simple upload failed for a.md: {status} - nameAlreadyExists" in caplog.text

    async def test_missing_local_file(self, make_connected, tmp_path, caplog):
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(tmp_path / "missing.md", "/a.md") is False

        assert "Upload error for /a.md" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sharepoint.py:_simple_upload interpolates the path into the URL without "
        "percent-encoding, so a '#' in a file name truncates the request URL",
    )
    async def test_special_characters_are_percent_encoded(self, make_connected, mock_api, local_md):
        route = mock_api.put(f"{DRIVE_URL}/root:/docs/C%23%20basics.md:/content").respond(201)
        dest = await make_connected(SharePointDestination)

        assert await dest.upload_file(local_md, "/docs/C# basics.md") is True
        assert route.call_count == 1

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sharepoint.py:upload_file silently uploads to the drive root when the "
        "configured folder cannot be resolved, instead of failing",
    )
    async def test_unresolvable_configured_folder_fails_the_upload(self, make_connected, mock_api, local_md):
        mock_api.get(f"{DRIVE_URL}/items/{FOLDER}").respond(404)
        root_put = mock_api.put(f"{DRIVE_URL}/root:/a.md:/content").respond(201)
        dest = await make_connected(SharePointDestination, folderId=FOLDER)

        assert await dest.upload_file(local_md, "/a.md") is False
        assert root_put.call_count == 0


class TestSessionUpload:
    @pytest.fixture
    def small_limit(self, monkeypatch):
        monkeypatch.setattr(SharePointDestination, "SIMPLE_UPLOAD_LIMIT", 4)

    @pytest.fixture
    def create_session(self, mock_api):
        return mock_api.post(f"{DRIVE_URL}/root:/out/big.bin:/createUploadSession")

    async def test_single_chunk(self, make_connected, mock_api, create_session, small_limit, tmp_path):
        local = tmp_path / "big.bin"
        local.write_bytes(b"0123456789")
        create_session.respond(200, json={"uploadUrl": UPLOAD_SESSION_URL})
        chunk = mock_api.put(UPLOAD_SESSION_URL).respond(201, json={"id": "N"})
        dest = await make_connected(SharePointDestination)

        assert await dest.upload_file(local, "/out/big.bin") is True

        session_request = create_session.calls.last.request
        assert json.loads(session_request.content) == {
            "item": {"@microsoft.graph.conflictBehavior": "replace", "name": "big.bin"}
        }
        assert session_request.headers["Content-Type"] == "application/json"
        assert_bearer(create_session)
        request = chunk.calls.last.request
        assert request.content == b"0123456789"
        assert request.headers["Content-Range"] == "bytes 0-9/10"
        assert request.headers["Content-Length"] == "10"
        # the pre-authenticated upload URL must not receive the Graph token
        assert "Authorization" not in request.headers

    async def test_content_above_simple_limit_is_sent_in_10_mib_chunks(
        self, make_connected, mock_api, create_session, tmp_path
    ):
        total = 10 * 1024 * 1024 + 3
        local = tmp_path / "big.bin"
        local.write_bytes(b"x" * total)
        create_session.respond(201, json={"uploadUrl": UPLOAD_SESSION_URL})
        chunk = mock_api.put(UPLOAD_SESSION_URL).mock(side_effect=[httpx.Response(202), httpx.Response(201)])
        dest = await make_connected(SharePointDestination)

        assert await dest.upload_file(local, "out/big.bin") is True

        ranges = [c.request.headers["Content-Range"] for c in chunk.calls]
        assert ranges == [f"bytes 0-{10 * 1024 * 1024 - 1}/{total}", f"bytes {10 * 1024 * 1024}-{total - 1}/{total}"]
        assert [len(c.request.content) for c in chunk.calls] == [10 * 1024 * 1024, 3]

    @pytest.mark.parametrize("status", [400, 403, 500])
    async def test_session_creation_rejected(self, make_connected, create_session, small_limit, local_md, status, caplog):
        create_session.respond(status)
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(local_md, "/out/big.bin") is False

        assert f"Failed to create upload session for out/big.bin: {status}" in caplog.text

    async def test_session_without_upload_url(self, make_connected, create_session, small_limit, local_md):
        create_session.respond(200, json={"expirationDateTime": "2026-01-15T12:30:00Z"})
        dest = await make_connected(SharePointDestination)

        assert await dest.upload_file(local_md, "/out/big.bin") is False

    async def test_chunk_rejected(self, make_connected, mock_api, create_session, small_limit, local_md, caplog):
        create_session.respond(200, json={"uploadUrl": UPLOAD_SESSION_URL})
        mock_api.put(UPLOAD_SESSION_URL).respond(416)
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.upload_file(local_md, "/out/big.bin") is False

        assert "Chunk upload failed at 0: 416" in caplog.text


# --- create_folder -----------------------------------------------------------

class TestCreateFolder:
    async def test_existing_folders_are_not_recreated(self, make_connected, mock_api):
        a = mock_api.get(f"{DRIVE_URL}/root:/a").respond(200, json={"id": "A"})
        ab = mock_api.get(f"{DRIVE_URL}/root:/a/b").respond(200, json={"id": "B"})
        dest = await make_connected(SharePointDestination)

        assert await dest.create_folder("/a/b/") is True

        assert a.call_count == ab.call_count == 1
        assert_bearer(a)

    async def test_creates_missing_folders_level_by_level(self, make_connected, mock_api):
        mock_api.get(f"{DRIVE_URL}/root:/reports").respond(404)
        mock_api.get(f"{DRIVE_URL}/root:/reports/2026").respond(404)
        top = mock_api.post(f"{DRIVE_URL}/root/children").respond(201, json={"id": "R"})
        nested = mock_api.post(f"{DRIVE_URL}/root:/reports:/children").respond(201, json={"id": "Y"})
        dest = await make_connected(SharePointDestination)

        assert await dest.create_folder("reports/2026") is True

        for route, name in ((top, "reports"), (nested, "2026")):
            request = route.calls.last.request
            assert json.loads(request.content) == {
                "name": name, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"
            }
            assert request.headers["Content-Type"] == "application/json"
            assert_bearer(route)

    async def test_conflict_means_someone_else_created_it(self, make_connected, mock_api):
        mock_api.get(f"{DRIVE_URL}/root:/a").respond(404)
        mock_api.post(f"{DRIVE_URL}/root/children").respond(409, json={"error": {"code": "nameAlreadyExists"}})
        mock_api.get(f"{DRIVE_URL}/root:/a/b").respond(200, json={"id": "B"})
        dest = await make_connected(SharePointDestination)

        assert await dest.create_folder("a/b") is True

    async def test_under_configured_folder(self, make_connected, mock_api):
        mock_api.get(f"{DRIVE_URL}/items/{FOLDER}").respond(
            200, json=folder_meta("Converted", "/drives/b!x/root:/Shared"))
        mock_api.get(f"{DRIVE_URL}/root:/Shared/Converted/new").respond(404)
        create = mock_api.post(f"{DRIVE_URL}/root:/Shared/Converted:/children").respond(201)
        dest = await make_connected(SharePointDestination, folderId=FOLDER)

        assert await dest.create_folder("new") is True
        assert json.loads(create.calls.last.request.content)["name"] == "new"

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sharepoint.py:create_folder silently works at the drive root when the "
        "configured folder cannot be resolved, instead of failing",
    )
    async def test_unresolvable_configured_folder_fails(self, make_connected, mock_api):
        mock_api.get(f"{DRIVE_URL}/items/{FOLDER}").respond(404)
        check = mock_api.get(f"{DRIVE_URL}/root:/new").respond(200)
        dest = await make_connected(SharePointDestination, folderId=FOLDER)

        assert await dest.create_folder("new") is False
        assert check.call_count == 0

    @pytest.mark.parametrize("path", ["", "/", "//"])
    async def test_empty_path_needs_no_request(self, make_connected, path):
        dest = await make_connected(SharePointDestination)

        assert await dest.create_folder(path) is True

    async def test_creation_rejected(self, make_connected, mock_api, caplog):
        mock_api.get(f"{DRIVE_URL}/root:/a").respond(404)
        mock_api.post(f"{DRIVE_URL}/root/children").respond(403, text="accessDenied")
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.create_folder("a/b") is False

        assert "Failed to create folder a: 403 - accessDenied" in caplog.text

    async def test_transport_error(self, make_connected, mock_api, caplog):
        mock_api.get(f"{DRIVE_URL}/root:/a").mock(side_effect=httpx.ConnectError("down"))
        dest = await make_connected(SharePointDestination)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.create_folder("a") is False

        assert "Failed to create folder a" in caplog.text
