"""
Tests for ``src.integration.sitemap`` (sitemap source).

HTTP is mocked at the httpx transport layer with respx, so the real client
configuration (User-Agent, redirects), sitemap XML parsing, sitemap-index
recursion and BeautifulSoup/lxml HTML cleaning are exercised end to end.
Throttling is disabled (``requestsPerSecond=0``) except in the dedicated
tests, which replace the module's clock and sleep instead of waiting.
"""
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from src.integration import sitemap as sitemap_mod
from src.integration.base import IntegrationType, SourceIntegration
from src.integration.sitemap import SitemapConfig, SitemapSource, _slugify, _url_to_relative_path
from tests.helpers import make_file_info

LOGGER = "src.integration.sitemap"
NOT_CONNECTED = "Not connected"
SITE = "https://www.example.com"
SITEMAP_URL = f"{SITE}/sitemap.xml"
NS = 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'


def urlset(*entries: str, ns: str = NS) -> bytes:
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset {ns}>{"".join(entries)}</urlset>'.encode()


def url_entry(loc: str, lastmod: str | None = None) -> str:
    lastmod_el = f"<lastmod>{lastmod}</lastmod>" if lastmod is not None else ""
    return f"<url><loc>{loc}</loc>{lastmod_el}</url>"


def sitemap_index(*locs: str) -> bytes:
    children = "".join(f"<sitemap><loc>{loc}</loc></sitemap>" for loc in locs)
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex {NS}>{children}</sitemapindex>'.encode()


def rel(url: str, parent: str) -> str:
    """Expected virtual path: ``/<parent>/<last-segment>__<sha1[:8]>.html``."""
    digest = hashlib.sha1(url.encode()).hexdigest()[:8]
    return f"/{parent}__{digest}.html"


def by_path(files) -> dict:
    return {f.path: f for f in files}


SAMPLE = urlset(
    url_entry(f" {SITE}/ ", "2026-01-15"),
    url_entry(f"{SITE}/blog/post-1?ref=x#comments", "2026-01-15T10:20:30Z"),
    url_entry(f"{SITE}/blog/post-2", "2026-01-15T10:20:30+02:00"),
    url_entry(f"{SITE}/about/", "yesterday"),
    url_entry(f"{SITE}/news/item.html"),
    url_entry(""),
    "<url><lastmod>2026-01-01</lastmod></url>",
)
SAMPLE_PATHS = {
    f"{SITE}/": rel(f"{SITE}/", "www.example.com/index"),
    f"{SITE}/blog/post-1?ref=x": rel(f"{SITE}/blog/post-1?ref=x", "www.example.com/blog/post-1"),
    f"{SITE}/blog/post-2": rel(f"{SITE}/blog/post-2", "www.example.com/blog/post-2"),
    f"{SITE}/about/": rel(f"{SITE}/about/", "www.example.com/about"),
    f"{SITE}/news/item.html": rel(f"{SITE}/news/item.html", "www.example.com/news/item.html"),
}

PAGE = """<!DOCTYPE html>
<html><head><title>  Blog post 1 </title><style>.x { color: red }</style><script>track()</script></head>
<body>
  <header>Site header</header>
  <nav><a href="/">Home</a></nav>
  <div class="promo">Promo banner</div>
  <main id="main">
    <h1>Hello</h1>
    <p>Body with <a href="https://www.example.com/x">a link</a>.</p>
    <table><tr><td>cell</td></tr></table>
    <form><input name="q"></form>
  </main>
  <aside>Related</aside>
  <footer>Copyright</footer>
  <noscript>enable js</noscript>
  <iframe src="https://ads.example.com"></iframe>
</body></html>"""


# --- fixtures ----------------------------------------------------------------

def _xfail_expected(request) -> bool:
    return request.node.get_closest_marker("xfail") is not None


@pytest.fixture
def mock_api(request, caplog):
    """respx router: every request must be mocked and every route must be hit."""
    strict = not _xfail_expected(request)
    with respx.mock(assert_all_called=strict, assert_all_mocked=True) as router:
        yield router
    if strict:
        leaked = [r.getMessage() for r in caplog.get_records("call") if "not mocked" in r.getMessage()]
        assert not leaked, f"request(s) without a mocked route: {leaked}"


@pytest.fixture
async def make_connected(mock_api):
    created = []

    async def _make(**config):
        config.setdefault("sitemapUrl", SITEMAP_URL)
        config.setdefault("requestsPerSecond", 0)
        source = SitemapSource(config)
        assert await source.connect() is True
        created.append(source)
        return source

    yield _make
    for source in created:
        await source.disconnect()


@pytest.fixture
def sample(mock_api):
    return mock_api.get(SITEMAP_URL).respond(200, content=SAMPLE, headers={"Content-Type": "application/xml"})


async def listed(source, url: str, **kwargs):
    """List the sitemap and return the FileInfo for ``url``."""
    files = by_path(await source.list_files(**kwargs))
    return files[SAMPLE_PATHS[url]]


# --- config & helpers --------------------------------------------------------

class TestConfig:
    def test_defaults(self):
        config = SitemapConfig.model_validate({"url": SITEMAP_URL, "unknown": 1})

        assert config.sitemap_url == SITEMAP_URL
        assert config.filter_urls == [] and config.exclude_urls == []
        assert config.content_selector is None
        assert config.remove_selectors == ["nav", "header", "footer", "aside", "noscript",
                                           "script", "style", "form", "iframe"]
        assert (config.requests_per_second, config.request_timeout, config.max_urls) == (2.0, 30.0, None)
        assert config.verify_ssl is True and config.follow_sitemap_index is True

    def test_camel_case_aliases(self):
        config = SitemapConfig.model_validate({
            "sitemapUrl": "s", "filterUrls": ["a"], "excludeUrls": ["b"], "contentSelector": "main",
            "removeSelectors": ["nav"], "requestsPerSecond": 0, "requestTimeout": 5, "maxUrls": 3,
            "userAgent": "bot/1", "verifySsl": False, "followSitemapIndex": False,
        })

        assert config.model_dump() == {
            "sitemap_url": "s", "filter_urls": ["a"], "exclude_urls": ["b"], "content_selector": "main",
            "remove_selectors": ["nav"], "requests_per_second": 0.0, "request_timeout": 5.0, "max_urls": 3,
            "user_agent": "bot/1", "verify_ssl": False, "follow_sitemap_index": False,
        }

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("/blog/\r\n/news/, /docs/ \n\n", ["/blog/", "/news/", "/docs/"]),
            ("", []),
            (None, []),
            (["a", "b"], ["a", "b"]),
        ],
    )
    def test_pattern_lists_accept_ui_strings(self, raw, expected):
        config = SitemapConfig.model_validate(
            {"sitemap_url": "s", "filter_urls": raw, "exclude_urls": raw, "remove_selectors": raw})

        assert config.filter_urls == config.exclude_urls == config.remove_selectors == expected

    @pytest.mark.parametrize(
        "field, value",
        [("requestsPerSecond", -1), ("requestTimeout", 0), ("maxUrls", 0)],
    )
    def test_invalid_numbers(self, field, value):
        with pytest.raises(ValidationError):
            SitemapConfig.model_validate({"sitemapUrl": "s", field: value})

    def test_sitemap_url_is_required(self):
        with pytest.raises(ValidationError):
            SitemapConfig.model_validate({})


@pytest.mark.parametrize(
    "value, expected",
    [
        ("post-1", "post-1"),
        ("Caffè & Tè", "Caff-T"),
        ("-.post_1.-", "post_1"),
        ("..--__", "index"),
        ("", "index"),
        ("a" * 100, "a" * 80),
    ],
)
def test_slugify(value, expected):
    assert _slugify(value) == expected


@pytest.mark.parametrize(
    "url, parent",
    [
        (f"{SITE}/blog/post-1", "www.example.com/blog/post-1"),
        (f"{SITE}", "www.example.com/index"),
        ("http://localhost:8080/a b/c", "localhost-8080/a-b/c"),
        ("/just/a/path", "host/just/a/path"),
    ],
)
def test_url_to_relative_path(url, parent):
    assert _url_to_relative_path(url) == rel(url, parent)


def test_query_strings_and_trailing_slashes_get_distinct_paths():
    paths = {_url_to_relative_path(u) for u in (f"{SITE}/a", f"{SITE}/a/", f"{SITE}/a?page=2")}

    assert len(paths) == 3


def test_integration_type_and_interface():
    source = SitemapSource({"url": SITEMAP_URL})

    assert source.integration_type is IntegrationType.SITEMAP
    assert isinstance(source, SourceIntegration)


# --- connect / disconnect ----------------------------------------------------

class TestConnect:
    async def test_client_uses_configured_options(self):
        source = SitemapSource({"url": SITEMAP_URL, "userAgent": "docs-bot/2.0", "requestTimeout": 7})

        assert await source.connect() is True

        client = source._client
        assert client.headers["User-Agent"] == "docs-bot/2.0"
        assert client.follow_redirects is True
        assert client.timeout == httpx.Timeout(7)
        await source.disconnect()
        assert client.is_closed

    async def test_client_creation_failure(self, monkeypatch, caplog):
        def broken(**kwargs):
            raise ValueError("bad TLS config")

        monkeypatch.setattr(sitemap_mod.httpx, "AsyncClient", broken)
        source = SitemapSource({"url": SITEMAP_URL})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.connect() is False

        assert "Failed to initialise sitemap client: bad TLS config" in caplog.text

    async def test_disconnect_is_idempotent_and_blocks_further_use(self, make_connected):
        source = await make_connected()

        await source.disconnect()
        await source.disconnect()

        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await source.list_files()

    @pytest.mark.parametrize(
        "call",
        [lambda s, p: s.list_files(), lambda s, p: s.download_file(make_file_info("/a.html"), p / "a.html")],
        ids=["list_files", "download_file"],
    )
    async def test_operations_require_connect(self, call, tmp_path):
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await call(SitemapSource({"url": SITEMAP_URL}), tmp_path)


# --- list_files: sitemap parsing ---------------------------------------------

class TestListFiles:
    async def test_urlset_entries_become_virtual_html_files(self, make_connected, sample):
        source = await make_connected(userAgent="docs-bot/2.0")

        before = datetime.now(timezone.utc)
        files = by_path(await source.list_files())

        assert set(files) == set(SAMPLE_PATHS.values())
        assert sample.calls.last.request.headers["User-Agent"] == "docs-bot/2.0"
        for info in files.values():
            assert info.content_type == "text/html"
            assert info.size is None
            assert info.name == Path(info.path).name
        modified = {url: files[path].modified_at for url, path in SAMPLE_PATHS.items()}
        assert modified[f"{SITE}/"] == datetime(2026, 1, 15, tzinfo=timezone.utc)
        assert modified[f"{SITE}/blog/post-1?ref=x"] == datetime(2026, 1, 15, 10, 20, 30, tzinfo=timezone.utc)
        assert modified[f"{SITE}/blog/post-2"] == datetime(2026, 1, 15, 8, 20, 30, tzinfo=timezone.utc)
        for undated in (f"{SITE}/about/", f"{SITE}/news/item.html"):
            assert before - timedelta(seconds=1) <= modified[undated] <= datetime.now(timezone.utc)

    @pytest.mark.parametrize("extensions", [None, [], [".html"], [".HTM"], [".pdf", ".html"]])
    async def test_html_extensions_are_listed(self, make_connected, sample, extensions):
        source = await make_connected()

        assert len(await source.list_files(extensions=extensions)) == len(SAMPLE_PATHS)

    @pytest.mark.parametrize("extensions", [[".pdf"], [".md", ".docx"]])
    async def test_other_extensions_yield_nothing_without_fetching(self, make_connected, mock_api, extensions):
        source = await make_connected()

        assert await source.list_files(extensions=extensions) == []
        assert not mock_api.calls

    @pytest.mark.parametrize(
        "config, expected_urls",
        [
            ({"filterUrls": "/blog/"}, {f"{SITE}/blog/post-1?ref=x", f"{SITE}/blog/post-2"}),
            ({"filterUrls": "/blog/", "excludeUrls": r"post-2$"}, {f"{SITE}/blog/post-1?ref=x"}),
            ({"filterUrls": ["about", r"\.html$"]}, {f"{SITE}/about/", f"{SITE}/news/item.html"}),
            ({"excludeUrls": "blog\nnews"}, {f"{SITE}/", f"{SITE}/about/"}),
            ({"filterUrls": "nothing-matches"}, set()),
        ],
    )
    async def test_url_filters(self, make_connected, sample, config, expected_urls):
        source = await make_connected(**config)

        assert set(by_path(await source.list_files())) == {SAMPLE_PATHS[u] for u in expected_urls}

    async def test_folder_path_filters_by_url_path_prefix(self, make_connected, sample):
        source = await make_connected()

        paths = set(by_path(await source.list_files(folder_path="/blog/")))

        assert paths == {SAMPLE_PATHS[f"{SITE}/blog/post-1?ref=x"], SAMPLE_PATHS[f"{SITE}/blog/post-2"]}

    async def test_max_urls_stops_after_the_limit(self, make_connected, sample):
        source = await make_connected(maxUrls=2)

        paths = [f.path for f in await source.list_files()]

        assert paths == [SAMPLE_PATHS[f"{SITE}/"], SAMPLE_PATHS[f"{SITE}/blog/post-1?ref=x"]]

    async def test_duplicate_urls_are_listed_once(self, make_connected, mock_api):
        mock_api.get(SITEMAP_URL).respond(200, content=urlset(
            url_entry(f"{SITE}/a#top"), url_entry(f"{SITE}/a"), url_entry(f"{SITE}/a#bottom")))
        source = await make_connected()

        assert len(await source.list_files()) == 1

    async def test_sitemap_index_is_followed_recursively_once_per_sitemap(self, make_connected, mock_api):
        root = mock_api.get(SITEMAP_URL).respond(200, content=sitemap_index(
            f"{SITE}/sitemap-blog.xml", "", f"{SITE}/sitemap-nested.xml", SITEMAP_URL))
        blog = mock_api.get(f"{SITE}/sitemap-blog.xml").respond(200, content=urlset(url_entry(f"{SITE}/blog/a")))
        nested = mock_api.get(f"{SITE}/sitemap-nested.xml").respond(200, content=sitemap_index(
            f"{SITE}/sitemap-pages.xml", f"{SITE}/sitemap-blog.xml"))
        pages = mock_api.get(f"{SITE}/sitemap-pages.xml").respond(200, content=urlset(url_entry(f"{SITE}/p")))
        source = await make_connected()

        paths = set(by_path(await source.list_files()))

        assert paths == {_url_to_relative_path(f"{SITE}/blog/a"), _url_to_relative_path(f"{SITE}/p")}
        assert [r.call_count for r in (root, blog, nested, pages)] == [1, 1, 1, 1]

    async def test_sitemap_index_can_be_ignored(self, make_connected, mock_api):
        mock_api.get(SITEMAP_URL).respond(200, content=sitemap_index(f"{SITE}/sitemap-blog.xml"))
        source = await make_connected(followSitemapIndex=False)

        assert await source.list_files() == []
        assert len(mock_api.calls) == 1

    async def test_broken_child_sitemap_is_skipped(self, make_connected, mock_api, caplog):
        mock_api.get(SITEMAP_URL).respond(200, content=sitemap_index(
            f"{SITE}/sitemap-gone.xml", f"{SITE}/sitemap-ok.xml"))
        mock_api.get(f"{SITE}/sitemap-gone.xml").respond(404)
        mock_api.get(f"{SITE}/sitemap-ok.xml").respond(200, content=urlset(url_entry(f"{SITE}/ok")))
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            paths = set(by_path(await source.list_files()))

        assert paths == {_url_to_relative_path(f"{SITE}/ok")}
        assert f"Failed to GET {SITE}/sitemap-gone.xml" in caplog.text

    async def test_redirected_sitemap(self, make_connected, mock_api):
        mock_api.get(SITEMAP_URL).respond(301, headers={"Location": f"{SITE}/sitemap_index.xml"})
        mock_api.get(f"{SITE}/sitemap_index.xml").respond(200, content=urlset(url_entry(f"{SITE}/r")))
        source = await make_connected()

        assert set(by_path(await source.list_files())) == {_url_to_relative_path(f"{SITE}/r")}

    @pytest.mark.parametrize(
        "response, message",
        [
            (httpx.Response(500), f"Failed to GET {SITEMAP_URL}"),
            (httpx.Response(200, content=b""), None),
            (httpx.Response(200, content=b"<urlset><url>"), f"Invalid sitemap XML at {SITEMAP_URL}"),
            (httpx.Response(200, content=b"<html><body>Not found</body></html>"),
             f"Unknown sitemap root element <html> at {SITEMAP_URL}"),
        ],
        ids=["http-error", "empty", "malformed", "not-a-sitemap"],
    )
    async def test_unusable_sitemap_yields_nothing(self, make_connected, mock_api, response, message, caplog):
        mock_api.get(SITEMAP_URL).mock(return_value=response)
        source = await make_connected()

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            assert await source.list_files() == []

        if message:
            assert message in caplog.text

    async def test_transport_error(self, make_connected, mock_api, caplog):
        mock_api.get(SITEMAP_URL).mock(side_effect=httpx.ConnectTimeout("slow"))
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.list_files() == []

        assert f"Failed to GET {SITEMAP_URL}" in caplog.text

    async def test_local_sitemap_files(self, make_connected, tmp_path):
        child = tmp_path / "pages.xml"
        child.write_bytes(urlset(url_entry(f"{SITE}/local")))
        index = tmp_path / "sitemap.xml"
        index.write_bytes(sitemap_index(str(child), str(tmp_path / "missing.xml")))
        source = await make_connected(sitemapUrl=str(index))

        assert set(by_path(await source.list_files())) == {_url_to_relative_path(f"{SITE}/local")}

    async def test_missing_local_sitemap(self, make_connected, tmp_path, caplog):
        source = await make_connected(sitemapUrl=str(tmp_path / "nope.xml"))

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.list_files() == []

        assert "Failed to read local sitemap" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sitemap.py:_collect_urls strips the namespace from the root tag but looks "
        "up <url>/<loc> only in the sitemaps.org namespace, so sitemaps without xmlns yield nothing",
    )
    async def test_sitemap_without_namespace(self, make_connected, mock_api):
        mock_api.get(SITEMAP_URL).respond(200, content=urlset(url_entry(f"{SITE}/a"), ns=""))
        source = await make_connected()

        assert set(by_path(await source.list_files())) == {_url_to_relative_path(f"{SITE}/a")}


# --- download_file -----------------------------------------------------------

PAGE_URL = f"{SITE}/blog/post-1?ref=x"


@pytest.fixture
def page(mock_api, sample):
    return mock_api.get(PAGE_URL)


async def download(source, tmp_path, **list_kwargs) -> str:
    info = await listed(source, PAGE_URL, **list_kwargs)
    target = tmp_path / "out" / info.path.lstrip("/")
    assert await source.download_file(info, target) is True
    return target.read_text(encoding="utf-8")


def text_of(html: str) -> str:
    return BeautifulSoup(html, "lxml").get_text(" ", strip=True)


class TestDownloadFile:
    async def test_default_cleaning_keeps_content_and_drops_chrome(self, make_connected, page, tmp_path):
        page.respond(200, html=PAGE)
        source = await make_connected()

        html = await download(source, tmp_path)

        soup = BeautifulSoup(html, "lxml")
        assert html.startswith("<!DOCTYPE html>")
        assert soup.find("meta", attrs={"name": "source-url"})["content"] == PAGE_URL
        assert soup.find("meta", attrs={"charset": "utf-8"}) is not None
        assert soup.title.string == "Blog post 1"
        assert soup.h1.string == "Hello"
        assert soup.find("a", href="https://www.example.com/x").string == "a link"
        assert soup.td.string == "cell"
        text = text_of(html)
        assert "Promo banner" in text
        for noise in ("Site header", "Home", "Related", "Copyright", "track()", "enable js", "color: red"):
            assert noise not in text
        assert soup.find(["form", "iframe", "script", "style", "input"]) is None

    async def test_content_selector_narrows_the_document(self, make_connected, page, tmp_path):
        page.respond(200, html=PAGE)
        source = await make_connected(contentSelector="#main")

        html = await download(source, tmp_path)

        assert "Promo banner" not in text_of(html)
        assert BeautifulSoup(html, "lxml").body.find("main") is None
        assert "Hello" in text_of(html)

    @pytest.mark.parametrize("selector", ["article.missing", "main[", "::nope"])
    async def test_unusable_content_selector_falls_back_to_body(self, make_connected, page, tmp_path, selector):
        page.respond(200, html=PAGE)
        source = await make_connected(contentSelector=selector)

        html = await download(source, tmp_path)

        assert "Promo banner" in text_of(html)
        assert "Hello" in text_of(html)

    async def test_custom_remove_selectors_skip_invalid_ones(self, make_connected, page, tmp_path):
        page.respond(200, html=PAGE)
        source = await make_connected(removeSelectors="nav[\n.promo, footer")

        text = text_of(await download(source, tmp_path))

        assert "Promo banner" not in text and "Copyright" not in text
        assert "Site header" in text and "Home" in text and "Related" in text

    @pytest.mark.parametrize(
        "body, expected_body_text",
        [
            ("<html><body><p>no title</p></body></html>", "no title"),
            ("<html><head><title></title></head><body><p>x</p></body></html>", "x"),
            ("", ""),
        ],
    )
    async def test_pages_without_title_or_body(self, make_connected, page, tmp_path, body, expected_body_text):
        page.respond(200, text=body)
        source = await make_connected()

        html = await download(source, tmp_path)

        assert "<title></title>" in html
        assert text_of(html) == expected_body_text

    async def test_follows_redirects(self, make_connected, mock_api, page, tmp_path):
        page.respond(302, headers={"Location": f"{SITE}/blog/post-1-new"})
        mock_api.get(f"{SITE}/blog/post-1-new").respond(200, html="<body><p>moved</p></body>")
        source = await make_connected()

        assert text_of(await download(source, tmp_path)) == "moved"

    async def test_unknown_entry_is_rejected_without_a_request(self, make_connected, mock_api, tmp_path, caplog):
        source = await make_connected()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(make_file_info("/x/y.html"), tmp_path / "y.html") is False

        assert "Unknown sitemap entry for path /x/y.html" in caplog.text
        assert not mock_api.calls

    async def test_entries_are_reset_by_each_listing(self, make_connected, sample, tmp_path):
        source = await make_connected()
        info = await listed(source, PAGE_URL)

        await source.list_files(folder_path="/about/")

        assert await source.download_file(info, tmp_path / "p.html") is False

    @pytest.mark.parametrize(
        "mock_kwargs",
        [{"return_value": httpx.Response(404)}, {"return_value": httpx.Response(503)},
         {"side_effect": httpx.ReadTimeout("slow")}],
        ids=["404", "503", "timeout"],
    )
    async def test_fetch_errors(self, make_connected, page, tmp_path, mock_kwargs, caplog):
        page.mock(**mock_kwargs)
        source = await make_connected()
        info = await listed(source, PAGE_URL)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(info, tmp_path / "p.html") is False

        assert f"Failed to fetch {PAGE_URL}" in caplog.text
        assert not (tmp_path / "p.html").exists()

    async def test_parse_error(self, make_connected, page, tmp_path, monkeypatch, caplog):
        page.respond(200, html=PAGE)

        def explode(self, html, url):
            raise RecursionError("maximum recursion depth exceeded")

        monkeypatch.setattr(SitemapSource, "_clean_html", explode)
        source = await make_connected()
        info = await listed(source, PAGE_URL)

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(info, tmp_path / "p.html") is False

        assert f"Failed to parse HTML for {PAGE_URL}" in caplog.text

    async def test_write_error(self, make_connected, page, tmp_path, caplog):
        page.respond(200, html=PAGE)
        source = await make_connected()
        info = await listed(source, PAGE_URL)
        occupied = tmp_path / "p.html"
        occupied.mkdir()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(info, occupied) is False

        assert "Failed to write cleaned HTML" in caplog.text

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: sitemap.py:_clean_html interpolates the page <title> text without HTML "
        "escaping, so entity-encoded markup in a title is injected into the cleaned document",
    )
    async def test_title_is_html_escaped(self, make_connected, page, tmp_path):
        page.respond(200, html="<html><head><title>A &lt;/title&gt;&lt;h1&gt;Injected&lt;/h1&gt;</title></head>"
                               "<body><p>x</p></body></html>")
        source = await make_connected()

        soup = BeautifulSoup(await download(source, tmp_path), "lxml")

        assert soup.title.string == "A </title><h1>Injected</h1>"
        assert soup.find("h1") is None


# --- throttling --------------------------------------------------------------

class TestThrottle:
    @pytest.fixture
    def fake_time(self, monkeypatch):
        """Replace the module's clock and sleep so throttling is observable but instant."""
        state = SimpleNamespace(now=100.0, sleeps=[])

        async def sleep(seconds):
            state.sleeps.append(seconds)
            state.now += seconds

        monkeypatch.setattr(sitemap_mod, "time", SimpleNamespace(monotonic=lambda: state.now))
        monkeypatch.setattr(sitemap_mod, "asyncio", SimpleNamespace(
            sleep=sleep, Lock=sitemap_mod.asyncio.Lock, to_thread=sitemap_mod.asyncio.to_thread))
        return state

    async def test_requests_are_spaced_by_the_configured_rate(self, make_connected, fake_time):
        source = await make_connected(requestsPerSecond=4)

        await source._throttle()
        fake_time.now += 0.1
        await source._throttle()
        fake_time.now += 1.0
        await source._throttle()

        assert fake_time.sleeps == [pytest.approx(0.15)]

    async def test_sitemap_and_page_requests_are_throttled(self, make_connected, page, fake_time, tmp_path):
        page.respond(200, html=PAGE)
        source = await make_connected(requestsPerSecond=2)

        await download(source, tmp_path)

        assert fake_time.sleeps == [pytest.approx(0.5)]

    @pytest.mark.parametrize("connected", [True, False])
    async def test_disabled_or_unconnected_never_sleeps(self, make_connected, fake_time, connected):
        if connected:
            source = await make_connected(requestsPerSecond=0)
        else:
            source = SitemapSource({"url": SITEMAP_URL, "requestsPerSecond": 5})

        await source._throttle()
        await source._throttle()

        assert fake_time.sleeps == []
