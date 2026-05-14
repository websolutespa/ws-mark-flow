"""
Sitemap source integration.

Loads a `sitemap.xml` (or sitemap index), fetches each page, strips noise
(nav/header/footer/scripts/etc.) and optionally narrows to a CSS selector,
then writes a cleaned HTML fragment to disk. The downstream `Converter`
(MarkItDown / Docling) then turns the HTML into Markdown, preserving
headings, links, tables, and lists.

No LangChain dependency: uses httpx (already a project dep) + BeautifulSoup4.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, urldefrag
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from pydantic import AliasChoices, BaseModel, Field, field_validator

from .base import FileInfo, IntegrationType, SourceIntegration

logger = logging.getLogger(__name__)


_DEFAULT_REMOVE_SELECTORS = [
    "nav", "header", "footer", "aside", "noscript",
    "script", "style", "form", "iframe",
]

_SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


class SitemapConfig(BaseModel):
    """Configuration for the Sitemap source integration."""

    sitemap_url: str = Field(
        description="HTTP(S) URL or local path to a sitemap.xml (sitemap indexes are followed).",
        validation_alias=AliasChoices("sitemapUrl", "sitemap_url", "url"),
    )
    filter_urls: list[str] = Field(
        default_factory=list,
        description="Regex patterns; if non-empty a URL must match at least one to be included.",
        validation_alias=AliasChoices("filterUrls", "filter_urls"),
    )
    exclude_urls: list[str] = Field(
        default_factory=list,
        description="Regex patterns; URLs matching any are skipped.",
        validation_alias=AliasChoices("excludeUrls", "exclude_urls"),
    )
    content_selector: Optional[str] = Field(
        default=None,
        description="Optional CSS selector to narrow content (e.g. '#main', 'main', 'article').",
        validation_alias=AliasChoices("contentSelector", "content_selector"),
    )
    remove_selectors: list[str] = Field(
        default_factory=lambda: list(_DEFAULT_REMOVE_SELECTORS),
        description="CSS selectors stripped before saving (default removes nav/header/footer/script/style/...).",
        validation_alias=AliasChoices("removeSelectors", "remove_selectors"),
    )
    requests_per_second: float = Field(
        default=2.0,
        ge=0.0,
        description="Politeness throttle. 0 disables throttling.",
        validation_alias=AliasChoices("requestsPerSecond", "requests_per_second"),
    )
    request_timeout: float = Field(
        default=30.0,
        gt=0.0,
        validation_alias=AliasChoices("requestTimeout", "request_timeout"),
    )
    max_urls: Optional[int] = Field(
        default=None,
        ge=1,
        validation_alias=AliasChoices("maxUrls", "max_urls"),
    )
    user_agent: str = Field(
        default="ws-mark-flow/1.0 (+sitemap-source)",
        validation_alias=AliasChoices("userAgent", "user_agent"),
    )
    verify_ssl: bool = Field(
        default=True,
        validation_alias=AliasChoices("verifySsl", "verify_ssl"),
    )
    follow_sitemap_index: bool = Field(
        default=True,
        validation_alias=AliasChoices("followSitemapIndex", "follow_sitemap_index"),
    )

    @field_validator("filter_urls", "exclude_urls", "remove_selectors", mode="before")
    @classmethod
    def _split_lines(cls, v):
        """Accept either a list or a newline/comma-separated string from the UI."""
        if v is None or v == "":
            return []
        if isinstance(v, str):
            parts = [p.strip() for p in re.split(r"[\r\n,]+", v)]
            return [p for p in parts if p]
        return v

    class Config:
        extra = "ignore"


def _slugify(value: str, max_len: int = 80) -> str:
    """Make a filesystem-friendly slug from a URL path segment."""
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-._")
    if not value:
        value = "index"
    return value[:max_len]


def _url_to_relative_path(url: str) -> str:
    """
    Map a URL to a stable, unique relative path ending in `.html`.

    Example: https://site.com/blog/post-1?x=y  -> /site.com/blog/post-1__<hash>.html
    The hash disambiguates query strings and trailing-slash variants.
    """
    parsed = urlparse(url)
    host = parsed.netloc or "host"
    path = parsed.path or "/"
    segments = [s for s in path.split("/") if s]
    if not segments:
        segments = ["index"]

    slug_segments = [_slugify(s) for s in segments]
    # Disambiguate with a short hash over the full URL (incl. query)
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    filename = f"{slug_segments[-1]}__{digest}.html"
    parent = "/".join([_slugify(host), *slug_segments[:-1]])
    rel = f"/{parent}/{filename}" if parent else f"/{filename}"
    return rel


class SitemapSource(SourceIntegration):
    """Source integration that enumerates URLs from a sitemap and downloads cleaned HTML."""

    def __init__(self, config: dict[str, Any]):
        self._config = SitemapConfig.model_validate(config)
        self._client: Optional[httpx.AsyncClient] = None
        # Cache: rel_path -> (url, lastmod)
        self._entries: dict[str, tuple[str, Optional[datetime]]] = {}
        # Throttle bookkeeping
        self._last_request_ts: float = 0.0
        self._throttle_lock: Optional[asyncio.Lock] = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.SITEMAP

    async def connect(self) -> bool:
        try:
            self._client = httpx.AsyncClient(
                timeout=self._config.request_timeout,
                follow_redirects=True,
                verify=self._config.verify_ssl,
                headers={"User-Agent": self._config.user_agent},
            )
            self._throttle_lock = asyncio.Lock()
            return True
        except Exception as e:
            logger.error(f"Failed to initialise sitemap client: {e}")
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        self._throttle_lock = None

    # ---- sitemap parsing ----

    async def _fetch_bytes(self, url: str) -> Optional[bytes]:
        if url.startswith(("http://", "https://")):
            assert self._client is not None
            await self._throttle()
            try:
                resp = await self._client.get(url)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.error(f"Failed to GET {url}: {e}")
                return None
        # local path
        try:
            return await asyncio.to_thread(Path(url).read_bytes)
        except Exception as e:
            logger.error(f"Failed to read local sitemap {url}: {e}")
            return None

    async def _throttle(self) -> None:
        rps = self._config.requests_per_second
        if rps <= 0 or self._throttle_lock is None:
            return
        min_gap = 1.0 / rps
        async with self._throttle_lock:
            elapsed = time.monotonic() - self._last_request_ts
            if elapsed < min_gap:
                await asyncio.sleep(min_gap - elapsed)
            self._last_request_ts = time.monotonic()

    @staticmethod
    def _strip_ns(tag: str) -> str:
        return tag.split("}", 1)[1] if "}" in tag else tag

    @staticmethod
    def _parse_lastmod(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        v = value.strip()
        # Accept YYYY-MM-DD and full ISO 8601
        try:
            if len(v) == 10:
                return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            # fromisoformat handles offsets in 3.11+; normalise trailing Z
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            return datetime.fromisoformat(v)
        except Exception:
            return None

    async def _collect_urls(
        self, sitemap_url: str, seen: set[str]
    ) -> list[tuple[str, Optional[datetime]]]:
        """Recursively walk sitemap (handling sitemapindex). Returns (url, lastmod) tuples."""
        if sitemap_url in seen:
            return []
        seen.add(sitemap_url)

        raw = await self._fetch_bytes(sitemap_url)
        if not raw:
            return []
        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            logger.error(f"Invalid sitemap XML at {sitemap_url}: {e}")
            return []

        tag = self._strip_ns(root.tag)
        results: list[tuple[str, Optional[datetime]]] = []

        if tag == "sitemapindex":
            if not self._config.follow_sitemap_index:
                return []
            children = [
                (child.findtext("sm:loc", namespaces=_SITEMAP_NS) or "").strip()
                for child in root.findall("sm:sitemap", namespaces=_SITEMAP_NS)
            ]
            for child_url in children:
                if child_url:
                    results.extend(await self._collect_urls(child_url, seen))
            return results

        if tag == "urlset":
            for url_el in root.findall("sm:url", namespaces=_SITEMAP_NS):
                loc = (url_el.findtext("sm:loc", namespaces=_SITEMAP_NS) or "").strip()
                if not loc:
                    continue
                loc, _ = urldefrag(loc)
                lastmod = self._parse_lastmod(
                    url_el.findtext("sm:lastmod", namespaces=_SITEMAP_NS)
                )
                results.append((loc, lastmod))
            return results

        logger.warning(f"Unknown sitemap root element <{tag}> at {sitemap_url}")
        return results

    def _matches_filters(self, url: str) -> bool:
        for pat in self._config.exclude_urls:
            if re.search(pat, url):
                return False
        if not self._config.filter_urls:
            return True
        return any(re.search(pat, url) for pat in self._config.filter_urls)

    # ---- SourceIntegration interface ----

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
    ) -> list[FileInfo]:
        """
        Enumerate URLs in the sitemap. Each URL is presented as a virtual `.html` file.

        `extensions` and `folder_path` are accepted for interface compatibility:
        - extensions: if provided and `.html` not in it, returns []
        - folder_path: filters by URL path prefix (e.g. "/blog/")
        """
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        if extensions:
            normalized = [e.lower() for e in extensions]
            if ".html" not in normalized and ".htm" not in normalized:
                return []

        seen: set[str] = set()
        entries = await self._collect_urls(self._config.sitemap_url, seen)

        files: list[FileInfo] = []
        self._entries.clear()
        for url, lastmod in entries:
            if not self._matches_filters(url):
                continue
            if folder_path:
                parsed = urlparse(url)
                if not parsed.path.startswith(folder_path):
                    continue

            rel_path = _url_to_relative_path(url)
            if rel_path in self._entries:
                # Duplicate slug+hash collision is essentially impossible, but guard anyway
                continue

            name = Path(rel_path).name
            modified = lastmod or datetime.now(timezone.utc)
            files.append(FileInfo(
                name=name,
                path=rel_path,
                modified_at=modified,
                size=None,
                content_type="text/html",
            ))
            self._entries[rel_path] = (url, lastmod)

            if self._config.max_urls and len(files) >= self._config.max_urls:
                break

        logger.info(f"Sitemap {self._config.sitemap_url} yielded {len(files)} URL(s).")
        return files

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        """Fetch the URL, clean its HTML, and write the result to `local_path`."""
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        entry = self._entries.get(file_info.path)
        if entry is None:
            logger.error(f"Unknown sitemap entry for path {file_info.path}; call list_files first.")
            return False
        url, _lastmod = entry

        await self._throttle()
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return False

        try:
            cleaned = await asyncio.to_thread(self._clean_html, resp.text, url)
        except Exception as e:
            logger.error(f"Failed to parse HTML for {url}: {e}")
            return False

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(local_path.write_text, cleaned, "utf-8")
            return True
        except Exception as e:
            logger.error(f"Failed to write cleaned HTML to {local_path}: {e}")
            return False

    def _clean_html(self, html: str, source_url: str) -> str:
        """Strip unwanted nodes, optionally narrow to content_selector, return a small HTML doc."""
        soup = BeautifulSoup(html, "lxml")

        # Drop unwanted nodes
        for selector in self._config.remove_selectors:
            try:
                for node in soup.select(selector):
                    node.decompose()
            except Exception:
                # bs4 raises on a few exotic selectors; ignore them
                continue

        # Narrow to main content if a selector is given
        root_node = None
        if self._config.content_selector:
            try:
                root_node = soup.select_one(self._config.content_selector)
            except Exception:
                root_node = None
        if root_node is None:
            root_node = soup.body or soup

        # Extract <title> for context (MarkItDown uses it as H1)
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        # Build a minimal, well-formed HTML document so MarkItDown converts cleanly.
        body_html = root_node.decode_contents() if hasattr(root_node, "decode_contents") else str(root_node)
        return (
            "<!DOCTYPE html>\n<html><head>"
            f'<meta charset="utf-8">'
            f'<meta name="source-url" content="{source_url}">'
            f"<title>{title}</title>"
            "</head><body>"
            f"{body_html}"
            "</body></html>"
        )
