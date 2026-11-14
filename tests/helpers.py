"""
In-memory fakes shared across the test suite.

They implement the real abstract interfaces, so code under test exercises the
same contract it uses in production without touching any external system.
"""
import importlib
import json
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Optional

from src.integration.base import (
    DestinationIntegration,
    FileInfo,
    IntegrationType,
    SourceIntegration,
)
from src.vectorstore.base import (
    Chunk,
    DocumentRecord,
    VectorStoreIntegration,
    VectorStoreType,
)

DEFAULT_MTIME = datetime(2026, 1, 15, 12, 30, 0)


class FakeWire:
    """
    Answers HTTP requests at the transport layer, for both ``httpx`` and
    ``httpx2`` (the OpenAI SDK >= 3 ships on httpx2, which respx cannot see).

    It lets tests drive the REAL vendor SDKs (openai, anthropic, google-genai)
    end to end - request building, auth headers, response parsing - without any
    network access, so an incompatible SDK upgrade fails a test instead of
    failing in production.

        wire = FakeWire(monkeypatch)
        wire.reply("POST", "/v1/embeddings", json={...})
        ...
        assert wire.requests[0].json["model"] == "text-embedding-3-small"
    """

    class Request:
        def __init__(self, method: str, url: str, headers: dict[str, str], body: bytes):
            self.method = method
            self.url = url
            self.headers = headers
            self.body = body

        @property
        def json(self) -> Any:
            return json.loads(self.body)

        @property
        def api_key(self) -> Optional[str]:
            """The credential sent, whichever header the SDK version uses for it
            (e.g. anthropic < 2 sends ``x-api-key``, >= 2 ``Authorization: Bearer``)."""
            if "x-api-key" in self.headers:
                return self.headers["x-api-key"]
            scheme, _, token = self.headers.get("authorization", "").partition(" ")
            return token if scheme.lower() == "bearer" else None

    def __init__(self, monkeypatch):
        self.requests: list[FakeWire.Request] = []
        self._routes: list[tuple[str, str, int, Any]] = []
        for module_name in ("httpx", "httpx2"):
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                continue
            self._install(monkeypatch, module)

    def reply(self, method: str, url_part: str, *, json: Any = None, status: int = 200) -> None:
        """Answer requests whose method matches and whose URL contains ``url_part``."""
        self._routes.append((method.upper(), url_part, status, json))

    def _respond(self, module, request):
        recorded = FakeWire.Request(
            request.method,
            str(request.url),
            {k.lower(): v for k, v in request.headers.items()},
            bytes(request.content),
        )
        self.requests.append(recorded)
        for method, url_part, status, payload in self._routes:
            if method == recorded.method and url_part in recorded.url:
                return module.Response(status, json=payload, request=request)
        raise AssertionError(f"FakeWire: unexpected request {recorded.method} {recorded.url}")

    def _install(self, monkeypatch, module) -> None:
        wire = self

        def handle_request(transport, request):
            request.read()
            return wire._respond(module, request)

        async def handle_async_request(transport, request):
            await request.aread()
            return wire._respond(module, request)

        monkeypatch.setattr(module.HTTPTransport, "handle_request", handle_request)
        monkeypatch.setattr(module.AsyncHTTPTransport, "handle_async_request", handle_async_request)


def make_file_info(
    path: str = "/docs/report.pdf",
    *,
    modified_at: datetime = DEFAULT_MTIME,
    size: Optional[int] = None,
    content_type: Optional[str] = None,
    name: Optional[str] = None,
) -> FileInfo:
    """Build a FileInfo; ``name`` defaults to the last segment of ``path``."""
    return FileInfo(
        name=name if name is not None else PurePosixPath(path.replace("\\", "/")).name,
        path=path,
        modified_at=modified_at,
        size=size,
        content_type=content_type,
    )


def _matches(path: str, extensions: Optional[Iterable[str]], folder_path: Optional[str]) -> bool:
    if extensions is not None:
        wanted = {e.lower() for e in extensions}
        if PurePosixPath(path).suffix.lower() not in wanted:
            return False
    if folder_path:
        prefix = "/" + folder_path.strip("/") + "/"
        if not path.startswith(prefix):
            return False
    return True


class _FakeBase:
    """Connection bookkeeping common to the fake source and destination."""

    def __init__(
        self,
        files: Optional[dict[str, bytes]] = None,
        *,
        connect_ok: bool = True,
        fail_download: Iterable[str] = (),
        list_error: Optional[Exception] = None,
    ):
        self.files: dict[str, bytes] = dict(files or {})
        self.connect_ok = connect_ok
        self.fail_download = set(fail_download)
        self.list_error = list_error
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.downloaded: list[str] = []

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.LOCAL

    async def connect(self) -> bool:
        self.connect_calls += 1
        self.connected = self.connect_ok
        return self.connect_ok

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
    ) -> list[FileInfo]:
        if self.list_error is not None:
            raise self.list_error
        return [
            make_file_info(path, size=len(content))
            for path, content in self.files.items()
            if _matches(path, extensions, folder_path)
        ]

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if file_info.path in self.fail_download or file_info.path not in self.files:
            return False
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(self.files[file_info.path])
        self.downloaded.append(file_info.path)
        return True


class FakeSource(_FakeBase, SourceIntegration):
    """In-memory source: ``FakeSource({"/docs/a.pdf": b"%PDF"})``."""


class FakeDestination(_FakeBase, DestinationIntegration):
    """In-memory destination recording uploads and created folders."""

    def __init__(self, *args: Any, fail_upload: Iterable[str] = (), **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.fail_upload = set(fail_upload)
        self.folders: list[str] = []
        self.uploaded: list[str] = []

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if remote_path in self.fail_upload:
            return False
        self.files[remote_path] = Path(local_path).read_bytes()
        self.uploaded.append(remote_path)
        return True

    async def create_folder(self, folder_path: str) -> bool:
        self.folders.append(folder_path)
        return True


class FakeVectorStore(VectorStoreIntegration):
    """In-memory vector store keyed by ``(namespace, doc_id)``."""

    def __init__(self, *, connect_ok: bool = True, query_error: Optional[Exception] = None):
        self.connect_ok = connect_ok
        self.query_error = query_error
        self.connected = False
        self.docs: dict[tuple[str, str], dict[str, Any]] = {}
        self.queries: list[dict[str, Any]] = []

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.CHROMA

    async def connect(self) -> bool:
        self.connected = self.connect_ok
        return self.connect_ok

    async def disconnect(self) -> None:
        self.connected = False

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        return {
            doc_id: DocumentRecord(
                doc_id=doc_id,
                source_hash=doc["source_hash"],
                embedding_model=doc["embedding_model"],
                chunking_version=doc["chunking_version"],
                chunk_count=len(doc["chunks"]),
            )
            for (ns, doc_id), doc in self.docs.items()
            if ns == namespace
        }

    async def upsert_document(
        self,
        namespace: str,
        doc_id: str,
        chunks: list[Chunk],
        source_hash: str,
        embedding_model: str,
        chunking_version: str,
    ) -> None:
        self.docs[(namespace, doc_id)] = {
            "chunks": list(chunks),
            "source_hash": source_hash,
            "embedding_model": embedding_model,
            "chunking_version": chunking_version,
        }

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        return self.docs.pop((namespace, doc_id), None) is not None

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        self.queries.append(
            {"namespace": namespace, "embedding": embedding, "k": k, "filter": filter}
        )
        if self.query_error is not None:
            raise self.query_error
        hits = [
            (chunk, 1.0 - index * 0.1)
            for (ns, _), doc in self.docs.items()
            if ns == namespace
            for index, chunk in enumerate(doc["chunks"])
        ]
        return hits[:k]
