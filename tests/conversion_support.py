"""
Support code shared by the converter and graph-extraction tests.

* ``FakeLLMServer`` - a loopback HTTP server speaking just enough of the
  OpenAI, Anthropic and Gemini REST APIs for the *real* SDK clients to complete
  a request against it. Tests then assert on the HTTP request the SDK actually
  produced, so an SDK upgrade that changes serialisation, auth headers or
  response parsing is caught. (The network guard in conftest allows loopback.)
* Builders for small real PDF / Office documents generated on the fly.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Union

import pymupdf

# --------------------------------------------------------------------------- #
# Fake LLM HTTP server
# --------------------------------------------------------------------------- #

_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
# Environment that could redirect or re-authenticate the SDK clients.
_SDK_VARS = (
    "OPENAI_API_KEY", "OPENAI_ADMIN_KEY", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID",
    "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
    "GOOGLE_API_KEY", "GEMINI_API_KEY", "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION",
)


@dataclass
class RecordedRequest:
    """One HTTP request received by the fake server."""
    path: str
    headers: dict[str, str]  # header names lower-cased
    json: dict[str, Any]

    @property
    def provider(self) -> str:
        return _provider_for(self.path)


def _provider_for(path: str) -> str:
    path = path.split("?", 1)[0]
    if path.endswith("/chat/completions"):
        return "openai"
    if path.endswith("/messages"):
        return "anthropic"
    if ":generateContent" in path:
        return "google"
    return "unknown"


def _envelope(provider: str, text: Optional[str]) -> dict[str, Any]:
    """Wrap ``text`` in a minimal, valid success response for ``provider``."""
    if provider == "openai":
        return {
            "id": "chatcmpl-test", "object": "chat.completion", "created": 0, "model": "test",
            "choices": [{
                "index": 0, "finish_reason": "stop",
                "message": {"role": "assistant", "content": text},
            }],
        }
    if provider == "anthropic":
        return {
            "id": "msg_test", "type": "message", "role": "assistant", "model": "test",
            "content": [] if text is None else [{"type": "text", "text": text}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
    if provider == "google":
        parts = [] if text is None else [{"text": text}]
        return {"candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}]}
    raise KeyError(provider)


_ERRORS = {
    "openai": {"error": {"message": "rejected", "type": "invalid_request_error", "code": "invalid_api_key"}},
    "anthropic": {"type": "error", "error": {"type": "authentication_error", "message": "rejected"}},
    "google": {"error": {"code": 400, "message": "rejected", "status": "INVALID_ARGUMENT"}},
}

Reply = Union[str, None, dict]


class FakeLLMServer:
    """Loopback stand-in for the OpenAI / Anthropic / Gemini HTTP APIs.

    ``reply("text")``            - every request is answered with that text
                                   (``None`` = a well-formed response with no text).
    ``reply_with(fn)``           - ``fn(request)`` returns the text (or a raw dict
                                   body) per request.
    ``fail(status)``             - every request is rejected with a provider-shaped
                                   error body. Use a non-retryable status (400/401)
                                   so the SDKs do not back off and retry.
    """

    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self._responder: Callable[[RecordedRequest], Reply] = lambda request: "ok"
        self._status = 200
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle --
    def __enter__(self) -> "FakeLLMServer":
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # keep test output quiet
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length)
                request = RecordedRequest(
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    json=json.loads(raw) if raw else {},
                )
                owner.requests.append(request)
                status, body = owner._respond(request)
                payload = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(
            target=lambda: self._httpd.serve_forever(poll_interval=0.01), daemon=True
        )
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        assert self._httpd is not None and self._thread is not None
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    # -- configuration --
    def activate(self, monkeypatch) -> "FakeLLMServer":
        """Reset state and point the SDKs' environment at this server."""
        self.requests.clear()
        self.reply("ok")
        for var in _PROXY_VARS + _SDK_VARS:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        # Anthropic and Gemini clients are built without a base_url by the code
        # under test; both SDKs honour these variables.
        monkeypatch.setenv("ANTHROPIC_BASE_URL", self.url)
        monkeypatch.setenv("GOOGLE_GEMINI_BASE_URL", self.url)
        # What an OpenAI client falls back to when no base_url is passed.
        monkeypatch.setenv("OPENAI_BASE_URL", self.openai_default_base_url)
        return self

    def reply(self, text: Reply) -> None:
        self._status = 200
        self._responder = lambda request: text

    def reply_with(self, responder: Callable[[RecordedRequest], Reply]) -> None:
        self._status = 200
        self._responder = responder

    def fail(self, status: int = 401) -> None:
        self._status = status

    def _respond(self, request: RecordedRequest) -> tuple[int, dict]:
        provider = request.provider
        if provider == "unknown":
            return 404, {"error": {"message": f"unexpected path {request.path}"}}
        if self._status != 200:
            return self._status, _ERRORS[provider]
        reply = self._responder(request)
        if isinstance(reply, dict):
            return 200, reply
        return 200, _envelope(provider, reply)

    # -- addresses --
    @property
    def url(self) -> str:
        assert self._httpd is not None
        return f"http://127.0.0.1:{self._httpd.server_address[1]}"

    @property
    def openai_base_url(self) -> str:
        """Value to put in ``llm_base_url`` for OpenAI-compatible providers."""
        return f"{self.url}/v1"

    @property
    def openai_default_base_url(self) -> str:
        """Where an OpenAI client lands when the code passes no base_url."""
        return f"{self.url}/sdk-default/v1"

    @property
    def last(self) -> RecordedRequest:
        assert self.requests, "the SDK never called the fake LLM server"
        return self.requests[-1]


# --------------------------------------------------------------------------- #
# Real documents generated on the fly
# --------------------------------------------------------------------------- #

DENSE_TEXT = "Lorem ipsum dolor sit amet consectetur. " * 20  # 800 chars: not "sparse"


def png_bytes(shade: int = 200, size: int = 16) -> bytes:
    """A tiny valid PNG; different shades give different image objects."""
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, size, size))
    pix.clear_with(shade)
    return pix.tobytes("png")


@dataclass
class PageSpec:
    """What to draw on one PDF page."""
    text: str = ""
    images: int = 0
    table: bool = False          # ruled grid: 4 horizontal + 3 vertical long lines
    strokes: int = 0             # short diagonal strokes (vector noise, never a table)
    lines: Sequence[tuple[tuple[float, float], tuple[float, float]]] = field(default_factory=tuple)


def make_pdf(path: Path, pages: Sequence[PageSpec]) -> Path:
    """Write a real PDF with PyMuPDF according to ``pages``."""
    doc = pymupdf.open()
    for spec in pages:
        page = doc.new_page()
        if spec.text:
            leftover = page.insert_textbox(pymupdf.Rect(40, 400, 560, 820), spec.text, fontsize=8)
            assert leftover >= 0, "text does not fit on the page"
        for index in range(spec.images):
            x = 40 + 60 * index
            page.insert_image(pymupdf.Rect(x, 40, x + 50, 90), stream=png_bytes(shade=40 + 20 * index))
        if spec.table:
            for y in (120, 150, 180, 210):
                page.draw_line((100, y), (400, y))
            for x in (100, 250, 400):
                page.draw_line((x, 120), (x, 210))
        for index in range(spec.strokes):
            x = 40 + (index % 50) * 10
            y = 240 + (index // 50) * 12
            page.draw_line((x, y), (x + 5, y + 5))
        for start, end in spec.lines:
            page.draw_line(start, end)
    path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(path))
    doc.close()
    return path


def pdf_bytes(tmp_dir: Path, pages: Sequence[PageSpec], name: str = "_build.pdf") -> bytes:
    path = make_pdf(tmp_dir / name, pages)
    data = path.read_bytes()
    path.unlink()
    return data


# A syntactically valid PDF whose page tree is empty (PyMuPDF refuses to *save*
# a zero-page document, so it has to be written by hand).
ZERO_PAGE_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    b"2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n"
    b"trailer\n<< /Root 1 0 R /Size 3 >>\n%%EOF\n"
)


def make_docx(path: Path) -> Path:
    import docx

    document = docx.Document()
    document.add_heading("Quarterly Report", level=1)
    document.add_paragraph("Revenue grew by ten percent.")
    table = document.add_table(rows=2, cols=2)
    table.cell(0, 0).text = "Region"
    table.cell(0, 1).text = "Sales"
    table.cell(1, 0).text = "EMEA"
    table.cell(1, 1).text = "42"
    document.save(str(path))
    return path


def make_xlsx(path: Path) -> Path:
    import openpyxl

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["item", "price"])
    sheet.append(["apple", 1.5])
    sheet.append(["pear", 2])
    workbook.save(str(path))
    return path


def make_pptx(path: Path) -> Path:
    from pptx import Presentation

    presentation = Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[0])
    slide.shapes.title.text = "Deck Title"
    slide.placeholders[1].text = "Subtitle text"
    presentation.save(str(path))
    return path
