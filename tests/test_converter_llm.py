"""
Tests for the LLM conversion back-ends and the job-level ``ConversionService``.

The real openai / anthropic / google-genai SDKs are driven end to end through
``FakeWire`` (answers at the HTTP transport layer), so request building, auth
headers and response parsing are exercised without any network access.

``ConversionService`` runs against the in-memory ``FakeSource`` /
``FakeDestination`` with the engine replaced by a recording stub; the real
``Converter`` is covered in ``test_converter.py``.
"""
import base64
import logging
import sys
import tempfile
import threading
import time
from pathlib import Path, PurePath
from unittest.mock import MagicMock

import pytest

import src.converter as converter_module
from src.converter import (
    LLM_CONVERSION_PROMPT,
    ConversionService,
    Converter,
    _convert_with_llm,
    _llm_anthropic,
    _llm_google,
    _llm_openai_compat,
)
from src.integration.base import MARKDOWN_EXTENSIONS, IntegrationType
from src.models import (
    ConversionJob,
    ConversionStrategy,
    FileConversionStatus,
    IntegrationConfig,
    JobLLMSettings,
    JobStatus,
)
from tests.helpers import FakeDestination, FakeSource, FakeWire

WINDOWS = sys.platform == "win32"
FAST = ConversionStrategy.FAST
ACCURATE = ConversionStrategy.ACCURATE

FILE_BYTES = b"%PDF-1.4 fake document bytes"
FILE_B64 = base64.b64encode(FILE_BYTES).decode()

# Environment that could redirect or re-authenticate the SDK clients.
_SDK_ENV = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
    "OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID",
    "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL",
    "GOOGLE_API_KEY", "GEMINI_API_KEY", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION",
)


# --------------------------------------------------------------------------- #
# Wire helpers
# --------------------------------------------------------------------------- #

def openai_body(text):
    return {
        "id": "chatcmpl-test", "object": "chat.completion", "created": 0, "model": "test",
        "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": text}}],
    }


def anthropic_body(text):
    return {
        "id": "msg_test", "type": "message", "role": "assistant", "model": "test",
        "content": [] if text is None else [{"type": "text", "text": text}],
        "stop_reason": "end_turn", "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


def google_body(text):
    parts = [] if text is None else [{"text": text}]
    return {"candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}]}


@pytest.fixture
def wire(monkeypatch) -> FakeWire:
    for var in _SDK_ENV:
        monkeypatch.delenv(var, raising=False)
    return FakeWire(monkeypatch)


@pytest.fixture
def anthropic_explicit_timeout(monkeypatch):
    """Build Anthropic clients with an explicit timeout.

    With the SDK's default timeout, ``messages.create(max_tokens=200_000)`` is
    refused before any request is sent (see the xfail test below). An explicit
    timeout bypasses that guard so the request/response handling of
    ``_llm_anthropic`` can still be exercised with the real SDK.
    """
    import anthropic

    class ExplicitTimeoutAnthropic(anthropic.Anthropic):
        def __init__(self, **kwargs):
            super().__init__(timeout=600.0, **kwargs)

    monkeypatch.setattr(anthropic, "Anthropic", ExplicitTimeoutAnthropic)


def llm_settings(settings, **overrides):
    return settings.model_copy(update=overrides)


# --------------------------------------------------------------------------- #
# _convert_with_llm dispatch
# --------------------------------------------------------------------------- #

class TestLLMDispatch:
    @pytest.fixture
    def backends(self, monkeypatch):
        calls = []

        def record(name):
            def backend(*args):
                calls.append((name, args))
                return f"from {name}"
            return backend

        monkeypatch.setattr(converter_module, "_llm_openai_compat", record("openai_compat"))
        monkeypatch.setattr(converter_module, "_llm_anthropic", record("anthropic"))
        monkeypatch.setattr(converter_module, "_llm_google", record("google"))
        return calls

    @pytest.mark.parametrize(
        "provider, backend",
        [("openai", "openai_compat"), ("ollama", "openai_compat"), ("OpenAI", "openai_compat"),
         ("anthropic", "anthropic"), ("Google", "google")],
    )
    def test_provider_selects_the_backend(self, backends, settings, tmp_path, provider, backend):
        path = tmp_path / "scan.png"
        path.write_bytes(FILE_BYTES)
        effective = llm_settings(settings, llm_provider=provider)

        assert _convert_with_llm(path, effective) == f"from {backend}"
        [(name, args)] = backends
        assert name == backend
        assert args[0] == FILE_BYTES
        assert args[-1] is effective

    @pytest.mark.parametrize(
        "name, mime",
        [("a.pdf", "application/pdf"), ("a.PNG", "image/png"), ("a.jpg", "image/jpeg"),
         ("a.jpeg", "image/jpeg"), ("a.gif", "image/gif"), ("a.bmp", "image/bmp"),
         ("a.tiff", "image/tiff"), ("a.webp", "application/octet-stream")],
    )
    def test_mime_type_follows_the_extension(self, backends, settings, tmp_path, name, mime):
        path = tmp_path / name
        path.write_bytes(FILE_BYTES)

        _convert_with_llm(path, llm_settings(settings, llm_provider="google"))

        [(_, (_, sent_mime, _))] = backends
        assert sent_mime == mime

    def test_anthropic_also_receives_the_lowercased_extension(self, backends, settings, tmp_path):
        path = tmp_path / "a.PDF"
        path.write_bytes(FILE_BYTES)

        _convert_with_llm(path, llm_settings(settings, llm_provider="anthropic"))

        [(_, (_, mime, ext, _))] = backends
        assert (mime, ext) == ("application/pdf", ".pdf")

    def test_unsupported_provider_returns_none(self, backends, settings, tmp_path, caplog):
        path = tmp_path / "a.png"
        path.write_bytes(FILE_BYTES)

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert _convert_with_llm(path, llm_settings(settings, llm_provider="mistral")) is None

        assert backends == []
        assert "Unsupported LLM provider: mistral" in caplog.text

    def test_unreadable_file_returns_none(self, backends, settings, tmp_path, caplog):
        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert _convert_with_llm(tmp_path / "missing.png", settings) is None

        assert backends == []
        assert "LLM conversion error" in caplog.text

    def test_backend_errors_are_swallowed(self, settings, tmp_path, monkeypatch, caplog):
        def boom(*args):
            raise RuntimeError("model overloaded")

        monkeypatch.setattr(converter_module, "_llm_openai_compat", boom)
        path = tmp_path / "a.png"
        path.write_bytes(FILE_BYTES)

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert _convert_with_llm(path, settings) is None

        assert "model overloaded" in caplog.text


# --------------------------------------------------------------------------- #
# OpenAI / Ollama
# --------------------------------------------------------------------------- #

class TestOpenAICompatible:
    def test_openai_pdf_is_sent_as_a_file_part(self, wire, settings):
        wire.reply("POST", "/chat/completions", json=openai_body("  # Report\n\nBody  \n"))
        effective = llm_settings(settings, llm_provider="openai", llm_api_key="sk-test",
                                 llm_model="gpt-vision", llm_base_url="https://llm.example/v1")

        result = _llm_openai_compat(FILE_BYTES, "application/pdf", effective)

        assert result == "# Report\n\nBody"
        [request] = wire.requests
        assert request.url == "https://llm.example/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer sk-test"
        assert request.json["model"] == "gpt-vision"
        assert request.json["messages"] == [{
            "role": "user",
            "content": [
                {"type": "file", "file": {"filename": "document.pdf",
                                          "file_data": f"data:application/pdf;base64,{FILE_B64}"}},
                {"type": "text", "text": LLM_CONVERSION_PROMPT},
            ],
        }]

    def test_ollama_pdf_is_sent_as_an_image_url_with_a_placeholder_key(self, wire, settings):
        wire.reply("POST", "/chat/completions", json=openai_body("# Local"))
        effective = llm_settings(settings, llm_provider="ollama", llm_api_key="",
                                 llm_model="llava", llm_base_url="http://ollama.local:11434/v1")

        assert _llm_openai_compat(FILE_BYTES, "application/pdf", effective) == "# Local"

        [request] = wire.requests
        assert request.url == "http://ollama.local:11434/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer ollama"
        assert request.json["messages"][0]["content"][0] == {
            "type": "image_url", "image_url": {"url": f"data:application/pdf;base64,{FILE_B64}"},
        }

    @pytest.mark.parametrize("provider", ["openai", "ollama"])
    def test_images_are_sent_as_image_urls(self, wire, settings, provider):
        wire.reply("POST", "/chat/completions", json=openai_body("# Image"))
        effective = llm_settings(settings, llm_provider=provider, llm_api_key="k")

        _llm_openai_compat(FILE_BYTES, "image/png", effective)

        assert wire.requests[0].json["messages"][0]["content"] == [
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{FILE_B64}"}},
            {"type": "text", "text": LLM_CONVERSION_PROMPT},
        ]

    def test_blank_base_url_uses_the_sdk_default_endpoint(self, wire, settings):
        wire.reply("POST", "/chat/completions", json=openai_body("ok"))

        _llm_openai_compat(FILE_BYTES, "image/png", llm_settings(settings, llm_api_key="k", llm_base_url=""))

        assert wire.requests[0].url == "https://api.openai.com/v1/chat/completions"

    def test_empty_answer_is_none(self, wire, settings):
        wire.reply("POST", "/chat/completions", json=openai_body(None))

        assert _llm_openai_compat(FILE_BYTES, "image/png", llm_settings(settings, llm_api_key="k")) is None

    def test_api_error_makes_the_conversion_fail(self, wire, settings, tmp_path, caplog):
        wire.reply("POST", "/chat/completions", status=401, json={
            "error": {"message": "bad key", "type": "invalid_request_error", "code": "invalid_api_key"},
        })
        path = tmp_path / "a.png"
        path.write_bytes(FILE_BYTES)

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert _convert_with_llm(path, llm_settings(settings, llm_api_key="k")) is None

        assert len(wire.requests) == 1
        assert "LLM conversion error" in caplog.text


# --------------------------------------------------------------------------- #
# Anthropic
# --------------------------------------------------------------------------- #

class TestAnthropic:
    @pytest.fixture
    def effective(self, settings):
        return llm_settings(settings, llm_provider="anthropic", llm_api_key="sk-ant", llm_model="claude-test")

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: converter.py _llm_anthropic calls messages.create(max_tokens=200_000) without "
        "streaming; the Anthropic SDK refuses it ('Streaming is required ...'), so every "
        "Anthropic conversion fails",
    )
    def test_conversion_with_the_stock_client_succeeds(self, wire, effective, tmp_path):
        wire.reply("POST", "/v1/messages", json=anthropic_body("# Converted"))
        path = tmp_path / "a.pdf"
        path.write_bytes(FILE_BYTES)

        assert _convert_with_llm(path, effective) == "# Converted"

    @pytest.mark.parametrize(
        "ext, mime, block_type",
        [(".pdf", "application/pdf", "document"), (".png", "image/png", "image")],
    )
    def test_request_carries_the_file_and_the_prompt(
        self, wire, anthropic_explicit_timeout, effective, ext, mime, block_type
    ):
        wire.reply("POST", "/v1/messages", json=anthropic_body("\n# Converted\n"))

        result = _llm_anthropic(FILE_BYTES, mime, ext, effective)

        assert result == "# Converted"
        [request] = wire.requests
        assert request.url == "https://api.anthropic.com/v1/messages"
        assert request.api_key == "sk-ant"
        assert request.json["model"] == "claude-test"
        assert request.json["max_tokens"] == 200_000
        assert request.json["messages"] == [{
            "role": "user",
            "content": [
                {"type": block_type, "source": {"type": "base64", "media_type": mime, "data": FILE_B64}},
                {"type": "text", "text": LLM_CONVERSION_PROMPT},
            ],
        }]

    def test_empty_answer_is_none(self, wire, anthropic_explicit_timeout, effective):
        wire.reply("POST", "/v1/messages", json=anthropic_body(None))

        assert _llm_anthropic(FILE_BYTES, "image/png", ".png", effective) is None


# --------------------------------------------------------------------------- #
# Google Gemini
# --------------------------------------------------------------------------- #

class TestGoogle:
    @pytest.fixture
    def effective(self, settings):
        return llm_settings(settings, llm_provider="google", llm_api_key="g-key", llm_model="gemini-test")

    def test_request_carries_the_file_and_the_prompt(self, wire, effective):
        wire.reply("POST", ":generateContent", json=google_body("  # Gemini  "))

        assert _llm_google(FILE_BYTES, "image/jpeg", effective) == "# Gemini"

        [request] = wire.requests
        assert "/models/gemini-test:generateContent" in request.url
        assert request.headers["x-goog-api-key"] == "g-key"
        [content] = request.json["contents"]
        inline, prompt = content["parts"]
        inline = inline.get("inlineData") or inline.get("inline_data")
        assert base64.b64decode(inline["data"]) == FILE_BYTES
        assert (inline.get("mimeType") or inline.get("mime_type")) == "image/jpeg"
        assert prompt == {"text": LLM_CONVERSION_PROMPT}

    def test_empty_answer_is_none(self, wire, effective):
        wire.reply("POST", ":generateContent", json=google_body(None))

        assert _llm_google(FILE_BYTES, "application/pdf", effective) is None


# --------------------------------------------------------------------------- #
# Converter -> real SDK
# --------------------------------------------------------------------------- #

class TestAccurateStrategyThroughTheSDK:
    @pytest.fixture
    def docling(self, monkeypatch):
        mock = MagicMock(name="docling")
        mock.convert.return_value.document.export_to_markdown.return_value = "DOCLING MARKDOWN"
        monkeypatch.setattr(Converter, "_doc_converter", mock)
        return mock

    def test_image_is_converted_with_the_job_llm(self, wire, settings, docling, tmp_path):
        wire.reply("POST", ":generateContent", json=google_body("# From Gemini"))
        image = tmp_path / "scan.png"
        image.write_bytes(FILE_BYTES)
        job_llm = JobLLMSettings(llm_provider="google", llm_model="gemini-job", llm_api_key="job-key")

        assert Converter(settings).convert(image, ACCURATE, job_llm) == "# From Gemini"

        [request] = wire.requests
        assert "/models/gemini-job:generateContent" in request.url
        assert request.headers["x-goog-api-key"] == "job-key"
        docling.convert.assert_not_called()

    def test_llm_api_failure_falls_back_to_docling(self, wire, settings, docling, tmp_path):
        wire.reply("POST", "/chat/completions", status=400, json={
            "error": {"message": "unsupported image", "type": "invalid_request_error", "code": None},
        })
        image = tmp_path / "scan.png"
        image.write_bytes(FILE_BYTES)

        result = Converter(llm_settings(settings, llm_api_key="k")).convert(image, ACCURATE)

        assert result == "DOCLING MARKDOWN"
        assert len(wire.requests) == 1
        docling.convert.assert_called_once_with(str(image), raises_on_error=True)


# --------------------------------------------------------------------------- #
# ConversionService
# --------------------------------------------------------------------------- #

class StubConverter:
    """Records ``convert`` calls; returns ``"# <stem>"`` unless configured otherwise."""

    def __init__(self):
        self.calls: list[tuple[Path, ConversionStrategy, object]] = []
        self.results: dict[str, object] = {}
        self.delay = 0.0
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def convert(self, input_path, strategy=FAST, job_llm=None):
        with self._lock:
            self.calls.append((Path(input_path), strategy, job_llm))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            result = self.results.get(Path(input_path).name, f"# {Path(input_path).stem}")
            if isinstance(result, Exception):
                raise result
            return result
        finally:
            with self._lock:
                self.active -= 1


@pytest.fixture
def system_temp(tmp_path, monkeypatch) -> Path:
    """Keep ``tempfile.mkdtemp`` fallbacks inside the test's tmp_path."""
    path = tmp_path / "system-temp"
    path.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


@pytest.fixture
def engine() -> StubConverter:
    return StubConverter()


@pytest.fixture
def service(settings, engine, system_temp, tmp_path, monkeypatch) -> ConversionService:
    monkeypatch.setattr(Converter, "_doc_converter", MagicMock(name="docling"))
    svc = ConversionService(settings)
    monkeypatch.setattr(svc, "_converter", engine)

    # Never let a conversion write outside the test sandbox (see the Windows
    # path bug below): refuse such outputs instead of writing them.
    real_convert_file = svc.convert_file
    sandbox = tmp_path.resolve()

    def sandboxed_convert_file(input_path, output_path, *args):
        if not Path(output_path).resolve().is_relative_to(sandbox):
            return False
        return real_convert_file(input_path, output_path, *args)

    monkeypatch.setattr(svc, "convert_file", sandboxed_convert_file)
    return svc


def conversion_job(**overrides) -> ConversionJob:
    fields = dict(
        name="docs",
        source=IntegrationConfig(type=IntegrationType.LOCAL),
        destination=IntegrationConfig(type=IntegrationType.LOCAL),
        source_extensions=[".pdf", ".docx"],
    )
    fields.update(overrides)
    return ConversionJob(**fields)


def results_by_path(job: ConversionJob) -> dict:
    return {r.source_path: r for r in job.file_results}


def posix(path: str) -> str:
    return PurePath(path).as_posix()


windows_run_path_bug = pytest.mark.xfail(
    WINDOWS,
    strict=True,
    reason="BUG: converter.py run_conversion does str(Path(p).parent).lstrip('/'), which is "
    "'\\\\docs' on Windows: joined onto the temp dir it escapes to the drive root, and "
    "destination paths are built with backslashes",
)
windows_single_path_bug = pytest.mark.xfail(
    WINDOWS,
    strict=True,
    reason="BUG: converter.py convert_single_file compares str(Path.parent) with '/' and builds "
    "the destination with str(Path), yielding backslash paths on Windows",
)


class TestServiceTempDir:
    def test_existing_configured_directory_is_used(self, service, settings):
        Path(settings.temp_dir).mkdir(parents=True)

        assert service._ensure_temp_dir() == Path(settings.temp_dir)

    def test_missing_configured_directory_falls_back_to_a_fresh_temp_dir(self, service, system_temp):
        temp = service._ensure_temp_dir()

        assert temp.is_dir()
        assert temp.parent == system_temp
        assert temp.name.startswith("converter_")
        assert service._ensure_temp_dir() == temp

    def test_cleanup_removes_the_directory_once(self, service):
        temp = service._ensure_temp_dir()
        (temp / "x.md").write_text("x", encoding="utf-8")

        service.cleanup()
        service.cleanup()

        assert not temp.exists()
        assert service._temp_dir is None

    def test_cleanup_of_a_never_created_directory_does_nothing(self, service, settings):
        service.cleanup()

        assert service._temp_dir == Path(settings.temp_dir)
        assert not Path(settings.temp_dir).exists()


class TestAnalyzeConversion:
    async def test_sources_are_matched_to_markdown_by_stem(self, service):
        source = FakeSource({"/in/Report.PDF": b"12345", "/in/deck.docx": b"1", "/in/notes.docx": b"12"})
        destination = FakeDestination({"/out/report.md": b"#", "/out/deck.txt": b"#", "/elsewhere/notes.md": b"#"})

        analysis = await service.analyze_conversion(
            source, destination, [".pdf", ".docx"], source_folder="in", destination_folder="out"
        )

        assert [f["path"] for f in analysis.already_converted] == ["/in/Report.PDF"]
        assert [f["path"] for f in analysis.files_to_convert] == ["/in/deck.docx", "/in/notes.docx"]
        assert [f["path"] for f in analysis.destination_files] == ["/out/report.md"]
        assert analysis.source_files[0] == {
            "name": "Report.PDF", "path": "/in/Report.PDF",
            "modified_at": analysis.source_files[0]["modified_at"], "size": 5,
        }
        assert analysis.total_source_files == 3
        assert analysis.total_converted_files == 1
        assert analysis.completion_percentage == 33.33

    async def test_only_requested_source_extensions_and_markdown_destinations_are_listed(self, service):
        calls = []

        class SpySource(FakeSource):
            async def list_files(self, extensions=None, folder_path=None):
                calls.append(("source", extensions, folder_path))
                return await super().list_files(extensions, folder_path)

        class SpyDestination(FakeDestination):
            async def list_files(self, extensions=None, folder_path=None):
                calls.append(("destination", extensions, folder_path))
                return await super().list_files(extensions, folder_path)

        await service.analyze_conversion(SpySource(), SpyDestination(), [".pdf"], "in", "out")

        assert calls == [("source", [".pdf"], "in"), ("destination", MARKDOWN_EXTENSIONS, "out")]

    async def test_empty_source_is_fully_converted(self, service):
        analysis = await service.analyze_conversion(FakeSource(), FakeDestination({"/a.md": b"#"}), [".pdf"])

        assert analysis.completion_percentage == 100.0
        assert analysis.total_source_files == 0


class TestConvertFile:
    def test_markdown_is_written_creating_parent_folders(self, service, engine, tmp_path):
        source = tmp_path / "a.pdf"
        output = tmp_path / "out" / "deep" / "a.md"
        job_llm = JobLLMSettings(llm_model="m")

        assert service.convert_file(source, output, ACCURATE, job_llm) is True

        assert output.read_text(encoding="utf-8") == "# a"
        assert engine.calls == [(source, ACCURATE, job_llm)]

    @pytest.mark.parametrize("empty", [None, ""])
    def test_no_content_is_a_failure(self, service, engine, tmp_path, caplog, empty):
        engine.results["a.pdf"] = empty
        output = tmp_path / "a.md"

        with caplog.at_level(logging.WARNING, logger="src.converter"):
            assert service.convert_file(tmp_path / "a.pdf", output) is False

        assert not output.exists()
        assert "No content extracted" in caplog.text

    def test_engine_errors_are_a_failure(self, service, engine, tmp_path, caplog):
        engine.results["a.pdf"] = RuntimeError("engine crashed")

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert service.convert_file(tmp_path / "a.pdf", tmp_path / "a.md") is False

        assert "engine crashed" in caplog.text

    async def test_async_wrapper_returns_the_same_outcome(self, service, engine, tmp_path):
        output = tmp_path / "a.md"

        assert await service.convert_file_async(tmp_path / "a.pdf", output, FAST) is True
        assert output.read_text(encoding="utf-8") == "# a"

        engine.results["b.pdf"] = None
        assert await service.convert_file_async(tmp_path / "b.pdf", tmp_path / "b.md") is False


class TestRunConversion:
    async def test_files_are_downloaded_converted_and_uploaded(self, service, engine, system_temp):
        source = FakeSource({"docs/a.pdf": b"pdf-bytes", "b.docx": b"docx"})
        destination = FakeDestination()
        job = conversion_job(destination_folder="out")

        returned = await service.run_conversion(job, source, destination)

        assert returned is job
        assert job.status == JobStatus.COMPLETED
        assert job.started_at is not None and job.completed_at is not None
        results = results_by_path(job)
        assert {r.status for r in results.values()} == {FileConversionStatus.COMPLETED}
        assert posix(results["docs/a.pdf"].destination_path) == "out/docs/a.md"
        assert posix(results["b.docx"].destination_path) == "out/b.md"
        assert results["docs/a.pdf"].file_size == len(b"pdf-bytes")
        assert results["docs/a.pdf"].converted_size == len(b"# a")
        assert all(r.completed_at is not None for r in results.values())
        assert {posix(p): content for p, content in destination.files.items()} == {
            "out/docs/a.md": b"# a", "out/b.md": b"# b",
        }
        assert sorted(source.downloaded) == ["b.docx", "docs/a.pdf"]
        assert job.stats.completed_files == 2
        assert job.stats.completion_percentage == 100.0
        assert list(system_temp.iterdir()) == []

    async def test_without_destination_folder_the_source_layout_is_mirrored(self, service):
        destination = FakeDestination()
        job = conversion_job()

        await service.run_conversion(job, FakeSource({"docs/a.pdf": b"x", "b.docx": b"y"}), destination)

        results = results_by_path(job)
        assert posix(results["docs/a.pdf"].destination_path) == "docs/a.md"
        assert posix(results["b.docx"].destination_path) == "b.md"

    @windows_run_path_bug
    async def test_absolute_source_paths_are_converted_inside_the_temp_dir(self, service):
        destination = FakeDestination()
        job = conversion_job(destination_folder="out")

        await service.run_conversion(job, FakeSource({"/docs/a.pdf": b"x"}), destination)

        [result] = job.file_results
        assert result.status == FileConversionStatus.COMPLETED
        assert result.destination_path == "out/docs/a.md"

    @windows_run_path_bug
    async def test_destination_paths_use_forward_slashes(self, service):
        job = conversion_job(destination_folder="out")

        await service.run_conversion(job, FakeSource({"docs/a.pdf": b"x"}), FakeDestination())

        assert job.file_results[0].destination_path == "out/docs/a.md"

    async def test_already_converted_files_are_skipped(self, service, engine):
        source = FakeSource({"/a.pdf": b"x", "b.docx": b"y"})
        destination = FakeDestination({"/out/A.md": b"old"})
        job = conversion_job(destination_folder="out")

        await service.run_conversion(job, source, destination)

        results = results_by_path(job)
        assert results["/a.pdf"].status == FileConversionStatus.SKIPPED
        assert results["/a.pdf"].file_size == 1
        assert results["b.docx"].status == FileConversionStatus.COMPLETED
        assert source.downloaded == ["b.docx"]
        assert [path.name for path, _, _ in engine.calls] == ["b.docx"]
        assert job.stats.skipped_files == 1
        assert job.stats.completed_files == 2

    @pytest.mark.parametrize(
        "setup, message",
        [
            (lambda src, dst, eng: src.fail_download.add("a.pdf"), "Download failed"),
            (lambda src, dst, eng: eng.results.update({"a.pdf": None}), "Conversion failed"),
            (lambda src, dst, eng: dst.fail_upload.add("a.md"), "Upload failed"),
        ],
        ids=["download", "conversion", "upload"],
    )
    async def test_a_failing_step_fails_only_that_file(self, service, engine, setup, message):
        source = FakeSource({"a.pdf": b"x", "b.pdf": b"y"})
        destination = FakeDestination()
        setup(source, destination, engine)
        job = conversion_job()

        await service.run_conversion(job, source, destination)

        results = results_by_path(job)
        assert results["a.pdf"].status == FileConversionStatus.FAILED
        assert results["a.pdf"].error_message == message
        assert results["a.pdf"].destination_path is None
        assert results["b.pdf"].status == FileConversionStatus.COMPLETED
        assert job.status == JobStatus.COMPLETED
        assert job.stats.failed_files == 1

    async def test_unexpected_errors_fail_the_file_not_the_job(self, service, caplog):
        class BrokenDestination(FakeDestination):
            async def upload_file(self, local_path, remote_path):
                raise OSError("disk full")

        job = conversion_job()

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            await service.run_conversion(job, FakeSource({"a.pdf": b"x"}), BrokenDestination())

        [result] = job.file_results
        assert result.status == FileConversionStatus.FAILED
        assert result.error_message == "disk full"
        assert job.status == JobStatus.COMPLETED
        assert "Error processing a.pdf: disk full" in caplog.text

    async def test_listing_failure_fails_the_job_and_cleans_up(self, service, settings, caplog):
        Path(settings.temp_dir).mkdir(parents=True)
        source = FakeSource(list_error=ConnectionError("source offline"))
        job = conversion_job()

        with caplog.at_level(logging.ERROR, logger="src.converter"):
            await service.run_conversion(job, source, FakeDestination())

        assert job.status == JobStatus.FAILED
        assert job.error_message == "source offline"
        assert job.completed_at is None
        assert "Job failed: source offline" in caplog.text
        assert not Path(settings.temp_dir).exists()

    async def test_strategy_and_llm_overrides_reach_the_engine(self, service, engine):
        job_llm = JobLLMSettings(llm_provider="anthropic")
        job = conversion_job(conversion_strategy=ACCURATE, llm_settings=job_llm)
        assert job.conversion_strategy == "accurate"  # stored as its value

        await service.run_conversion(job, FakeSource({"a.pdf": b"x"}), FakeDestination())

        [(_, strategy, received_llm)] = engine.calls
        assert strategy is ACCURATE
        assert received_llm == job_llm

    async def test_strategy_assigned_as_an_enum_is_accepted(self, service, engine):
        job = conversion_job()
        job.conversion_strategy = ConversionStrategy.BALANCED

        await service.run_conversion(job, FakeSource({"a.pdf": b"x"}), FakeDestination())

        assert engine.calls[0][1] is ConversionStrategy.BALANCED

    async def test_progress_is_reported_for_every_converted_file(self, service):
        source = FakeSource({"a.pdf": b"x", "b.pdf": b"y", "/c.pdf": b"z"})
        destination = FakeDestination({"/c.md": b"#"})
        calls = []

        async def progress(job, processed, total):
            calls.append((job, processed, total))

        job = conversion_job()
        await service.run_conversion(job, source, destination, progress_callback=progress)

        assert len(calls) == 2
        assert all(reported is job and total == 3 for reported, _, total in calls)
        assert max(processed for _, processed, _ in calls) == 3

    async def test_batch_size_one_converts_sequentially(self, service, engine):
        engine.delay = 0.02
        job = conversion_job(batch_size=1)

        await service.run_conversion(job, FakeSource({f"{n}.pdf": b"x" for n in "abc"}), FakeDestination())

        assert len(engine.calls) == 3
        assert engine.max_active == 1


class TestConvertSingleFile:
    async def test_file_is_converted_and_uploaded(self, service, engine, system_temp):
        source = FakeSource({"docs/a.pdf": b"pdf", "docs/other.pdf": b"x"})
        destination = FakeDestination()

        result = await service.convert_single_file(source, destination, "docs/a.pdf", "out", ACCURATE)

        assert result.status == FileConversionStatus.COMPLETED
        assert posix(result.destination_path) == "out/docs/a.md"
        assert result.file_size == 3
        assert result.converted_size == len(b"# a")
        assert result.started_at is not None and result.completed_at is not None
        assert {posix(p): c for p, c in destination.files.items()} == {"out/docs/a.md": b"# a"}
        assert source.downloaded == ["docs/a.pdf"]
        assert [(path.name, strategy) for path, strategy, _ in engine.calls] == [("a.pdf", ACCURATE)]
        assert list(system_temp.iterdir()) == []

    async def test_default_strategy_is_fast_and_layout_is_kept_without_folder(self, service, engine):
        result = await service.convert_single_file(FakeSource({"docs/a.pdf": b"x"}), FakeDestination(), "docs/a.pdf")

        assert posix(result.destination_path) == "docs/a.md"
        assert engine.calls[0][1] is FAST

    @windows_single_path_bug
    async def test_root_file_goes_straight_into_the_destination_folder(self, service):
        result = await service.convert_single_file(FakeSource({"/a.pdf": b"x"}), FakeDestination(), "/a.pdf", "out")

        assert result.destination_path == "out/a.md"

    @windows_single_path_bug
    async def test_absolute_source_path_is_mirrored_with_forward_slashes(self, service):
        result = await service.convert_single_file(
            FakeSource({"/docs/a.pdf": b"x"}), FakeDestination(), "/docs/a.pdf"
        )

        assert result.destination_path == "/docs/a.md"

    async def test_unknown_file_fails_without_downloading(self, service, engine):
        source = FakeSource({"a.pdf": b"x"})

        result = await service.convert_single_file(source, FakeDestination(), "missing.pdf")

        assert result.status == FileConversionStatus.FAILED
        assert result.error_message == "File not found: missing.pdf"
        assert source.downloaded == [] and engine.calls == []

    @pytest.mark.parametrize(
        "setup, message",
        [
            (lambda src, dst, eng: src.fail_download.add("a.pdf"), "Download failed"),
            (lambda src, dst, eng: eng.results.update({"a.pdf": None}), "Conversion failed"),
            (lambda src, dst, eng: dst.fail_upload.add("a.md"), "Upload failed"),
        ],
        ids=["download", "conversion", "upload"],
    )
    async def test_a_failing_step_is_reported(self, service, engine, settings, setup, message):
        source = FakeSource({"a.pdf": b"x"})
        destination = FakeDestination()
        setup(source, destination, engine)

        result = await service.convert_single_file(source, destination, "a.pdf")

        assert result.status == FileConversionStatus.FAILED
        assert result.error_message == message
        assert result.destination_path is None
        assert service._temp_dir is None

    async def test_unexpected_errors_are_reported(self, service):
        source = FakeSource(list_error=TimeoutError("listing timed out"))

        result = await service.convert_single_file(source, FakeDestination(), "a.pdf")

        assert result.status == FileConversionStatus.FAILED
        assert result.error_message == "listing timed out"
        assert service._temp_dir is None
