"""
Tests for the conversion engines and the strategy-aware ``Converter``.

PyMuPDF and MarkItDown run for real on documents generated in ``tmp_path``.
Docling is always a mock (the real pipeline loads ML models); the mock is
auto-specced against Docling's classes so a signature change is still caught.

LLM back-ends are covered in ``test_converter_llm.py`` and the job-level
``ConversionService`` in ``test_converter_service.py``.
"""
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from docling.datamodel.accelerator_options import AcceleratorDevice
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import RapidOcrOptions, TableFormerMode
from docling.document_converter import (
    DocumentConverter,
    ImageFormatOption,
    InputFormat,
    PdfFormatOption,
)
from docling_core.types.doc import DoclingDocument

import src.converter as converter_module
from src.converter import (
    Converter,
    PdfAnalysis,
    _analyze_pdf_complexity,
    _convert_with_docling,
    _convert_with_markitdown,
)
from src.integration import CONVERTIBLE_EXTENSIONS
from src.models import ConversionStrategy, JobLLMSettings
from tests.conversion_support import (
    DENSE_TEXT,
    ZERO_PAGE_PDF,
    PageSpec,
    make_docx,
    make_pdf,
    make_pptx,
    make_xlsx,
    png_bytes,
)

FAST = ConversionStrategy.FAST
BALANCED = ConversionStrategy.BALANCED
ACCURATE = ConversionStrategy.ACCURATE


def expected_score(analysis: PdfAnalysis) -> float:
    """The documented weighting, recomputed from the raw counters."""
    n = analysis.page_count
    return min(1.0, (
        0.35 * (analysis.pages_with_images / n)
        + 0.25 * min((analysis.total_drawings / n) / 50.0, 1.0)
        + 0.20 * (analysis.total_tables / n)
        + 0.20 * max(0.0, 1.0 - (analysis.total_text_chars / n) / 500.0)
    ))


# --------------------------------------------------------------------------- #
# _analyze_pdf_complexity  (real PyMuPDF on real PDFs)
# --------------------------------------------------------------------------- #

class TestPdfComplexity:
    def test_defaults_describe_an_empty_document(self):
        assert PdfAnalysis() == PdfAnalysis(
            page_count=0, total_images=0, pages_with_images=0, total_tables=0,
            total_drawings=0, total_text_chars=0, complexity_score=0.0,
        )

    def test_text_dense_pdf_is_not_complex(self, tmp_path):
        pdf = make_pdf(tmp_path / "dense.pdf", [PageSpec(text=DENSE_TEXT)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.page_count == 1
        assert analysis.total_images == 0
        assert analysis.pages_with_images == 0
        assert analysis.total_drawings == 0
        assert analysis.total_tables == 0
        # every non-space character of the source text was extracted
        assert analysis.total_text_chars >= len(DENSE_TEXT.replace(" ", ""))
        assert analysis.complexity_score == 0.0

    def test_sparse_text_raises_the_score_proportionally(self, tmp_path):
        pdf = make_pdf(tmp_path / "short.pdf", [PageSpec(text="Hello PDF world")])

        analysis = _analyze_pdf_complexity(pdf)

        assert 15 <= analysis.total_text_chars <= 20
        assert analysis.complexity_score == pytest.approx(
            0.20 * (1 - analysis.total_text_chars / 500.0)
        )
        assert 0.19 < analysis.complexity_score < 0.20

    def test_blank_page_scores_only_text_sparsity(self, tmp_path):
        pdf = make_pdf(tmp_path / "blank.pdf", [PageSpec()])

        analysis = _analyze_pdf_complexity(pdf)

        assert (analysis.page_count, analysis.total_text_chars) == (1, 0)
        assert analysis.complexity_score == pytest.approx(0.20)

    def test_images_are_counted_per_page(self, tmp_path):
        pdf = make_pdf(tmp_path / "images.pdf", [
            PageSpec(images=1),
            PageSpec(images=2),
            PageSpec(),
        ])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.page_count == 3
        assert analysis.total_images == 3
        assert analysis.pages_with_images == 2
        # 0.35 * 2/3 image density + 0.20 text sparsity (no text at all)
        assert analysis.complexity_score == pytest.approx(0.35 * 2 / 3 + 0.20)

    def test_image_on_text_dense_page_scores_image_density_only(self, tmp_path):
        pdf = make_pdf(tmp_path / "illustrated.pdf", [PageSpec(text=DENSE_TEXT, images=1)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.pages_with_images == 1
        assert analysis.complexity_score == pytest.approx(0.35)

    def test_ruled_grid_is_detected_as_a_table(self, tmp_path):
        pdf = make_pdf(tmp_path / "table.pdf", [PageSpec(table=True)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.total_drawings == 7  # 4 horizontal + 3 vertical rules
        assert analysis.total_tables == 1
        # 0.25 * 7/50 drawings + 0.20 table + 0.20 sparsity
        assert analysis.complexity_score == pytest.approx(0.25 * 7 / 50 + 0.20 + 0.20)

    @pytest.mark.parametrize(
        "lines",
        [
            pytest.param(
                [((100, 100), (400, 100)), ((100, 130), (400, 130)),
                 ((100, 100), (100, 130)), ((400, 100), (400, 130))],
                id="two-horizontal-rules-are-not-enough",
            ),
            pytest.param(
                [((100, 100), (400, 100)), ((100, 130), (400, 130)), ((100, 160), (400, 160)),
                 ((100, 100), (100, 160))],
                id="one-vertical-rule-is-not-enough",
            ),
            pytest.param(
                [((100, y), (115, y)) for y in (100, 110, 120, 130)]
                + [((x, 200), (x, 215)) for x in (100, 110, 120)],
                id="short-ticks-are-ignored",
            ),
            pytest.param(
                [((100, 100 + 10 * i), (300, 250 + 10 * i)) for i in range(6)],
                id="diagonals-are-ignored",
            ),
        ],
    )
    def test_line_patterns_that_are_not_tables(self, tmp_path, lines):
        pdf = make_pdf(tmp_path / "lines.pdf", [PageSpec(lines=lines)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.total_drawings == len(lines)
        assert analysis.total_tables == 0

    def test_tables_are_counted_per_page_not_per_grid(self, tmp_path):
        pdf = make_pdf(tmp_path / "tables.pdf", [
            PageSpec(table=True, text=DENSE_TEXT),
            PageSpec(text=DENSE_TEXT),
            PageSpec(table=True, text=DENSE_TEXT),
            PageSpec(text=DENSE_TEXT),
        ])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.page_count == 4
        assert analysis.total_tables == 2
        assert analysis.total_drawings == 14
        assert analysis.complexity_score == pytest.approx(0.25 * (14 / 4) / 50 + 0.20 * 2 / 4)

    def test_drawing_density_saturates_at_fifty_per_page(self, tmp_path):
        pdf = make_pdf(tmp_path / "busy.pdf", [PageSpec(strokes=60, text=DENSE_TEXT)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.total_drawings == 60
        assert analysis.total_tables == 0
        assert analysis.complexity_score == pytest.approx(0.25)

    def test_non_line_shapes_count_as_drawings_but_not_as_table_rules(self, tmp_path):
        import pymupdf

        path = tmp_path / "shapes.pdf"
        doc = pymupdf.open()
        page = doc.new_page()
        for row in range(4):
            page.draw_rect(pymupdf.Rect(100, 100 + 40 * row, 400, 130 + 40 * row))
        page.draw_circle((300, 500), 50)
        doc.save(str(path))
        doc.close()

        analysis = _analyze_pdf_complexity(path)

        assert analysis.total_drawings == 5
        assert analysis.total_tables == 0

    def test_score_is_capped_at_one_for_the_most_complex_page(self, tmp_path):
        pdf = make_pdf(tmp_path / "max.pdf", [PageSpec(images=1, table=True, strokes=60)])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.pages_with_images == 1
        assert analysis.total_tables == 1
        assert analysis.total_drawings == 67
        assert analysis.complexity_score == pytest.approx(1.0)
        assert analysis.complexity_score <= 1.0

    def test_mixed_document_matches_the_documented_weighting(self, tmp_path):
        pdf = make_pdf(tmp_path / "mixed.pdf", [
            PageSpec(text=DENSE_TEXT),
            PageSpec(text="Figure 1", images=2),
            PageSpec(table=True, strokes=10),
            PageSpec(),
            PageSpec(text=DENSE_TEXT, images=1),
        ])

        analysis = _analyze_pdf_complexity(pdf)

        assert analysis.page_count == 5
        assert analysis.total_images == 3
        assert analysis.pages_with_images == 2
        assert analysis.total_tables == 1
        assert analysis.total_drawings == 17
        assert analysis.complexity_score == pytest.approx(expected_score(analysis))
        assert 0 < analysis.complexity_score < 1

    def test_zero_page_pdf_has_zero_score(self, tmp_path):
        pdf = tmp_path / "zero.pdf"
        pdf.write_bytes(ZERO_PAGE_PDF)

        assert _analyze_pdf_complexity(pdf) == PdfAnalysis()

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"this is not a pdf at all", id="garbage"),
            pytest.param(b"", id="empty-file"),
            pytest.param(None, id="missing-file"),
        ],
    )
    def test_unreadable_pdf_is_assumed_complex(self, tmp_path, caplog, content):
        pdf = tmp_path / "broken.pdf"
        if content is not None:
            pdf.write_bytes(content)

        with caplog.at_level(logging.WARNING, logger="src.converter"):
            analysis = _analyze_pdf_complexity(pdf)

        # "On failure, assume complex so LLM path is attempted"
        assert analysis.complexity_score == 1.0
        assert analysis.page_count == 0
        assert "PDF analysis failed" in caplog.text

    def test_analysis_releases_the_file(self, tmp_path):
        """A leaked handle would make temp-dir cleanup fail on Windows."""
        pdf = make_pdf(tmp_path / "locked.pdf", [PageSpec(text="x", images=1)])

        _analyze_pdf_complexity(pdf)

        pdf.unlink()
        assert not pdf.exists()

    def test_analysis_is_logged(self, tmp_path, caplog):
        pdf = make_pdf(tmp_path / "logged.pdf", [PageSpec(), PageSpec(images=1)])

        with caplog.at_level(logging.INFO, logger="src.converter"):
            _analyze_pdf_complexity(pdf)

        assert "PDF analysis for logged.pdf: pages=2, images=1," in caplog.text


# --------------------------------------------------------------------------- #
# _convert_with_markitdown  (real MarkItDown)
# --------------------------------------------------------------------------- #

class TestMarkItDown:
    def test_plain_text_round_trips(self, tmp_path):
        source = tmp_path / "a.txt"
        source.write_text("Hello plain text\nSecond line", encoding="utf-8")

        assert _convert_with_markitdown(source) == "Hello plain text\nSecond line"

    def test_non_ascii_text_survives(self, tmp_path):
        source = tmp_path / "unicode.txt"
        source.write_text("Città è già qui — naïve café 日本語", encoding="utf-8")

        assert _convert_with_markitdown(source) == "Città è già qui — naïve café 日本語"

    def test_csv_becomes_a_markdown_table(self, tmp_path):
        source = tmp_path / "a.csv"
        source.write_text("name,qty\nwidget,3\ngadget,5\n", encoding="utf-8")

        lines = _convert_with_markitdown(source).splitlines()

        assert lines == [
            "| name | qty |",
            "| --- | --- |",
            "| widget | 3 |",
            "| gadget | 5 |",
        ]

    def test_html_structure_is_converted_and_scripts_dropped(self, tmp_path):
        source = tmp_path / "a.html"
        source.write_text(
            "<html><head><title>T</title><script>var secret = 1;</script></head><body>"
            "<h1>Heading One</h1>"
            "<p>Para with <b>bold</b> and <a href='https://example.com/x'>link</a>.</p>"
            "<ul><li>one</li><li>two</li></ul>"
            "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"
            "</body></html>",
            encoding="utf-8",
        )

        markdown = _convert_with_markitdown(source)

        assert markdown.startswith("# Heading One")
        assert "Para with **bold** and [link](https://example.com/x)." in markdown
        assert "* one" in markdown and "* two" in markdown
        assert "| A | B |" in markdown
        assert "| 1 | 2 |" in markdown
        assert "secret" not in markdown
        assert "<" not in markdown

    @pytest.mark.parametrize(
        "name, content",
        [
            ("a.json", '{"k": "v", "n": [1, 2]}'),
            ("a.xml", "<?xml version='1.0'?><root><item id='1'>alpha</item></root>"),
            ("a.yaml", "key: value\nlist:\n  - a\n  - b\n"),
        ],
    )
    def test_structured_text_formats_pass_through(self, tmp_path, name, content):
        source = tmp_path / name
        source.write_text(content, encoding="utf-8")

        assert _convert_with_markitdown(source).strip() == content.strip()

    def test_docx_headings_paragraphs_and_tables(self, tmp_path):
        markdown = _convert_with_markitdown(make_docx(tmp_path / "a.docx"))

        assert "# Quarterly Report" in markdown
        assert "Revenue grew by ten percent." in markdown
        assert "| Region | Sales |" in markdown
        assert "| EMEA | 42 |" in markdown

    def test_xlsx_sheet_becomes_a_titled_table(self, tmp_path):
        markdown = _convert_with_markitdown(make_xlsx(tmp_path / "a.xlsx"))

        assert "## Data" in markdown
        assert "| item | price |" in markdown
        assert "| apple | 1.5 |" in markdown
        assert "| pear | 2" in markdown

    def test_pptx_slide_title_and_body(self, tmp_path):
        markdown = _convert_with_markitdown(make_pptx(tmp_path / "a.pptx"))

        assert "# Deck Title" in markdown
        assert "Subtitle text" in markdown

    def test_pdf_text_layer_is_extracted(self, tmp_path):
        pdf = make_pdf(tmp_path / "a.pdf", [PageSpec(text="Hello PDF world")])

        assert _convert_with_markitdown(pdf).strip() == "Hello PDF world"

    @pytest.mark.parametrize(
        "name, content",
        [
            pytest.param("empty.txt", b"", id="empty-text-file"),
            pytest.param("picture.png", None, id="image-without-text"),
        ],
    )
    def test_empty_result_becomes_a_placeholder_not_a_failure(self, tmp_path, caplog, name, content):
        source = tmp_path / name
        source.write_bytes(png_bytes() if content is None else content)

        with caplog.at_level(logging.WARNING, logger="src.converter"):
            result = _convert_with_markitdown(source)

        # Documented: avoids re-processing the same empty file forever.
        assert result == "-"
        assert "MarkItDown: no content" in caplog.text

    def test_missing_file_returns_none(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="src.converter"):
            result = _convert_with_markitdown(tmp_path / "missing.txt")

        assert result is None
        assert "MarkItDown error" in caplog.text

    def test_falsy_result_object_becomes_placeholder(self, tmp_path, monkeypatch):
        fake = MagicMock()
        fake.return_value.convert.return_value = None
        monkeypatch.setattr(converter_module, "MarkItDown", fake)

        assert _convert_with_markitdown(tmp_path / "a.txt") == "-"
        fake.return_value.convert.assert_called_once_with(str(tmp_path / "a.txt"))


# --------------------------------------------------------------------------- #
# _convert_with_docling  (auto-specced Docling)
# --------------------------------------------------------------------------- #

def docling_returning(markdown):
    """A DocumentConverter mock whose conversion exports ``markdown``.

    ``convert`` and ``export_to_markdown`` are signature-checked against the
    installed Docling, so renamed/removed parameters fail the test.
    """
    document = create_autospec(DoclingDocument, instance=True)
    document.export_to_markdown.return_value = markdown
    converter = create_autospec(DocumentConverter, instance=True)
    converter.convert.return_value = SimpleNamespace(document=document)
    return converter


class TestDocling:
    def test_conversion_result_still_exposes_document(self):
        # _convert_with_docling reads ``result.document``; the SimpleNamespace
        # used above cannot check that, so pin the contract here.
        assert "document" in ConversionResult.model_fields

    def test_markdown_is_exported_without_image_placeholders(self, tmp_path):
        converter = docling_returning("# Title\n\nBody")
        source = tmp_path / "scan.pdf"

        assert _convert_with_docling(source, converter) == "# Title\n\nBody"

        converter.convert.assert_called_once_with(str(source), raises_on_error=True)
        document = converter.convert.return_value.document
        document.export_to_markdown.assert_called_once_with(image_placeholder="")

    def test_empty_markdown_is_a_failure(self, tmp_path, caplog):
        converter = docling_returning("")

        with caplog.at_level(logging.WARNING, logger="src.converter"):
            assert _convert_with_docling(tmp_path / "a.pdf", converter) is None

        assert "Docling: no content" in caplog.text

    def test_result_without_document_is_a_failure(self, tmp_path):
        converter = create_autospec(DocumentConverter, instance=True)
        converter.convert.return_value = SimpleNamespace(document=None)

        assert _convert_with_docling(tmp_path / "a.pdf", converter) is None

    def test_no_result_is_a_failure(self, tmp_path):
        converter = create_autospec(DocumentConverter, instance=True)
        converter.convert.return_value = None

        assert _convert_with_docling(tmp_path / "a.pdf", converter) is None

    def test_conversion_error_is_swallowed(self, tmp_path, caplog):
        converter = create_autospec(DocumentConverter, instance=True)
        converter.convert.side_effect = RuntimeError("pipeline exploded")

        with caplog.at_level(logging.WARNING, logger="src.converter"):
            assert _convert_with_docling(tmp_path / "a.pdf", converter) is None

        assert "Docling error" in caplog.text
        assert "pipeline exploded" in caplog.text


class TestDoclingFactory:
    """The genuine factory, with DocumentConverter itself auto-specced.

    The option objects are Docling's real pydantic models, so a renamed option
    or enum member in an upgraded Docling breaks these tests; only pipeline
    initialisation (model loading) is mocked away.
    """

    @pytest.fixture
    def built(self, real_docling_factory, monkeypatch):
        converter_cls = create_autospec(DocumentConverter)
        monkeypatch.setattr(converter_module, "DocumentConverter", converter_cls)
        instance = real_docling_factory()
        return converter_cls, instance

    def test_returns_the_configured_converter(self, built):
        converter_cls, instance = built

        converter_cls.assert_called_once()
        assert instance is converter_cls.return_value
        assert set(converter_cls.call_args.kwargs) == {"format_options"}
        assert converter_cls.call_args.args == ()

    def test_only_pdf_and_image_formats_are_customised(self, built):
        converter_cls, _ = built
        format_options = converter_cls.call_args.kwargs["format_options"]

        assert set(format_options) == {InputFormat.PDF, InputFormat.IMAGE}
        assert isinstance(format_options[InputFormat.PDF], PdfFormatOption)
        assert isinstance(format_options[InputFormat.IMAGE], ImageFormatOption)

    def test_pdf_pipeline_skips_ocr_and_uses_accurate_tables(self, built):
        converter_cls, _ = built
        options = converter_cls.call_args.kwargs["format_options"][InputFormat.PDF].pipeline_options

        assert options.do_ocr is False
        assert options.table_structure_options.mode == TableFormerMode.ACCURATE
        assert options.accelerator_options.device == AcceleratorDevice.AUTO
        assert options.accelerator_options.cuda_use_flash_attention2 is False

    def test_image_pipeline_runs_rapidocr(self, built):
        converter_cls, _ = built
        options = converter_cls.call_args.kwargs["format_options"][InputFormat.IMAGE].pipeline_options

        assert options.do_ocr is True
        assert isinstance(options.ocr_options, RapidOcrOptions)
        assert options.ocr_options.text_score == 0.5
        assert options.ocr_options.print_verbose is False
        assert options.table_structure_options.mode == TableFormerMode.ACCURATE
        assert options.accelerator_options.device == AcceleratorDevice.AUTO

    def test_both_pipelines_are_initialised_eagerly(self, built):
        _, instance = built

        assert [c.args for c in instance.initialize_pipeline.call_args_list] == [
            (InputFormat.PDF,),
            (InputFormat.IMAGE,),
        ]


# --------------------------------------------------------------------------- #
# Converter
# --------------------------------------------------------------------------- #

@pytest.fixture
def docling(monkeypatch):
    """Install a controllable Docling mock as the Converter's cached pipeline."""
    mock = docling_returning("DOCLING MARKDOWN")
    monkeypatch.setattr(Converter, "_doc_converter", mock)
    return mock


class Engines:
    """Recording stand-ins for the three engines and the PDF analysis."""

    def __init__(self):
        self.calls: list[str] = []
        self.results: dict[str, object] = {"markitdown": "MARKITDOWN", "docling": "DOCLING", "llm": "LLM"}
        self.analysis = PdfAnalysis(page_count=1, complexity_score=1.0)
        self.paths: list = []
        self.docling_converter = None
        self.llm_settings = None

    def markitdown(self, input_path):
        self.calls.append("markitdown")
        self.paths.append(input_path)
        return self.results["markitdown"]

    def docling(self, input_path, converter):
        self.calls.append("docling")
        self.paths.append(input_path)
        self.docling_converter = converter
        return self.results["docling"]

    def llm(self, input_path, settings):
        self.calls.append("llm")
        self.paths.append(input_path)
        self.llm_settings = settings
        return self.results["llm"]

    def analyze(self, input_path):
        self.calls.append("analyze")
        return self.analysis


@pytest.fixture
def engines(monkeypatch, docling):
    recorder = Engines()
    monkeypatch.setattr(converter_module, "_convert_with_markitdown", recorder.markitdown)
    monkeypatch.setattr(converter_module, "_convert_with_docling", recorder.docling)
    monkeypatch.setattr(converter_module, "_convert_with_llm", recorder.llm)
    monkeypatch.setattr(converter_module, "_analyze_pdf_complexity", recorder.analyze)
    return recorder


@pytest.fixture
def converter(settings, docling):
    return Converter(settings)


class TestConverterConstruction:
    def test_docling_pipeline_is_built_once_and_cached_on_the_class(self, settings, monkeypatch):
        built = []

        def factory():
            built.append(MagicMock(name=f"pipeline-{len(built)}"))
            return built[-1]

        monkeypatch.setattr(Converter, "_doc_converter", None)
        monkeypatch.setattr(converter_module, "_docling_converter", factory)

        first = Converter(settings)
        second = Converter(settings)

        assert len(built) == 1
        assert Converter._doc_converter is built[0]
        assert first._get_docling() is built[0]
        assert second._get_docling() is built[0]

    def test_existing_pipeline_is_reused(self, settings, docling, monkeypatch):
        monkeypatch.setattr(
            converter_module, "_docling_converter",
            lambda: pytest.fail("pipeline must not be rebuilt"),
        )

        assert Converter(settings)._get_docling() is docling

    def test_pipeline_is_rebuilt_lazily_if_the_cache_was_cleared(self, converter, monkeypatch):
        rebuilt = MagicMock(name="rebuilt")
        monkeypatch.setattr(Converter, "_doc_converter", None)
        monkeypatch.setattr(converter_module, "_docling_converter", lambda: rebuilt)

        assert converter._get_docling() is rebuilt
        assert Converter._doc_converter is rebuilt


class TestEffectiveSettings:
    def test_without_job_overrides_the_global_settings_are_used_as_is(self, converter, settings):
        assert converter._effective_settings(None) is settings

    def test_empty_overrides_change_nothing(self, converter, settings):
        merged = converter._effective_settings(JobLLMSettings())

        assert merged is not settings
        assert merged == settings

    def test_every_field_can_be_overridden(self, converter, settings):
        merged = converter._effective_settings(JobLLMSettings(
            llm_provider="anthropic",
            llm_model="claude-test",
            llm_api_key="job-key",
            llm_base_url="http://job.example/v1",
            llm_max_pages=7,
            pdf_complexity_threshold=0.75,
        ))

        assert merged.llm_provider == "anthropic"
        assert merged.llm_model == "claude-test"
        assert merged.llm_api_key == "job-key"
        assert merged.llm_base_url == "http://job.example/v1"
        assert merged.llm_max_pages == 7
        assert merged.pdf_complexity_threshold == 0.75

    @pytest.mark.parametrize(
        "field, value",
        [
            ("llm_provider", "google"),
            ("llm_model", "gemini-test"),
            ("llm_api_key", "k"),
            ("llm_base_url", "http://x/v1"),
            ("llm_max_pages", 3),
            ("pdf_complexity_threshold", 0.9),
        ],
    )
    def test_only_explicitly_set_fields_are_patched(self, converter, settings, field, value):
        merged = converter._effective_settings(JobLLMSettings(**{field: value}))

        assert getattr(merged, field) == value
        assert merged.model_dump(exclude={field}) == settings.model_dump(exclude={field})

    def test_falsy_but_explicit_overrides_are_honoured(self, converter):
        merged = converter._effective_settings(JobLLMSettings(
            llm_api_key="", llm_base_url="", llm_max_pages=0, pdf_complexity_threshold=0.0,
        ))

        assert merged.llm_max_pages == 0
        assert merged.pdf_complexity_threshold == 0.0
        assert merged.llm_api_key == ""
        assert merged.llm_base_url == ""

    def test_global_settings_are_never_mutated(self, converter, settings):
        before = settings.model_dump()

        converter._effective_settings(JobLLMSettings(llm_provider="ollama", llm_max_pages=1))

        assert settings.model_dump() == before


class TestConverterDispatch:
    @pytest.mark.parametrize("name", ["archive.zip", "song.mp3", "README", "notes.md"])
    @pytest.mark.parametrize("strategy", [FAST, BALANCED, ACCURATE])
    def test_unsupported_extensions_are_rejected_without_touching_any_engine(
        self, converter, engines, tmp_path, caplog, name, strategy
    ):
        with caplog.at_level(logging.WARNING, logger="src.converter"):
            assert converter.convert(tmp_path / name, strategy) is None

        assert engines.calls == []
        assert "Unsupported file type" in caplog.text

    def test_default_strategy_is_fast(self, converter, engines, tmp_path):
        assert converter.convert(tmp_path / "a.pdf") == "MARKITDOWN"
        assert engines.calls == ["markitdown"]

    def test_extension_matching_is_case_insensitive(self, converter, engines, tmp_path):
        assert converter.convert(tmp_path / "SCAN.PDF", BALANCED) == "DOCLING"
        assert engines.calls == ["docling"]

    def test_unknown_strategy_is_an_error(self, converter, engines, tmp_path, caplog):
        with caplog.at_level(logging.ERROR, logger="src.converter"):
            assert converter.convert(tmp_path / "a.txt", "turbo") is None

        assert engines.calls == []
        assert "Unknown strategy" in caplog.text

    def test_strategy_may_be_given_as_its_string_value(self, converter, engines, tmp_path):
        # ConversionStrategy is a str-enum; jobs loaded from storage carry strings.
        assert converter.convert(tmp_path / "a.png", "balanced") == "DOCLING"

    @pytest.mark.parametrize("extension", sorted(CONVERTIBLE_EXTENSIONS))
    def test_every_convertible_extension_is_accepted(self, converter, engines, tmp_path, extension):
        assert converter.convert(tmp_path / f"file{extension}", FAST) == "MARKITDOWN"


class TestFastStrategy:
    @pytest.mark.parametrize("name", ["a.pdf", "a.png", "a.docx", "a.txt"])
    def test_markitdown_first_for_every_type(self, converter, engines, tmp_path, name):
        assert converter.convert(tmp_path / name, FAST) == "MARKITDOWN"
        assert engines.calls == ["markitdown"]
        assert engines.paths == [tmp_path / name]

    def test_falls_back_to_docling(self, converter, engines, docling, tmp_path):
        engines.results["markitdown"] = None

        assert converter.convert(tmp_path / "a.docx", FAST) == "DOCLING"
        assert engines.calls == ["markitdown", "docling"]
        assert engines.docling_converter is docling

    def test_returns_none_when_both_fail(self, converter, engines, tmp_path):
        engines.results.update(markitdown=None, docling=None)

        assert converter.convert(tmp_path / "a.docx", FAST) is None
        assert engines.calls == ["markitdown", "docling"]

    def test_never_uses_the_llm_or_pdf_analysis(self, converter, engines, tmp_path):
        engines.results.update(markitdown=None, docling=None)

        converter.convert(tmp_path / "a.pdf", FAST)

        assert "llm" not in engines.calls and "analyze" not in engines.calls


class TestBalancedStrategy:
    @pytest.mark.parametrize("name", ["a.pdf", "a.jpg", "a.jpeg", "a.png", "a.gif", "a.bmp", "a.tiff"])
    def test_pdf_and_images_go_to_docling_first(self, converter, engines, docling, tmp_path, name):
        assert converter.convert(tmp_path / name, BALANCED) == "DOCLING"
        assert engines.calls == ["docling"]
        assert engines.docling_converter is docling

    def test_pdf_falls_back_to_markitdown(self, converter, engines, tmp_path):
        engines.results["docling"] = None

        assert converter.convert(tmp_path / "a.pdf", BALANCED) == "MARKITDOWN"
        assert engines.calls == ["docling", "markitdown"]

    @pytest.mark.parametrize("name", ["a.docx", "a.xlsx", "a.pptx", "a.csv", "a.html", "a.txt"])
    def test_other_formats_go_to_markitdown_first(self, converter, engines, tmp_path, name):
        assert converter.convert(tmp_path / name, BALANCED) == "MARKITDOWN"
        assert engines.calls == ["markitdown"]

    def test_other_formats_fall_back_to_docling(self, converter, engines, tmp_path):
        engines.results["markitdown"] = None

        assert converter.convert(tmp_path / "a.docx", BALANCED) == "DOCLING"
        assert engines.calls == ["markitdown", "docling"]

    @pytest.mark.parametrize("name", ["a.pdf", "a.docx"])
    def test_returns_none_when_both_fail_and_never_uses_the_llm(self, converter, engines, tmp_path, name):
        engines.results.update(markitdown=None, docling=None)

        assert converter.convert(tmp_path / name, BALANCED) is None
        assert sorted(engines.calls) == ["docling", "markitdown"]


class TestAccurateStrategy:
    # ---- non PDF/image ----
    def test_office_documents_use_docling_then_markitdown(self, converter, engines, tmp_path):
        assert converter.convert(tmp_path / "a.docx", ACCURATE) == "DOCLING"
        assert engines.calls == ["docling"]

        engines.calls.clear()
        engines.results["docling"] = None
        assert converter.convert(tmp_path / "a.docx", ACCURATE) == "MARKITDOWN"
        assert engines.calls == ["docling", "markitdown"]

    def test_office_documents_never_reach_the_llm(self, converter, engines, tmp_path):
        engines.results.update(markitdown=None, docling=None)

        assert converter.convert(tmp_path / "a.xlsx", ACCURATE) is None
        assert engines.calls == ["docling", "markitdown"]

    # ---- images ----
    @pytest.mark.parametrize("name", ["a.jpg", "a.jpeg", "a.png", "a.gif", "a.bmp", "a.tiff"])
    def test_images_go_straight_to_the_llm(self, converter, engines, settings, tmp_path, name):
        assert converter.convert(tmp_path / name, ACCURATE) == "LLM"
        assert engines.calls == ["llm"]  # no PDF analysis for images
        assert engines.llm_settings is settings

    def test_images_fall_back_to_docling(self, converter, engines, docling, tmp_path):
        engines.results["llm"] = None

        assert converter.convert(tmp_path / "a.png", ACCURATE) == "DOCLING"
        assert engines.calls == ["llm", "docling"]
        assert engines.docling_converter is docling

    def test_images_do_not_fall_back_to_markitdown(self, converter, engines, tmp_path):
        engines.results.update(llm=None, docling=None)

        assert converter.convert(tmp_path / "a.png", ACCURATE) is None
        assert engines.calls == ["llm", "docling"]

    # ---- PDFs: page-count gate ----
    @pytest.mark.parametrize("pages", [50, 51, 500])
    def test_long_pdfs_skip_the_llm(self, converter, engines, tmp_path, pages):
        engines.analysis = PdfAnalysis(page_count=pages, complexity_score=1.0)

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "DOCLING"
        assert engines.calls == ["analyze", "docling"]

    def test_long_pdfs_fall_back_to_markitdown_not_the_llm(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=50, complexity_score=1.0)
        engines.results["docling"] = None

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "MARKITDOWN"
        assert engines.calls == ["analyze", "docling", "markitdown"]

    def test_pdf_one_page_under_the_limit_may_use_the_llm(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=49, complexity_score=1.0)

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "LLM"
        assert engines.calls == ["analyze", "llm"]

    # ---- PDFs: complexity gate ----
    def test_simple_pdfs_are_downscaled_to_docling(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.05)

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "DOCLING"
        assert engines.calls == ["analyze", "docling"]

    def test_simple_pdfs_fall_back_to_the_llm(self, converter, engines, settings, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.05)
        engines.results["docling"] = None

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "LLM"
        assert engines.calls == ["analyze", "docling", "llm"]
        assert engines.llm_settings is settings

    def test_simple_pdf_returns_none_when_docling_and_llm_fail(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.0)
        engines.results.update(docling=None, llm=None)

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) is None
        assert engines.calls == ["analyze", "docling", "llm"]

    def test_complexity_equal_to_the_threshold_uses_the_llm(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.1)  # default threshold

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "LLM"
        assert engines.calls == ["analyze", "llm"]

    def test_complex_pdfs_use_the_llm_then_docling(self, converter, engines, docling, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.8)

        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "LLM"
        assert engines.calls == ["analyze", "llm"]

        engines.calls.clear()
        engines.results["llm"] = None
        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "DOCLING"
        assert engines.calls == ["analyze", "llm", "docling"]
        assert engines.docling_converter is docling

    def test_uppercase_pdf_extension_is_still_analysed(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=3, complexity_score=0.0)

        assert converter.convert(tmp_path / "REPORT.PDF", ACCURATE) == "DOCLING"
        assert engines.calls == ["analyze", "docling"]

    def test_page_gate_wins_over_complexity_gate(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=80, complexity_score=0.0)
        engines.results["docling"] = None

        # long *and* simple: fallback is MarkItDown (page gate), not the LLM
        assert converter.convert(tmp_path / "a.pdf", ACCURATE) == "MARKITDOWN"
        assert "llm" not in engines.calls

    # ---- per-job overrides ----
    def test_job_can_lower_the_page_limit(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=5, complexity_score=1.0)

        result = converter.convert(tmp_path / "a.pdf", ACCURATE, JobLLMSettings(llm_max_pages=5))

        assert result == "DOCLING"
        assert engines.calls == ["analyze", "docling"]

    def test_job_can_raise_the_page_limit(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=120, complexity_score=1.0)

        result = converter.convert(tmp_path / "a.pdf", ACCURATE, JobLLMSettings(llm_max_pages=121))

        assert result == "LLM"
        assert engines.llm_settings.llm_max_pages == 121

    def test_job_can_raise_the_complexity_threshold(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=2, complexity_score=0.5)

        result = converter.convert(
            tmp_path / "a.pdf", ACCURATE, JobLLMSettings(pdf_complexity_threshold=0.6)
        )

        assert result == "DOCLING"
        assert engines.calls == ["analyze", "docling"]

    def test_job_can_disable_the_complexity_gate(self, converter, engines, tmp_path):
        engines.analysis = PdfAnalysis(page_count=2, complexity_score=0.0)

        result = converter.convert(
            tmp_path / "a.pdf", ACCURATE, JobLLMSettings(pdf_complexity_threshold=0.0)
        )

        assert result == "LLM"

    def test_llm_receives_the_job_specific_credentials(self, converter, engines, settings, tmp_path):
        job_llm = JobLLMSettings(
            llm_provider="ollama", llm_model="llava", llm_api_key="job-key",
            llm_base_url="http://ollama.internal:11434/v1",
        )

        assert converter.convert(tmp_path / "a.png", ACCURATE, job_llm) == "LLM"

        used = engines.llm_settings
        assert used is not settings
        assert (used.llm_provider, used.llm_model) == ("ollama", "llava")
        assert (used.llm_api_key, used.llm_base_url) == ("job-key", "http://ollama.internal:11434/v1")
        # untouched fields still come from the global settings
        assert used.llm_max_pages == settings.llm_max_pages
        assert settings.llm_provider == "openai"

    def test_job_overrides_are_ignored_by_other_strategies(self, converter, engines, tmp_path):
        job_llm = JobLLMSettings(llm_max_pages=0, pdf_complexity_threshold=1.0)

        assert converter.convert(tmp_path / "a.pdf", BALANCED, job_llm) == "DOCLING"
        assert engines.calls == ["docling"]


class TestConverterWithRealEngines:
    """End-to-end through Converter.convert with real MarkItDown / PyMuPDF."""

    @pytest.fixture
    def llm_calls(self, monkeypatch):
        calls = []

        def fake_llm(input_path, settings):
            calls.append((input_path, settings))
            return "LLM MARKDOWN"

        monkeypatch.setattr(converter_module, "_convert_with_llm", fake_llm)
        return calls

    def test_fast_converts_office_documents_with_markitdown(self, converter, docling, tmp_path):
        assert "# Quarterly Report" in converter.convert(make_docx(tmp_path / "a.docx"), FAST)
        assert "| apple | 1.5 |" in converter.convert(make_xlsx(tmp_path / "a.xlsx"), FAST)
        assert "# Deck Title" in converter.convert(make_pptx(tmp_path / "a.pptx"), FAST)
        docling.convert.assert_not_called()

    def test_fast_falls_back_to_docling_when_markitdown_cannot_read_the_file(
        self, converter, docling, tmp_path
    ):
        missing = tmp_path / "vanished.docx"

        assert converter.convert(missing, FAST) == "DOCLING MARKDOWN"
        docling.convert.assert_called_once_with(str(missing), raises_on_error=True)

    def test_balanced_pdf_falls_back_to_real_markitdown(self, converter, docling, tmp_path):
        docling.convert.side_effect = RuntimeError("no pipeline")
        pdf = make_pdf(tmp_path / "a.pdf", [PageSpec(text="Hello PDF world")])

        assert converter.convert(pdf, BALANCED).strip() == "Hello PDF world"

    def test_balanced_text_file_never_touches_docling(self, converter, docling, tmp_path):
        source = tmp_path / "a.txt"
        source.write_text("plain", encoding="utf-8")

        assert converter.convert(source, BALANCED) == "plain"
        docling.convert.assert_not_called()

    def test_accurate_text_dense_pdf_is_downscaled_to_docling(
        self, converter, docling, llm_calls, tmp_path
    ):
        pdf = make_pdf(tmp_path / "dense.pdf", [PageSpec(text=DENSE_TEXT)] * 2)

        assert converter.convert(pdf, ACCURATE) == "DOCLING MARKDOWN"
        assert llm_calls == []

    def test_accurate_image_heavy_pdf_goes_to_the_llm(
        self, converter, docling, llm_calls, settings, tmp_path
    ):
        pdf = make_pdf(tmp_path / "scan.pdf", [PageSpec(images=1), PageSpec(images=1)])

        assert converter.convert(pdf, ACCURATE) == "LLM MARKDOWN"
        assert llm_calls == [(pdf, settings)]
        docling.convert.assert_not_called()

    def test_accurate_respects_page_limit_on_a_real_pdf(
        self, converter, docling, llm_calls, tmp_path
    ):
        pdf = make_pdf(tmp_path / "long.pdf", [PageSpec(images=1)] * 3)

        result = converter.convert(pdf, ACCURATE, JobLLMSettings(llm_max_pages=3))

        assert result == "DOCLING MARKDOWN"
        assert llm_calls == []

        assert converter.convert(pdf, ACCURATE, JobLLMSettings(llm_max_pages=4)) == "LLM MARKDOWN"

    def test_accurate_unreadable_pdf_is_treated_as_complex(
        self, converter, docling, llm_calls, tmp_path
    ):
        pdf = tmp_path / "corrupt.pdf"
        pdf.write_bytes(b"definitely not a pdf")

        assert converter.convert(pdf, ACCURATE) == "LLM MARKDOWN"
        assert len(llm_calls) == 1

    def test_accurate_zero_page_pdf_is_downscaled_to_docling(
        self, converter, docling, llm_calls, tmp_path
    ):
        pdf = tmp_path / "zero.pdf"
        pdf.write_bytes(ZERO_PAGE_PDF)

        assert converter.convert(pdf, ACCURATE) == "DOCLING MARKDOWN"
        assert llm_calls == []
