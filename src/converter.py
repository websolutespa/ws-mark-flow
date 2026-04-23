"""
Converter service using multiple strategies.
Converts various file formats to Markdown.

Strategies:
- FAST: MarkItDown for all files, fallback to Docling on error
- BALANCED: Docling for PDFs/images, MarkItDown for others; cross-fallback on error
- ACCURATE: LLM for PDFs/images, Docling for others; cross-fallback on error
"""
import asyncio
import base64
import logging
import tempfile
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import fitz  # PyMuPDF

# conversion libraries
from markitdown import MarkItDown
from docling.document_converter import DocumentConverter, InputFormat, PdfFormatOption, ImageFormatOption
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableStructureOptions, TableFormerMode
from docling.datamodel.pipeline_options import TableStructureOptions, TableFormerMode, RapidOcrOptions
from docling.document_converter import DocumentConverter, PdfFormatOption, ImageFormatOption

from .config import Settings

from .integration import (
    SourceIntegration, 
    DestinationIntegration, 
    FileInfo,
    CONVERTIBLE_EXTENSIONS,
    MARKDOWN_EXTENSIONS
)
from .models import (
    ConversionJob,
    ConversionAnalysis,
    ConversionStrategy,
    FileConversionResult,
    FileConversionStatus,
    JobLLMSettings,
    JobStatus
)

logger = logging.getLogger(__name__)

# Extensions treated as PDF/image for strategy routing
_PDF_IMAGE_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff"}


@dataclass
class PdfAnalysis:
    """Result of PDF complexity analysis via PyMuPDF."""
    page_count: int = 0
    total_images: int = 0
    pages_with_images: int = 0
    total_tables: int = 0  # heuristic: detected table-like structures
    total_drawings: int = 0
    total_text_chars: int = 0
    complexity_score: float = 0.0  # 0-1, higher = more complex


def _analyze_pdf_complexity(input_path: Path) -> PdfAnalysis:
    """
    Analyze a PDF's structural complexity using PyMuPDF.

    Complexity score (0-1) considers:
    - Image density: ratio of pages containing images
    - Drawing density: non-trivial vector drawings per page (charts, diagrams)
    - Table heuristic: pages where horizontal+vertical lines suggest tables
    - Text sparsity: very little text relative to pages suggests scanned/image PDF

    A high score means the document benefits from LLM vision capabilities;
    a low score means Docling can handle it well enough.
    """
    analysis = PdfAnalysis()
    try:
        doc = fitz.open(str(input_path))
        analysis.page_count = doc.page_count

        if analysis.page_count == 0:
            return analysis

        for page in doc:
            # Images
            images = page.get_images(full=True)
            img_count = len(images)
            analysis.total_images += img_count
            if img_count > 0:
                analysis.pages_with_images += 1

            # Drawings (vector paths – charts, diagrams, shapes)
            drawings = page.get_drawings()
            analysis.total_drawings += len(drawings)

            # Table heuristic: look for rect/line-heavy areas
            # Pages with many short horizontal+vertical lines likely contain tables
            h_lines = 0
            v_lines = 0
            for d in drawings:
                for item in d.get("items", []):
                    if item[0] == "l":  # line
                        p1, p2 = item[1], item[2]
                        dx = abs(p2.x - p1.x)
                        dy = abs(p2.y - p1.y)
                        if dy < 2 and dx > 20:
                            h_lines += 1
                        elif dx < 2 and dy > 20:
                            v_lines += 1
            if h_lines >= 3 and v_lines >= 2:
                analysis.total_tables += 1

            # Text length
            analysis.total_text_chars += len(page.get_text())

        doc.close()

        # --- Compute composite complexity score ---
        n = analysis.page_count

        # Image density: fraction of pages with images (0-1)
        image_density = analysis.pages_with_images / n

        # Drawing density: saturates at ~50 drawings/page
        avg_drawings = analysis.total_drawings / n
        drawing_density = min(avg_drawings / 50.0, 1.0)

        # Table density: fraction of pages with detected tables
        table_density = analysis.total_tables / n

        # Text sparsity: very low text per page suggests scanned/image-heavy PDF
        avg_chars = analysis.total_text_chars / n
        text_sparsity = max(0.0, 1.0 - (avg_chars / 500.0))  # <500 chars/page = sparse

        # Weighted composite
        analysis.complexity_score = min(1.0, (
            0.35 * image_density
            + 0.25 * drawing_density
            + 0.20 * table_density
            + 0.20 * text_sparsity
        ))

        logger.info(
            f"PDF analysis for {input_path.name}: pages={n}, images={analysis.total_images}, "
            f"tables={analysis.total_tables}, drawings={analysis.total_drawings}, "
            f"chars={analysis.total_text_chars}, complexity={analysis.complexity_score:.3f}"
        )

    except Exception as e:
        logger.warning(f"PDF analysis failed for {input_path}: {e}")
        # On failure, assume complex so LLM path is attempted
        analysis.complexity_score = 1.0

    return analysis

LLM_CONVERSION_PROMPT = """Convert the content of this document to well-structured Markdown.

Rules:
- Preserve ALL text content faithfully — do not summarize or omit anything.
- Use proper Markdown headings (#, ##, ###) reflecting the document structure.
- Preserve tables as Markdown tables.
- Preserve lists as Markdown lists.
- For images describe them in Markdown with high-level details (e.g. "Image: A bar chart showing sales growth over 2023, with categories A, B, C on the x-axis and values on the y-axis").
- For diagrams and flowcharts, describe them as numerated steps, with flow indicators (e.g. Step 1 -> Step 2), subject involved, and key details.
- Do not add commentary, explanations, or notes — only the converted content.
- Output raw Markdown only, no code fences wrapping the entire document."""

#region converters


def _docling_converter() -> DocumentConverter:
    _pipeline_config = {
        "accelerator_options": AcceleratorOptions(
            device=AcceleratorDevice.AUTO,
            cuda_use_flash_attention2=False,
        ),
        "table_structure_options": TableStructureOptions(mode=TableFormerMode.ACCURATE),
    }
    _base_pipeline_options = PdfPipelineOptions(
        **_pipeline_config,
        do_ocr=False)
    _ocr_pipeline_options = PdfPipelineOptions(
        **_pipeline_config,
        ocr_options=RapidOcrOptions(
            print_verbose=False,
            text_score=0.5,
        ))
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=_base_pipeline_options,
            ),
            InputFormat.IMAGE: ImageFormatOption(
                pipeline_options=_ocr_pipeline_options,
            ),
        }
    )
    for frm in [InputFormat.PDF, InputFormat.IMAGE]:
        doc_converter.initialize_pipeline(frm)
    return doc_converter


def _convert_with_markitdown(input_path: Path) -> Optional[str]:
    """Convert a file using MarkItDown. Returns markdown string or None."""
    try:
        result = MarkItDown().convert(str(input_path))
        if result and result.markdown:
            return result.markdown
        logger.warning(f"MarkItDown: no content from {input_path}")
    except Exception as e:
        logger.warning(f"MarkItDown error for {input_path}: {e}")
    return None


def _convert_with_docling(input_path: Path, converter: DocumentConverter) -> Optional[str]:
    """Convert a file using Docling. Returns markdown string or None."""
    try:
        result = converter.convert(str(input_path), raises_on_error=True)
        if result and result.document:
            md = result.document.export_to_markdown(image_placeholder="")
            if md:
                return md
        logger.warning(f"Docling: no content from {input_path}")
    except Exception as e:
        logger.warning(f"Docling error for {input_path}: {e}")
    return None


def _convert_with_llm(input_path: Path, settings: Settings) -> Optional[str]:
    """Convert a file using an LLM with vision capabilities. Returns markdown string or None."""
    try:
        provider = settings.llm_provider.lower()
        file_bytes = input_path.read_bytes()
        ext = input_path.suffix.lower()

        # Determine MIME type
        mime_map = {
            ".pdf": "application/pdf",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".bmp": "image/bmp",
            ".tiff": "image/tiff",
        }
        mime_type = mime_map.get(ext, "application/octet-stream")

        if provider == "openai" or provider == "ollama":
            return _llm_openai_compat(file_bytes, mime_type, settings)
        elif provider == "anthropic":
            return _llm_anthropic(file_bytes, mime_type, ext, settings)
        elif provider == "google":
            return _llm_google(file_bytes, mime_type, settings)
        else:
            logger.error(f"Unsupported LLM provider: {provider}")
            return None

    except Exception as e:
        logger.error(f"LLM conversion error for {input_path}: {e}")
        return None


def _llm_openai_compat(file_bytes: bytes, mime_type: str, settings: Settings) -> Optional[str]:
    """OpenAI / Ollama (OpenAI-compatible) conversion."""
    from openai import OpenAI

    base_url = settings.llm_base_url or None
    api_key = settings.llm_api_key or "ollama"  # ollama doesn't need a real key

    client = OpenAI(api_key=api_key, base_url=base_url)
    b64 = base64.b64encode(file_bytes).decode("utf-8")
    data_uri = f"data:{mime_type};base64,{b64}"

    if mime_type == "application/pdf":
        # For PDF, use file input type if supported (OpenAI gpt-4o supports it)
        content = [
            {"type": "file", "file": {"filename": "document.pdf", "file_data": data_uri}}
            if settings.llm_provider.lower() == "openai"
            else {"type": "image_url", "image_url": {"url": data_uri}},
            {"type": "text", "text": LLM_CONVERSION_PROMPT},
        ]
    else:
        content = [
            {"type": "image_url", "image_url": {"url": data_uri}},
            {"type": "text", "text": LLM_CONVERSION_PROMPT},
        ]

    response = client.chat.completions.create(
        model=settings.llm_model,
        messages=[{"role": "user", "content": content}],
    )

    text = response.choices[0].message.content
    return text.strip() if text else None


def _llm_anthropic(file_bytes: bytes, mime_type: str, ext: str, settings: Settings) -> Optional[str]:
    """Anthropic Claude conversion."""
    from anthropic import Anthropic

    client = Anthropic(api_key=settings.llm_api_key)
    b64 = base64.b64encode(file_bytes).decode("utf-8")

    if ext == ".pdf":
        source_block = {
            "type": "document",
            "source": {"type": "base64", "media_type": mime_type, "data": b64},
        }
    else:
        source_block = {
            "type": "image",
            "source": {"type": "base64", "media_type": mime_type, "data": b64},
        }

    response = client.messages.create(
        model=settings.llm_model,
        max_tokens=200_000,
        messages=[
            {
                "role": "user",
                "content": [source_block, {"type": "text", "text": LLM_CONVERSION_PROMPT}],
            }
        ],
    )

    text = response.content[0].text if response.content else None
    return text.strip() if text else None


def _llm_google(file_bytes: bytes, mime_type: str, settings: Settings) -> Optional[str]:
    """Google Gemini conversion."""
    from google import genai

    client = genai.Client(api_key=settings.llm_api_key)

    response = client.models.generate_content(
        model=settings.llm_model,
        contents=[
            genai.types.Part.from_bytes(data=file_bytes, mime_type=mime_type),
            LLM_CONVERSION_PROMPT,
        ],
    )

    text = response.text if response else None
    return text.strip() if text else None


class Converter:
    """
    Strategy-aware converter. Dispatches to MarkItDown, Docling, or LLM
    based on the ConversionStrategy and file extension, with fallback chains.
    """

    _doc_converter: Optional[DocumentConverter] = None

    def __init__(self, settings: Settings):
        self._settings = settings
        if Converter._doc_converter is None:
            Converter._doc_converter = _docling_converter()

    def _effective_settings(self, job_llm: Optional[JobLLMSettings]) -> Settings:
        """Return a Settings-like object with per-job overrides applied."""
        if job_llm is None:
            return self._settings
        # Shallow-copy then patch only the fields the job explicitly set
        merged = self._settings.model_copy()
        if job_llm.llm_provider is not None:
            merged.llm_provider = job_llm.llm_provider
        if job_llm.llm_model is not None:
            merged.llm_model = job_llm.llm_model
        if job_llm.llm_api_key is not None:
            merged.llm_api_key = job_llm.llm_api_key
        if job_llm.llm_base_url is not None:
            merged.llm_base_url = job_llm.llm_base_url
        if job_llm.llm_max_pages is not None:
            merged.llm_max_pages = job_llm.llm_max_pages
        if job_llm.pdf_complexity_threshold is not None:
            merged.pdf_complexity_threshold = job_llm.pdf_complexity_threshold
        return merged

    def _get_docling(self) -> DocumentConverter:
        if Converter._doc_converter is None:
            Converter._doc_converter = _docling_converter()
        return Converter._doc_converter

    def convert(self, input_path: Path, strategy: ConversionStrategy = ConversionStrategy.FAST, job_llm: Optional[JobLLMSettings] = None) -> Optional[str]:
        """
        Convert a file to Markdown using the given strategy.

        Returns markdown content or None on failure.
        """
        extension = input_path.suffix.lower()
        if extension not in CONVERTIBLE_EXTENSIONS:
            logger.warning(f"Unsupported file type: {input_path}")
            return None

        is_pdf_image = extension in _PDF_IMAGE_EXTENSIONS
        effective = self._effective_settings(job_llm)

        if strategy == ConversionStrategy.FAST:
            return self._convert_fast(input_path)
        elif strategy == ConversionStrategy.BALANCED:
            return self._convert_balanced(input_path, is_pdf_image)
        elif strategy == ConversionStrategy.ACCURATE:
            return self._convert_accurate(input_path, is_pdf_image, effective)
        else:
            logger.error(f"Unknown strategy: {strategy}")
            return None

    # ---- Strategy implementations ----

    def _convert_fast(self, input_path: Path) -> Optional[str]:
        """FAST: MarkItDown first, fallback to Docling."""
        result = _convert_with_markitdown(input_path)
        if result:
            return result
        logger.info(f"FAST fallback to Docling for {input_path}")
        return _convert_with_docling(input_path, self._get_docling())

    def _convert_balanced(self, input_path: Path, is_pdf_image: bool) -> Optional[str]:
        """BALANCED: Docling for PDF/images, MarkItDown for others; cross-fallback."""
        if is_pdf_image:
            result = _convert_with_docling(input_path, self._get_docling())
            if result:
                return result
            logger.info(f"BALANCED fallback to MarkItDown for {input_path}")
            return _convert_with_markitdown(input_path)
        else:
            result = _convert_with_markitdown(input_path)
            if result:
                return result
            logger.info(f"BALANCED fallback to Docling for {input_path}")
            return _convert_with_docling(input_path, self._get_docling())

    def _convert_accurate(self, input_path: Path, is_pdf_image: bool, effective_settings: Settings) -> Optional[str]:
        """ACCURATE: LLM for complex PDF/images, Docling for others; cross-fallback.
        
        For PDFs, uses PyMuPDF analysis to decide whether LLM is warranted:
        - Page count >= llm_max_pages  → skip LLM, use Docling directly
        - Complexity score < threshold  → downscale to Docling
        - Otherwise                     → use LLM
        """
        if is_pdf_image:
            # For PDFs, perform complexity analysis to decide LLM vs Docling
            if input_path.suffix.lower() == ".pdf":
                analysis = _analyze_pdf_complexity(input_path)

                if analysis.page_count >= effective_settings.llm_max_pages:
                    logger.info(
                        f"ACCURATE: PDF has {analysis.page_count} pages (>= {effective_settings.llm_max_pages}), "
                        f"skipping LLM, using Docling for {input_path}"
                    )
                    result = _convert_with_docling(input_path, self._get_docling())
                    if result:
                        return result
                    logger.info(f"ACCURATE fallback to MarkItDown for {input_path}")
                    return _convert_with_markitdown(input_path)

                if analysis.complexity_score < effective_settings.pdf_complexity_threshold:
                    logger.info(
                        f"ACCURATE: PDF complexity {analysis.complexity_score:.3f} "
                        f"< threshold {effective_settings.pdf_complexity_threshold}, "
                        f"downscaling to Docling for {input_path}"
                    )
                    result = _convert_with_docling(input_path, self._get_docling())
                    if result:
                        return result
                    logger.info(f"ACCURATE fallback to LLM for {input_path}")
                    return _convert_with_llm(input_path, effective_settings)

            # Complex PDF or image → LLM
            result = _convert_with_llm(input_path, effective_settings)
            if result:
                return result
            logger.info(f"ACCURATE fallback to Docling for {input_path}")
            return _convert_with_docling(input_path, self._get_docling())
        else:
            result = _convert_with_docling(input_path, self._get_docling())
            if result:
                return result
            logger.info(f"ACCURATE fallback to MarkItDown for {input_path}")
            return _convert_with_markitdown(input_path)


#endregion

class ConversionService:
    """
    Service for converting files from source to destination.
    Uses Microsoft MarkItDown for file format conversion.
    """
    
    def __init__(self, settings: Settings):
        self._converter = Converter(settings)
        self._temp_dir: Optional[Path] = Path(settings.temp_dir) 
    
    def _ensure_temp_dir(self) -> Path:
        """Create and return temp directory for conversions."""
        if self._temp_dir is None or not self._temp_dir.exists():
            self._temp_dir = Path(tempfile.mkdtemp(prefix="converter_"))
        logger.debug(f"Using temporary directory: {self._temp_dir}")
        return self._temp_dir
    
    def cleanup(self) -> None:
        """Clean up temporary files."""
        if self._temp_dir and self._temp_dir.exists():
            logger.debug(f"Cleaning up temporary directory: {self._temp_dir}")
            shutil.rmtree(self._temp_dir)
            self._temp_dir = None
    
    async def analyze_conversion(
        self,
        source: SourceIntegration,
        destination: DestinationIntegration,
        source_extensions: list[str],
        source_folder: Optional[str] = None,
        destination_folder: Optional[str] = None
    ) -> ConversionAnalysis:
        """
        Analyze what files need to be converted.
        
        Args:
            source: Source integration instance
            destination: Destination integration instance
            source_extensions: File extensions to convert
            source_folder: Optional source folder filter
            destination_folder: Optional destination folder filter
            
        Returns:
            ConversionAnalysis with details of what needs conversion
        """
        # List source files
        source_files = await source.list_files(
            extensions=source_extensions,
            folder_path=source_folder
        )
        
        # List destination files (only markdown)
        dest_files = await destination.list_files(
            extensions=MARKDOWN_EXTENSIONS,
            folder_path=destination_folder
        )
        
        # Build set of already converted files (by stem)
        dest_stems = {Path(f.path).stem.lower() for f in dest_files}
        
        # Categorize source files
        files_to_convert = []
        already_converted = []
        
        for src_file in source_files:
            src_stem = Path(src_file.path).stem.lower()
            
            file_dict = {
                "name": src_file.name,
                "path": src_file.path,
                "modified_at": src_file.modified_at.isoformat(),
                "size": src_file.size
            }
            
            if src_stem in dest_stems:
                already_converted.append(file_dict)
            else:
                files_to_convert.append(file_dict)
        
        total = len(source_files)
        converted = len(already_converted)
        completion_pct = (converted / total * 100) if total > 0 else 100.0
        
        return ConversionAnalysis(
            source_files=[{
                "name": f.name,
                "path": f.path,
                "modified_at": f.modified_at.isoformat(),
                "size": f.size
            } for f in source_files],
            destination_files=[{
                "name": f.name,
                "path": f.path,
                "modified_at": f.modified_at.isoformat(),
                "size": f.size
            } for f in dest_files],
            files_to_convert=files_to_convert,
            already_converted=already_converted,
            completion_percentage=round(completion_pct, 2),
            total_source_files=total,
            total_converted_files=converted
        )
    
    def convert_file(self, input_path: Path, output_path: Path, strategy: ConversionStrategy = ConversionStrategy.FAST, job_llm: Optional[JobLLMSettings] = None) -> bool:
        """
        Convert a single file to Markdown.
        
        Args:
            input_path: Path to input file
            output_path: Path for output markdown file
            strategy: Conversion strategy to use
            job_llm: Optional per-job LLM overrides (ACCURATE strategy only)
            
        Returns:
            True if conversion was successful
        """
        try:
            result = self._converter.convert(input_path, strategy, job_llm)
            
            if result:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(result, encoding="utf-8")
                return True
            else:
                logger.warning(f"No content extracted from {input_path}")
                return False
                
        except Exception as e:
            logger.error(f"Conversion error for {input_path}: {e}")
            return False
    
    async def convert_file_async(self, input_path: Path, output_path: Path, strategy: ConversionStrategy = ConversionStrategy.FAST, job_llm: Optional[JobLLMSettings] = None) -> bool:
        """Async wrapper for file conversion."""
        return await asyncio.to_thread(self.convert_file, input_path, output_path, strategy, job_llm)
    
    async def run_conversion(
        self,
        job: ConversionJob,
        source: SourceIntegration,
        destination: DestinationIntegration,
        progress_callback: Optional[callable] = None
    ) -> ConversionJob:
        """
        Run a full conversion job.
        
        Args:
            job: Conversion job to execute
            source: Connected source integration
            destination: Connected destination integration
            progress_callback: Optional callback for progress updates
            
        Returns:
            Updated ConversionJob with results
        """
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        
        temp_dir = self._ensure_temp_dir()
        
        try:
            # Analyze what needs conversion
            analysis = await self.analyze_conversion(
                source,
                destination,
                job.source_extensions,
                job.source_folder,
                job.destination_folder
            )
            
            # Initialize file results
            job.file_results = []
            for file_info in analysis.files_to_convert:
                job.file_results.append(FileConversionResult(
                    source_path=file_info["path"],
                    file_size=file_info.get("size"),
                    status=FileConversionStatus.PENDING
                ))
            
            # Skip already converted
            for file_info in analysis.already_converted:
                job.file_results.append(FileConversionResult(
                    source_path=file_info["path"],
                    file_size=file_info.get("size"),
                    status=FileConversionStatus.SKIPPED
                ))
            
            job.update_stats()
            
            # Process files in batches
            batch_size = job.batch_size
            semaphore = asyncio.Semaphore(batch_size)
            processed_count = 0
            total_count = len(job.file_results)
            
            async def _process_file(result: FileConversionResult) -> None:
                nonlocal processed_count
                if result.status == FileConversionStatus.SKIPPED:
                    processed_count += 1
                    return
                
                async with semaphore:
                    result.started_at = datetime.utcnow()
                    
                    try:
                        # 1. Download
                        result.status = FileConversionStatus.DOWNLOADING
                        
                        file_info = FileInfo(
                            name=Path(result.source_path).name,
                            path=result.source_path,
                            modified_at=datetime.utcnow(),
                            size=result.file_size
                        )
                        
                        local_source_path = temp_dir / "source" / result.source_path.lstrip("/")
                        local_source_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        if not await source.download_file(file_info, local_source_path):
                            result.status = FileConversionStatus.FAILED
                            result.error_message = "Download failed"
                            return
                        
                        # 2. Convert
                        result.status = FileConversionStatus.CONVERTING
                        
                        md_filename = f"{Path(result.source_path).stem}.md"
                        parent_path = str(Path(result.source_path).parent).lstrip("/")
                        local_md_path = temp_dir / "converted" / parent_path / md_filename if parent_path else temp_dir / "converted" / md_filename
                        
                        strategy = ConversionStrategy(job.conversion_strategy) if isinstance(job.conversion_strategy, str) else job.conversion_strategy
                        if not await self.convert_file_async(local_source_path, local_md_path, strategy, job.llm_settings):
                            result.status = FileConversionStatus.FAILED
                            result.error_message = "Conversion failed"
                            return
                        
                        # 3. Upload
                        result.status = FileConversionStatus.UPLOADING
                        
                        # Determine destination path
                        dest_parent_str = str(Path(result.source_path).parent).lstrip("/")
                        if job.destination_folder:
                            dest_parent = Path(job.destination_folder) / dest_parent_str if dest_parent_str else Path(job.destination_folder)
                        else:
                            dest_parent = Path(dest_parent_str) if dest_parent_str else Path(".")
                        
                        dest_path = str(dest_parent / md_filename)
                        
                        if not await destination.upload_file(local_md_path, dest_path):
                            result.status = FileConversionStatus.FAILED
                            result.error_message = "Upload failed"
                            return
                        
                        # Success
                        result.status = FileConversionStatus.COMPLETED
                        result.destination_path = dest_path
                        result.converted_size = local_md_path.stat().st_size
                        result.completed_at = datetime.utcnow()
                        
                    except Exception as e:
                        result.status = FileConversionStatus.FAILED
                        result.error_message = str(e)
                        logger.error(f"Error processing {result.source_path}: {e}")
                    
                    finally:
                        processed_count += 1
                        job.update_stats()
                        if progress_callback:
                            await progress_callback(job, processed_count, total_count)
            
            await asyncio.gather(*[_process_file(r) for r in job.file_results])
            
            # Job completed
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            logger.error(f"Job failed: {e}")
        
        finally:
            self.cleanup()
        
        return job
    
    async def convert_single_file(
        self,
        source: SourceIntegration,
        destination: DestinationIntegration,
        file_path: str,
        destination_folder: Optional[str] = None,
        strategy: ConversionStrategy = ConversionStrategy.FAST
    ) -> FileConversionResult:
        """
        Convert and upload a single file.
        
        Args:
            source: Connected source integration
            destination: Connected destination integration
            file_path: Source file path
            destination_folder: Optional destination folder
            
        Returns:
            FileConversionResult with status
        """
        result = FileConversionResult(
            source_path=file_path,
            started_at=datetime.utcnow()
        )
        
        temp_dir = self._ensure_temp_dir()
        
        try:
            # Get file info
            all_files = await source.list_files()
            file_info = next((f for f in all_files if f.path == file_path), None)
            
            if not file_info:
                result.status = FileConversionStatus.FAILED
                result.error_message = f"File not found: {file_path}"
                return result
            
            result.file_size = file_info.size
            
            # Download
            result.status = FileConversionStatus.DOWNLOADING
            local_path = temp_dir / file_info.name
            
            if not await source.download_file(file_info, local_path):
                result.status = FileConversionStatus.FAILED
                result.error_message = "Download failed"
                return result
            
            # Convert
            result.status = FileConversionStatus.CONVERTING
            md_path = local_path.with_suffix(".md")
            
            if not await self.convert_file_async(local_path, md_path, strategy):
                result.status = FileConversionStatus.FAILED
                result.error_message = "Conversion failed"
                return result
            
            # Upload
            result.status = FileConversionStatus.UPLOADING
            
            dest_parent = Path(file_path).parent
            if destination_folder:
                if str(dest_parent) == "/":
                    dest_parent = Path(destination_folder)
                else:
                    dest_parent = Path(destination_folder) / str(dest_parent).lstrip("/")
            
            dest_path = str(dest_parent / md_path.name)
            
            if not await destination.upload_file(md_path, dest_path):
                result.status = FileConversionStatus.FAILED
                result.error_message = "Upload failed"
                return result
            
            result.status = FileConversionStatus.COMPLETED
            result.destination_path = dest_path
            result.converted_size = md_path.stat().st_size
            result.completed_at = datetime.utcnow()
            
        except Exception as e:
            result.status = FileConversionStatus.FAILED
            result.error_message = str(e)
        
        finally:
            self.cleanup()
        
        return result
