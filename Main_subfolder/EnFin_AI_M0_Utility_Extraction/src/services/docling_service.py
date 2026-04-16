"""
Docling service for extracting text from documents (PDF, DOCX) while preserving format.

Replaces Textract OCR with Docling's AI-powered document understanding.
Supports PDF and DOCX formats. Image files are rejected.

OCR engine is configurable via OCR_ENGINE env var:
- "easyocr"  (default) — EasyOCR with models downloaded to /tmp/easyocr
- "rapidocr" — RapidOCR with bundled ONNX models from rapidocr-onnxruntime
"""

import os
import logging
import tempfile
from typing import Dict
from dataclasses import dataclass

# Set environment variables before importing anything
# These paths use /tmp/ — models download at runtime on cold start.
os.environ.setdefault("HF_HOME", "/tmp/huggingface")
os.environ.setdefault("EASYOCR_MODULE_PATH", "/tmp/easyocr")
os.environ.setdefault("MODULE_PATH", "/tmp/modules")
os.environ.setdefault("TORCH_HOME", "/tmp/torch")
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")

# Fix pin_memory warning at its root: EasyOCR's internal PyTorch DataLoader
# uses pin_memory=True by default, which is meaningless on CPU-only Lambda.
# Monkeypatch DataLoader.__init__ to force pin_memory=False BEFORE any
# docling/easyocr import creates DataLoader instances.
from torch.utils import data as _torch_data

_original_dataloader_init = _torch_data.DataLoader.__init__


def _patched_dataloader_init(self, *args, **kwargs):
    kwargs["pin_memory"] = False
    _original_dataloader_init(self, *args, **kwargs)


_torch_data.DataLoader.__init__ = _patched_dataloader_init

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    RapidOcrOptions,
    AcceleratorOptions,
    AcceleratorDevice,
)

# Import PictureItem for skipping images during text extraction
try:
    from docling_core.types.doc import PictureItem
except ImportError:
    PictureItem = None

from config.settings import settings

logger = logging.getLogger(__name__)


# Supported document formats (not images)
SUPPORTED_EXTENSIONS = {'.pdf', '.docx', '.doc', '.pptx', '.xlsx', '.html'}

# Image extensions that should be rejected
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.gif', '.webp'}


class UnsupportedFileTypeError(Exception):
    """Raised when an unsupported file type (e.g., image) is provided."""
    pass


@dataclass
class DoclingResult:
    """Result from Docling text extraction."""
    full_markdown: str
    page_texts: Dict[int, str]
    has_images: bool
    page_count: int


class DoclingService:
    """Service for extracting text from documents using Docling with configurable OCR."""

    def __init__(self):
        # Build pipeline options with OCR enabled
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.do_table_structure = True
        pipeline_options.images_scale = 2.0  # ~432 DPI — better OCR on scanned PDFs
        pipeline_options.ocr_options = self._build_ocr_options()
        pipeline_options.generate_picture_images = True
        pipeline_options.generate_page_images = True

        # Force CPU — avoids pin_memory warning on Lambda (no GPU)
        pipeline_options.accelerator_options = AcceleratorOptions(
            num_threads=4,
            device=AcceleratorDevice.CPU,
        )

        # Initialize converter once (reused across warm Lambda invocations)
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
        logger.info(
            "DoclingService initialized (ocr_engine=%s, lang=%s, force_ocr=%s)",
            settings.OCR_ENGINE, settings.OCR_LANG, settings.FORCE_OCR
        )

    def _build_ocr_options(self):
        """
        Build OCR options based on the configured OCR_ENGINE setting.

        Returns EasyOcrOptions or RapidOcrOptions depending on settings.OCR_ENGINE.
        """
        engine = settings.OCR_ENGINE.lower()

        if engine == "easyocr":
            ocr_options = EasyOcrOptions(
                use_gpu=None,  # None = auto-detect from accelerator_options.device (avoids deprecation warning)
                lang=settings.OCR_LANG,
                model_storage_directory=os.environ.get(
                    "EASYOCR_MODULE_PATH", "/tmp/easyocr"
                ),
                download_enabled=True,
                force_full_page_ocr=settings.FORCE_OCR,
            )
            logger.info("Using EasyOCR engine (lang=%s)", settings.OCR_LANG)
            return ocr_options

        elif engine == "rapidocr":
            # RapidOcrOptions uses default bundled ONNX models from
            # rapidocr-onnxruntime package. No separate model download needed.
            ocr_options = RapidOcrOptions(
                force_full_page_ocr=settings.FORCE_OCR,
            )
            logger.info("Using RapidOCR engine")
            return ocr_options

        else:
            logger.warning(
                "Unknown OCR engine '%s', falling back to EasyOCR", engine
            )
            return EasyOcrOptions(
                use_gpu=None,
                lang=settings.OCR_LANG,
                model_storage_directory=os.environ.get(
                    "EASYOCR_MODULE_PATH", "/tmp/easyocr"
                ),
                download_enabled=True,
                force_full_page_ocr=settings.FORCE_OCR,
            )

    def extract_text(self, file_bytes: bytes, file_path: str) -> DoclingResult:
        """
        Extract text from a document while preserving format.

        Args:
            file_bytes: Raw document bytes
            file_path: Original file path (used for extension detection)

        Returns:
            DoclingResult with extracted text content

        Raises:
            UnsupportedFileTypeError: If the file is an image or unsupported format
        """
        # Detect file type from extension
        ext = self._get_extension(file_path)

        # Reject image files
        if ext in IMAGE_EXTENSIONS:
            raise UnsupportedFileTypeError(
                f"Image files ({ext}) are not supported. Only document formats "
                f"(PDF, DOCX) are accepted."
            )

        # Validate supported format
        if ext not in SUPPORTED_EXTENSIONS:
            raise UnsupportedFileTypeError(
                f"Unsupported file format: {ext}. "
                f"Supported formats: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
            )

        # Write to temp file (Docling needs a file path)
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix=ext, delete=False, dir="/tmp"
            ) as tmp_file:
                tmp_file.write(file_bytes)
                tmp_path = tmp_file.name

            logger.info(f"Processing document with Docling: {file_path} ({ext})")

            # Convert document
            result = self.converter.convert(tmp_path)
            doc = result.document

            # Extract full markdown — traverse_pictures=True so OCR text
            # inside PictureItem children (scanned pages) is included
            # instead of being replaced with "<!-- image -->"
            full_markdown = doc.export_to_markdown(traverse_pictures=True)

            # Extract per-page text
            page_texts = self._extract_page_texts(doc)

            # Check for embedded images
            has_images = self._check_for_images(doc)

            # Get page count
            page_count = len(doc.pages) if hasattr(doc, 'pages') and doc.pages else len(page_texts)

            logger.info(
                f"Docling extraction complete: {page_count} pages, "
                f"has_images: {has_images}, "
                f"markdown_length: {len(full_markdown)}"
            )

            # Log the extracted markdown for debugging
            logger.info("Docling extracted markdown:\n%s", full_markdown)

            return DoclingResult(
                full_markdown=full_markdown,
                page_texts=page_texts,
                has_images=has_images,
                page_count=page_count
            )

        finally:
            # Clean up temp file
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _get_extension(self, file_path: str) -> str:
        """Get lowercase file extension from path."""
        if not file_path:
            return ''
        _, ext = os.path.splitext(file_path.lower())
        return ext

    def _extract_page_texts(self, doc) -> Dict[int, str]:
        """Extract text content per page from Docling document."""
        page_texts = {}

        try:
            # traverse_pictures=True makes iterate_items descend into
            # PictureItem children where OCR text lives for scanned pages.
            # We still skip the PictureItem node itself (no direct text),
            # but its child TextItem nodes are yielded and captured below.
            for element, _level in doc.iterate_items(traverse_pictures=True):
                if PictureItem and isinstance(element, PictureItem):
                    continue

                # Get page number from element's provenance
                page_no = None
                if hasattr(element, 'prov') and element.prov:
                    for prov in element.prov:
                        if hasattr(prov, 'page_no'):
                            page_no = prov.page_no
                            break

                if page_no is None:
                    continue

                # Get text content — pass doc to export_to_markdown to
                # avoid "without doc argument is deprecated" warning
                text = ''
                if hasattr(element, 'text'):
                    text = element.text
                elif hasattr(element, 'export_to_markdown'):
                    try:
                        text = element.export_to_markdown(doc=doc)
                    except TypeError:
                        try:
                            text = element.export_to_markdown()
                        except TypeError:
                            continue

                if text:
                    if page_no not in page_texts:
                        page_texts[page_no] = ''
                    page_texts[page_no] += text + '\n'

        except Exception as e:
            logger.warning(f"Error extracting per-page text: {e}")
            # Fallback: put all text on page 1
            full_text = doc.export_to_markdown(traverse_pictures=True)
            if full_text:
                page_texts[1] = full_text

        return page_texts

    def _check_for_images(self, doc) -> bool:
        """Check if document contains embedded images/pictures."""
        if not PictureItem:
            return False
        try:
            for element, _level in doc.iterate_items():
                if isinstance(element, PictureItem):
                    return True
        except Exception:
            pass
        return False
