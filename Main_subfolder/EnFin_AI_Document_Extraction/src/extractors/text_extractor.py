"""
Text extractor — wraps Docling for format-preserving text extraction.

**Lazy imports**: ``docling``, ``torch``, and ``easyocr`` are imported
inside method bodies (not at module level) to prevent Lambda init
timeouts.  These packages take several seconds to load.

Supports PDF and DOCX.  For image files that were embedded into a PDF
by ``DocumentProcessor``, Docling processes the resulting PDF and
``doc.export_to_markdown(traverse_pictures=True)`` ensures OCR text
from embedded images is included.

Author: Reet Roy
Version: 1.0.0
"""

import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Dict

logger = logging.getLogger(__name__)

# Singleton converter — initialised on first use
_converter = None


# =========================================================================
# Result model
# =========================================================================


@dataclass
class TextExtractionResult:
    """Result of Docling text extraction."""

    full_markdown: str
    page_texts: Dict[int, str]
    has_images: bool
    page_count: int


# =========================================================================
# Public API
# =========================================================================


def extract_text(
    file_bytes: bytes,
    path: str,
) -> TextExtractionResult:
    """Extract text from a document using Docling.

    Heavy packages are imported lazily inside this function.

    Args:
        file_bytes: Raw document bytes (PDF or image).
        path: File path for extension detection (Docling
            accepts ``.pdf``, ``.png``, ``.jpg``, ``.tiff``, etc.).

    Returns:
        ``TextExtractionResult`` with per-page markdown.
    """
    converter = _get_converter()
    ext = os.path.splitext(path.lower())[1] or ".pdf"

    # Write to temp file — Docling requires a file path
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=ext,
            delete=False,
            dir="/tmp",  # nosec B108
        ) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        logger.info("Processing document with Docling: %s (%s)", path, ext)

        result = converter.convert(tmp_path)
        doc = result.document

        # Full markdown — traverse_pictures=True includes OCR text
        # from PictureItem children (scanned pages / embedded images)
        full_markdown = doc.export_to_markdown(traverse_pictures=True)

        # Per-page text
        page_texts = _extract_page_texts(doc)

        # Check for images
        has_images = _check_for_images(doc)

        page_count = (
            len(doc.pages) if hasattr(doc, "pages") and doc.pages else len(page_texts)
        )

        logger.info(
            "Docling extraction complete: %d pages, has_images=%s, "
            "markdown_length=%d",
            page_count,
            has_images,
            len(full_markdown),
        )

        return TextExtractionResult(
            full_markdown=full_markdown,
            page_texts=page_texts,
            has_images=has_images,
            page_count=page_count,
        )

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# =========================================================================
# Lazy initialisation
# =========================================================================


def _get_converter():
    """Lazy-initialise the Docling DocumentConverter.

    All heavy imports (``docling``, ``torch``, ``easyocr``) happen here
    — **not** at module level — to avoid Lambda init timeouts.
    """
    global _converter
    if _converter is not None:
        return _converter

    from config import config

    # Set environment variables BEFORE importing docling/torch/easyocr
    os.environ.setdefault("HF_HOME", "/tmp/huggingface")  # nosec B108
    os.environ.setdefault("EASYOCR_MODULE_PATH", "/tmp/easyocr")  # nosec B108
    os.environ.setdefault("MODULE_PATH", "/tmp/modules")  # nosec B108
    os.environ.setdefault("TORCH_HOME", "/tmp/torch")  # nosec B108
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")

    # Monkey-patch DataLoader to force pin_memory=False (CPU Lambda)
    from torch.utils import data as _torch_data

    _orig_init = _torch_data.DataLoader.__init__

    def _patched_init(self, *args, **kwargs):
        """Monkey-patched __init__ for DataLoader to disable pin_memory.

        Args:
            *args: Positional arguments for DataLoader.
            **kwargs: Keyword arguments for DataLoader.

        Returns:
            None
        """
        kwargs["pin_memory"] = False
        _orig_init(self, *args, **kwargs)

    _torch_data.DataLoader.__init__ = _patched_init

    # Now import docling
    from docling.document_converter import (
        DocumentConverter,
        ImageFormatOption,
        PdfFormatOption,
    )
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import (
        PdfPipelineOptions,
        EasyOcrOptions,
        RapidOcrOptions,
        AcceleratorOptions,
        AcceleratorDevice,
    )

    # Build pipeline options
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.images_scale = 2.0
    pipeline_options.generate_picture_images = True
    pipeline_options.generate_page_images = True

    # OCR engine selection
    engine = config.ocr.engine.lower()
    if engine == "rapidocr":
        pipeline_options.ocr_options = RapidOcrOptions(
            force_full_page_ocr=config.ocr.force_ocr,
        )
        logger.info("Using RapidOCR engine")
    else:
        pipeline_options.ocr_options = EasyOcrOptions(
            use_gpu=None,
            lang=config.ocr.languages,
            model_storage_directory=os.environ.get(
                "EASYOCR_MODULE_PATH",
                "/tmp/easyocr",  # nosec B108
            ),
            download_enabled=True,
            force_full_page_ocr=config.ocr.force_ocr,
        )
        logger.info("Using EasyOCR engine (lang=%s)", config.ocr.languages)

    # Force CPU
    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=4,
        device=AcceleratorDevice.CPU,
    )

    _converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
            InputFormat.IMAGE: ImageFormatOption(pipeline_options=pipeline_options),
        }
    )

    logger.info(
        "DoclingService initialised (engine=%s, force_ocr=%s)",
        engine,
        config.ocr.force_ocr,
    )
    return _converter


# =========================================================================
# Internal helpers
# =========================================================================


def _extract_page_texts(doc) -> Dict[int, str]:  # noqa: C901
    """Extract text content per page from a Docling document.

    Args:
        doc: The Docling document object.

    Returns:
        A dictionary mapping page numbers to extracted text.
    """
    try:
        from docling_core.types.doc import PictureItem
    except ImportError:
        PictureItem = None

    page_texts: Dict[int, str] = {}

    try:
        for element, _level in doc.iterate_items(traverse_pictures=True):
            if PictureItem and isinstance(element, PictureItem):
                continue

            page_no = None
            if hasattr(element, "prov") and element.prov:
                for prov in element.prov:
                    if hasattr(prov, "page_no"):
                        page_no = prov.page_no
                        break

            if page_no is None:
                continue

            text = ""
            if hasattr(element, "text"):
                text = element.text
            elif hasattr(element, "export_to_markdown"):
                try:
                    text = element.export_to_markdown(doc=doc)
                except TypeError:
                    try:
                        text = element.export_to_markdown()
                    except TypeError:
                        continue

            if text:
                page_texts.setdefault(page_no, "")
                page_texts[page_no] += text + "\n"

    except Exception as exc:
        logger.warning("Error extracting per-page text: %s", exc)
        full_text = doc.export_to_markdown(traverse_pictures=True)
        if full_text:
            page_texts[1] = full_text

    return page_texts


def _check_for_images(doc) -> bool:
    """Check whether the document contains embedded images.

    Args:
        doc: The Docling document object.

    Returns:
        True if the document contains images, False otherwise.
    """
    try:
        from docling_core.types.doc import PictureItem
    except ImportError:
        return False

    try:
        for element, _level in doc.iterate_items():
            if isinstance(element, PictureItem):
                return True
    except Exception:
        pass  # nosec B110
    return False
