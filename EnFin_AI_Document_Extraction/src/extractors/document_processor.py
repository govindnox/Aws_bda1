"""
Document processor — converts PDFs **and images** to page images.

For standalone image files (PNG, JPG, TIFF, etc.) the raw bytes are
stored directly as a single page.  Both Docling and Textract accept
images natively, so no intermediate PDF conversion is needed.

For PDFs, pages are rendered to PNG via fitz (PyMuPDF).

Author: Reet Roy
Version: 1.1.0
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =========================================================================
# File type detection
# =========================================================================


class FileType(Enum):
    """Supported file types."""

    PDF = "pdf"
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    TIFF = "tiff"
    BMP = "bmp"
    GIF = "gif"
    WEBP = "webp"
    UNKNOWN = "unknown"


# Magic-byte signatures
_FILE_SIGNATURES = {
    b"%PDF": FileType.PDF,
    b"\x89PNG": FileType.PNG,
    b"\xff\xd8\xff": FileType.JPG,
    b"GIF87a": FileType.GIF,
    b"GIF89a": FileType.GIF,
    b"BM": FileType.BMP,
    b"II*\x00": FileType.TIFF,
    b"MM\x00*": FileType.TIFF,
    b"RIFF": FileType.WEBP,
}

# Image types handled natively (no PDF conversion)
_IMAGE_TYPES = {
    FileType.PNG,
    FileType.JPG,
    FileType.JPEG,
    FileType.TIFF,
    FileType.BMP,
    FileType.GIF,
    FileType.WEBP,
}

# Extension → FileType mapping
_EXT_MAP = {
    "pdf": FileType.PDF,
    "png": FileType.PNG,
    "jpg": FileType.JPG,
    "jpeg": FileType.JPEG,
    "tiff": FileType.TIFF,
    "tif": FileType.TIFF,
    "bmp": FileType.BMP,
    "gif": FileType.GIF,
    "webp": FileType.WEBP,
}


# =========================================================================
# Result model
# =========================================================================


@dataclass
class ProcessedDocument:
    """Represents a processed document ready for the pipeline."""

    page_count: int
    page_images: Dict[int, bytes]  # page_number → PNG bytes
    page_dimensions: Dict[int, Tuple[int, int]]  # page_number → (w, h)
    metadata: Dict[str, str]
    file_type: FileType = FileType.PDF
    is_image: bool = False  # True if source was a standalone image


# =========================================================================
# Processor
# =========================================================================


class DocumentProcessor:
    """Converts source files into standardised page images.

    * PDF → page rendering via fitz (PyMuPDF).
    * Image → stored directly as a single page (native support).
    """

    def __init__(self):
        """Initialise the DocumentProcessor, loading configs."""
        from config import config

        self._dpi = config.image.pdf_dpi
        self._max_dim = config.image.max_dimension

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_document(
        self,
        file_bytes: bytes,
        path: str,
        app_no: str = "",
        pages_to_process: Optional[List[int]] = None,
    ) -> ProcessedDocument:
        """Process any supported document type.

        Args:
            file_bytes: Raw file bytes.
            path: Original S3 key (used for extension detection).
            app_no: Application number (for logging context).
            pages_to_process: Subset of pages (1-indexed). PDF only.

        Returns:
            ``ProcessedDocument`` with page images.
        """
        file_type = self._detect_type(file_bytes, path)
        logger.info("Detected file type: %s", file_type.value)

        if file_type in _IMAGE_TYPES:
            return self._process_image(file_bytes, file_type)
        if file_type == FileType.PDF:
            return self._process_pdf(file_bytes, file_type, pages_to_process)

        raise ValueError(f"Unsupported file type: {file_type.value}")

    # ------------------------------------------------------------------
    # Image processing (no conversion — store raw bytes)
    # ------------------------------------------------------------------

    def _process_image(
        self,
        image_bytes: bytes,
        file_type: FileType,
    ) -> ProcessedDocument:
        """Process a standalone image file.

        The raw image bytes are stored as page 1.  Both Docling and
        Textract accept images natively so no PDF embedding is needed.

        If the image exceeds ``max_dimension``, it is resized via fitz.

        Args:
            image_bytes: Raw image file bytes.
            file_type: Detected image file type.

        Returns:
            ``ProcessedDocument`` with one page.
        """
        import fitz  # lazy import — only for dimension check / resize

        filetype_hint = (
            "jpeg" if file_type in (FileType.JPG, FileType.JPEG) else file_type.value
        )
        img_doc = fitz.open(stream=image_bytes, filetype=filetype_hint)
        page = img_doc[0]
        width = int(page.rect.width)
        height = int(page.rect.height)

        # Resize if exceeds max dimension
        if width > self._max_dim or height > self._max_dim:
            scale = min(
                self._max_dim / width,
                self._max_dim / height,
            )
            matrix = fitz.Matrix(scale, scale)
            pixmap = page.get_pixmap(matrix=matrix)
            image_bytes = pixmap.tobytes("png")
            width, height = pixmap.width, pixmap.height

        img_doc.close()

        logger.info(
            "Image processed directly: %dx%d, type=%s",
            width,
            height,
            file_type.value,
        )

        return ProcessedDocument(
            page_count=1,
            page_images={1: image_bytes},
            page_dimensions={1: (width, height)},
            metadata={"page_count": "1"},
            file_type=file_type,
            is_image=True,
        )

    # ------------------------------------------------------------------
    # PDF processing (render pages via fitz)
    # ------------------------------------------------------------------

    def _process_pdf(
        self,
        file_bytes: bytes,
        file_type: FileType,
        pages_to_process: Optional[List[int]] = None,
    ) -> ProcessedDocument:
        """Render PDF pages to PNG images via fitz.

        Args:
            file_bytes: Raw PDF bytes.
            file_type: Always ``FileType.PDF``.
            pages_to_process: Subset of pages (1-indexed).

        Returns:
            ``ProcessedDocument`` with per-page images.
        """
        import fitz  # lazy import

        doc = fitz.open(stream=file_bytes, filetype="pdf")
        page_count = len(doc)
        page_images: Dict[int, bytes] = {}
        page_dimensions: Dict[int, Tuple[int, int]] = {}

        target_pages = (
            pages_to_process if pages_to_process else list(range(1, page_count + 1))
        )
        target_pages = [p for p in target_pages if 1 <= p <= page_count]

        for page_num in target_pages:
            page = doc[page_num - 1]
            zoom = self._dpi / 72
            matrix = fitz.Matrix(zoom, zoom)
            pixmap = page.get_pixmap(matrix=matrix)

            # Resize if necessary
            if pixmap.width > self._max_dim or pixmap.height > self._max_dim:
                scale = min(
                    self._max_dim / pixmap.width,
                    self._max_dim / pixmap.height,
                )
                zoom *= scale
                matrix = fitz.Matrix(zoom, zoom)
                pixmap = page.get_pixmap(matrix=matrix)

            page_images[page_num] = pixmap.tobytes("png")
            page_dimensions[page_num] = (pixmap.width, pixmap.height)

        metadata = {
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "page_count": str(page_count),
        }
        doc.close()

        return ProcessedDocument(
            page_count=page_count,
            page_images=page_images,
            page_dimensions=page_dimensions,
            metadata=metadata,
            file_type=file_type,
            is_image=False,
        )

    # ------------------------------------------------------------------
    # File type detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_type(file_bytes: bytes, path: str) -> FileType:
        """Detect file type from magic bytes, falling back to extension.

        Args:
            file_bytes: Raw file bytes to inspect for magic signatures.
            path: File path used as a fallback for extension matching.

        Returns:
            The detected FileType enum.
        """
        for sig, ftype in _FILE_SIGNATURES.items():
            if file_bytes.startswith(sig):
                if ftype == FileType.WEBP and b"WEBP" not in file_bytes[:12]:
                    continue
                return ftype

        # Fallback to extension
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        return _EXT_MAP.get(ext, FileType.UNKNOWN)
