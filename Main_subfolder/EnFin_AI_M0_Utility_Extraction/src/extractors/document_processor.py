"""
Document processor for converting PDFs and images to standardized format.

Supports:
- PDF files (multi-page)
- Image files: PNG, JPG, JPEG, TIFF, BMP, GIF, WEBP
"""

import io
import logging
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
from PIL import Image
import fitz  # PyMuPDF

from config.settings import settings

logger = logging.getLogger(__name__)


class FileType(Enum):
    """Supported file types"""
    PDF = "pdf"
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    TIFF = "tiff"
    BMP = "bmp"
    GIF = "gif"
    WEBP = "webp"
    UNKNOWN = "unknown"


# File type detection based on magic bytes
FILE_SIGNATURES = {
    b'%PDF': FileType.PDF,
    b'\x89PNG': FileType.PNG,
    b'\xff\xd8\xff': FileType.JPG,
    b'GIF87a': FileType.GIF,
    b'GIF89a': FileType.GIF,
    b'BM': FileType.BMP,
    b'II*\x00': FileType.TIFF,  # Little-endian TIFF
    b'MM\x00*': FileType.TIFF,  # Big-endian TIFF
    b'RIFF': FileType.WEBP,  # WEBP starts with RIFF
}


@dataclass
class ProcessedDocument:
    """Represents a processed document (PDF or image)"""
    page_count: int
    page_images: Dict[int, bytes]  # page_number -> PNG bytes
    page_dimensions: Dict[int, Tuple[int, int]]  # page_number -> (width, height)
    metadata: Dict[str, str]
    file_type: FileType = FileType.PDF
    is_single_image: bool = False


class DocumentProcessor:
    """Processor for converting PDFs and images to standardized format"""

    def __init__(self):
        self.dpi = settings.PDF_IMAGE_DPI
        self.max_dimension = settings.MAX_IMAGE_DIMENSION

    @staticmethod
    def detect_file_type(file_bytes: bytes) -> FileType:
        """
        Detect file type from magic bytes.

        Args:
            file_bytes: Raw file bytes

        Returns:
            Detected FileType enum
        """
        for signature, file_type in FILE_SIGNATURES.items():
            if file_bytes.startswith(signature):
                # Special handling for WEBP (need to check further)
                if file_type == FileType.WEBP and b'WEBP' not in file_bytes[:12]:
                    continue
                return file_type
        return FileType.UNKNOWN

    def process_document(
        self,
        file_bytes: bytes,
        file_path: Optional[str] = None,
        pages_to_process: Optional[List[int]] = None
    ) -> ProcessedDocument:
        """
        Process any supported document type (PDF or image).

        Args:
            file_bytes: Raw file bytes
            file_path: Optional file path for extension-based type detection
            pages_to_process: Optional list of specific page numbers to process (1-indexed).
                              Only applicable for PDFs.

        Returns:
            ProcessedDocument with page images and metadata
        """
        # Detect file type
        file_type = self.detect_file_type(file_bytes)

        # Fallback to extension if detection fails
        if file_type == FileType.UNKNOWN and file_path:
            ext = file_path.lower().split('.')[-1]
            ext_mapping = {
                'pdf': FileType.PDF,
                'png': FileType.PNG,
                'jpg': FileType.JPG,
                'jpeg': FileType.JPEG,
                'tiff': FileType.TIFF,
                'tif': FileType.TIFF,
                'bmp': FileType.BMP,
                'gif': FileType.GIF,
                'webp': FileType.WEBP
            }
            file_type = ext_mapping.get(ext, FileType.UNKNOWN)

        logger.info(f"Detected file type: {file_type.value}")

        if file_type == FileType.PDF:
            return self.process_pdf(file_bytes, pages_to_process)
        elif file_type in [FileType.PNG, FileType.JPG, FileType.JPEG,
                          FileType.TIFF, FileType.BMP, FileType.GIF, FileType.WEBP]:
            return self.process_image(file_bytes, file_type)
        else:
            raise ValueError(f"Unsupported file type: {file_type.value}")

    def process_image(
        self,
        image_bytes: bytes,
        file_type: FileType
    ) -> ProcessedDocument:
        """
        Process an image file.

        Args:
            image_bytes: Raw image file bytes
            file_type: Detected file type

        Returns:
            ProcessedDocument with single page image
        """
        # Open image with PIL
        img = Image.open(io.BytesIO(image_bytes))

        # Convert to RGB if necessary (handle RGBA, P mode, etc.)
        if img.mode in ('RGBA', 'P', 'LA'):
            # Create white background for transparency
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        # Get original dimensions
        original_width, original_height = img.size

        # Resize if too large
        if original_width > self.max_dimension or original_height > self.max_dimension:
            scale = min(
                self.max_dimension / original_width,
                self.max_dimension / original_height
            )
            new_width = int(original_width * scale)
            new_height = int(original_height * scale)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            logger.info(f"Resized image from {original_width}x{original_height} to {new_width}x{new_height}")

        # Convert to PNG bytes
        output = io.BytesIO()
        img.save(output, format='PNG')
        png_bytes = output.getvalue()

        # Build metadata
        metadata = {
            "original_format": file_type.value,
            "original_width": str(original_width),
            "original_height": str(original_height),
            "page_count": "1"
        }

        return ProcessedDocument(
            page_count=1,
            page_images={1: png_bytes},
            page_dimensions={1: (img.width, img.height)},
            metadata=metadata,
            file_type=file_type,
            is_single_image=True
        )

    def process_pdf(
        self,
        pdf_bytes: bytes,
        pages_to_process: Optional[List[int]] = None
    ) -> ProcessedDocument:
        """
        Process a PDF document and convert pages to images.

        Args:
            pdf_bytes: Raw PDF file bytes
            pages_to_process: Optional list of specific page numbers to process (1-indexed).
                              If None, processes all pages.

        Returns:
            ProcessedDocument with page images and metadata
        """
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        page_count = len(doc)
        page_images = {}
        page_dimensions = {}
        metadata = {}

        # Extract metadata
        metadata = {
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "subject": doc.metadata.get("subject", ""),
            "creator": doc.metadata.get("creator", ""),
            "page_count": str(page_count)
        }

        # Determine which pages to process
        if pages_to_process is None:
            pages_to_convert = list(range(1, page_count + 1))
        else:
            pages_to_convert = [p for p in pages_to_process if 1 <= p <= page_count]

        # Convert each page to image
        for page_num in pages_to_convert:
            page = doc[page_num - 1]  # fitz uses 0-indexed pages

            # Calculate zoom factor for desired DPI
            zoom = self.dpi / 72  # 72 is default PDF DPI
            matrix = fitz.Matrix(zoom, zoom)

            # Render page to pixmap
            pixmap = page.get_pixmap(matrix=matrix)

            # Resize if too large
            if pixmap.width > self.max_dimension or pixmap.height > self.max_dimension:
                scale = min(
                    self.max_dimension / pixmap.width,
                    self.max_dimension / pixmap.height
                )
                new_width = int(pixmap.width * scale)
                new_height = int(pixmap.height * scale)

                # Re-render at smaller size
                zoom = zoom * scale
                matrix = fitz.Matrix(zoom, zoom)
                pixmap = page.get_pixmap(matrix=matrix)

            # Convert to PNG bytes
            png_bytes = pixmap.tobytes("png")

            page_images[page_num] = png_bytes
            page_dimensions[page_num] = (pixmap.width, pixmap.height)

        doc.close()

        return ProcessedDocument(
            page_count=page_count,
            page_images=page_images,
            page_dimensions=page_dimensions,
            metadata=metadata,
            file_type=FileType.PDF,
            is_single_image=False
        )

    def process_specific_pages(
        self,
        pdf_bytes: bytes,
        page_numbers: List[int]
    ) -> Dict[int, bytes]:
        """
        Process only specific pages from a PDF.

        Args:
            pdf_bytes: Raw PDF file bytes
            page_numbers: List of page numbers to process (1-indexed)

        Returns:
            Dictionary mapping page numbers to PNG bytes
        """
        result = self.process_pdf(pdf_bytes, pages_to_process=page_numbers)
        return result.page_images

    def get_page_count(self, pdf_bytes: bytes) -> int:
        """Get the number of pages in a PDF without full processing"""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        count = len(doc)
        doc.close()
        return count

    def create_thumbnail(
        self,
        pdf_bytes: bytes,
        page_number: int = 1,
        max_size: int = 500
    ) -> bytes:
        """
        Create a thumbnail of a PDF page.

        Args:
            pdf_bytes: Raw PDF file bytes
            page_number: Page to thumbnail (1-indexed)
            max_size: Maximum dimension in pixels

        Returns:
            PNG bytes of thumbnail
        """
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        if page_number < 1 or page_number > len(doc):
            doc.close()
            raise ValueError(f"Invalid page number: {page_number}")

        page = doc[page_number - 1]

        # Calculate zoom to fit in max_size
        rect = page.rect
        zoom = min(max_size / rect.width, max_size / rect.height)
        matrix = fitz.Matrix(zoom, zoom)

        pixmap = page.get_pixmap(matrix=matrix)
        png_bytes = pixmap.tobytes("png")

        doc.close()
        return png_bytes

    def create_all_thumbnails(
        self,
        pdf_bytes: bytes,
        max_size: int = 500
    ) -> Dict[int, bytes]:
        """
        Create thumbnails for all pages.

        Args:
            pdf_bytes: Raw PDF file bytes
            max_size: Maximum dimension in pixels

        Returns:
            Dictionary mapping page numbers to thumbnail PNG bytes
        """
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        thumbnails = {}

        for page_num in range(1, len(doc) + 1):
            page = doc[page_num - 1]
            rect = page.rect
            zoom = min(max_size / rect.width, max_size / rect.height)
            matrix = fitz.Matrix(zoom, zoom)
            pixmap = page.get_pixmap(matrix=matrix)
            thumbnails[page_num] = pixmap.tobytes("png")

        doc.close()
        return thumbnails


class ImagePreparer:
    """Helper for preparing images for LLM input"""

    @staticmethod
    def prepare_images_for_prompt(
        page_images: Dict[int, bytes],
        page_numbers: Optional[List[int]] = None
    ) -> List[Tuple[int, bytes]]:
        """
        Prepare images for LLM prompt in correct format.

        Args:
            page_images: Dictionary of page_number -> image bytes
            page_numbers: Optional list to filter/order pages

        Returns:
            List of (page_number, image_bytes) tuples, sorted by page number
        """
        if page_numbers is None:
            page_numbers = sorted(page_images.keys())

        return [
            (page_num, page_images[page_num])
            for page_num in sorted(page_numbers)
            if page_num in page_images
        ]

    @staticmethod
    def generate_page_images_description(
        page_images: Dict[int, bytes]
    ) -> str:
        """
        Generate a text description of provided page images for prompts.

        Args:
            page_images: Dictionary of page_number -> image bytes

        Returns:
            Description string listing the pages
        """
        page_numbers = sorted(page_images.keys())

        if len(page_numbers) == 1:
            return f"Page {page_numbers[0]} image is provided."
        else:
            pages_str = ", ".join(str(p) for p in page_numbers[:-1])
            pages_str += f" and {page_numbers[-1]}"
            return f"Page images for pages {pages_str} are provided."
