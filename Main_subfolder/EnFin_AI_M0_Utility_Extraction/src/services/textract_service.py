"""
AWS Textract service for extracting high-confidence reference text from utility bills.
"""

import re
import boto3
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from ..config.settings import settings


@dataclass
class TextractWord:
    """Represents a word extracted by Textract"""
    text: str
    confidence: float
    page: int
    bounding_box: Dict[str, float] = field(default_factory=dict)


@dataclass
class TextractLine:
    """Represents a line of text extracted by Textract"""
    text: str
    confidence: float
    page: int
    words: List[TextractWord] = field(default_factory=list)


@dataclass
class TextractKeyValue:
    """Represents a key-value pair extracted by Textract"""
    key: str
    value: str
    key_confidence: float
    value_confidence: float
    page: int


@dataclass
class TextractTable:
    """Represents a table extracted by Textract"""
    rows: List[List[str]]
    page: int
    confidence: float


@dataclass
class TextractResult:
    """Complete Textract extraction result for a page"""
    page_number: int
    raw_text: str  # All high-confidence text concatenated
    structured_text: str  # Text preserving line structure
    lines: List[TextractLine]
    key_value_pairs: List[TextractKeyValue]
    tables: List[TextractTable]
    candidate_values: Dict[str, List[str]]  # Pattern matches
    word_count: int
    average_confidence: float


class TextractService:
    """Service for interacting with AWS Textract"""

    def __init__(self):
        self.client = boto3.client('textract', region_name=settings.REGION)
        self.min_confidence = settings.TEXTRACT_MIN_CONFIDENCE
        self.extract_candidates = settings.EXTRACT_CANDIDATE_VALUES
        self.candidate_patterns = settings.CANDIDATE_PATTERNS

    def analyze_page(self, image_bytes: bytes, page_number: int) -> TextractResult:
        """
        Analyze a single page image using Textract.

        Args:
            image_bytes: PNG/JPEG image bytes
            page_number: Page number (1-indexed)

        Returns:
            TextractResult with extracted text and metadata
        """
        # Call Textract API
        response = self.client.analyze_document(
            Document={'Bytes': image_bytes},
            FeatureTypes=settings.TEXTRACT_FEATURES
        )

        # Parse response
        return self._parse_textract_response(response, page_number)

    def analyze_pages(
        self,
        page_images: Dict[int, bytes]
    ) -> Dict[int, TextractResult]:
        """
        Analyze multiple pages.

        Args:
            page_images: Dictionary mapping page numbers to image bytes

        Returns:
            Dictionary mapping page numbers to TextractResult
        """
        results = {}
        for page_num, image_bytes in page_images.items():
            results[page_num] = self.analyze_page(image_bytes, page_num)
        return results

    def _parse_textract_response(
        self,
        response: Dict[str, Any],
        page_number: int
    ) -> TextractResult:
        """Parse Textract API response into structured result"""

        blocks = response.get('Blocks', [])

        # Extract words with confidence filtering
        words = []
        high_confidence_words = []
        for block in blocks:
            if block['BlockType'] == 'WORD':
                word = TextractWord(
                    text=block['Text'],
                    confidence=block['Confidence'],
                    page=page_number,
                    bounding_box=block.get('Geometry', {}).get('BoundingBox', {})
                )
                words.append(word)
                if word.confidence >= self.min_confidence:
                    high_confidence_words.append(word)

        # Extract lines with confidence filtering
        lines = []
        for block in blocks:
            if block['BlockType'] == 'LINE':
                if block['Confidence'] >= self.min_confidence:
                    line = TextractLine(
                        text=block['Text'],
                        confidence=block['Confidence'],
                        page=page_number
                    )
                    lines.append(line)

        # Sort lines by vertical position for structured text
        lines_sorted = sorted(
            lines,
            key=lambda l: blocks[self._find_block_index(blocks, l.text)].get(
                'Geometry', {}
            ).get('BoundingBox', {}).get('Top', 0)
            if self._find_block_index(blocks, l.text) >= 0 else 0
        )

        # Extract key-value pairs (FORMS feature)
        key_value_pairs = self._extract_key_values(blocks, page_number)

        # Extract tables (TABLES feature)
        tables = self._extract_tables(blocks, page_number)

        # Create raw text (all high-confidence words)
        raw_text = ' '.join([w.text for w in high_confidence_words])

        # Create structured text (preserving lines)
        structured_text = '\n'.join([l.text for l in lines_sorted])

        # Extract candidate values if enabled
        candidate_values = {}
        if self.extract_candidates:
            candidate_values = self._extract_candidates(raw_text)

        # Calculate average confidence
        avg_confidence = (
            sum(w.confidence for w in high_confidence_words) /
            len(high_confidence_words)
            if high_confidence_words else 0.0
        )

        return TextractResult(
            page_number=page_number,
            raw_text=raw_text,
            structured_text=structured_text,
            lines=lines_sorted,
            key_value_pairs=key_value_pairs,
            tables=tables,
            candidate_values=candidate_values,
            word_count=len(high_confidence_words),
            average_confidence=avg_confidence
        )

    def _find_block_index(self, blocks: List[Dict], text: str) -> int:
        """Find block index by text (helper for sorting)"""
        for i, block in enumerate(blocks):
            if block.get('Text') == text:
                return i
        return -1

    def _extract_key_values(
        self,
        blocks: List[Dict[str, Any]],
        page_number: int
    ) -> List[TextractKeyValue]:
        """Extract key-value pairs from Textract FORMS output"""
        key_values = []

        # Build block ID to block mapping
        block_map = {block['Id']: block for block in blocks}

        # Find KEY_VALUE_SET blocks
        for block in blocks:
            if block['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in block.get('EntityTypes', []):
                    # This is a key block, find its value
                    key_text = self._get_text_from_relationships(
                        block, block_map, 'CHILD'
                    )
                    value_block = self._get_value_block(block, block_map)
                    value_text = ""
                    value_confidence = 0.0

                    if value_block:
                        value_text = self._get_text_from_relationships(
                            value_block, block_map, 'CHILD'
                        )
                        value_confidence = value_block.get('Confidence', 0.0)

                    if key_text and block.get('Confidence', 0) >= self.min_confidence:
                        key_values.append(TextractKeyValue(
                            key=key_text,
                            value=value_text,
                            key_confidence=block.get('Confidence', 0.0),
                            value_confidence=value_confidence,
                            page=page_number
                        ))

        return key_values

    def _get_text_from_relationships(
        self,
        block: Dict[str, Any],
        block_map: Dict[str, Dict],
        relationship_type: str
    ) -> str:
        """Extract text from block relationships"""
        text_parts = []
        relationships = block.get('Relationships', [])

        for relationship in relationships:
            if relationship['Type'] == relationship_type:
                for child_id in relationship.get('Ids', []):
                    child_block = block_map.get(child_id, {})
                    if child_block.get('BlockType') == 'WORD':
                        text_parts.append(child_block.get('Text', ''))

        return ' '.join(text_parts)

    def _get_value_block(
        self,
        key_block: Dict[str, Any],
        block_map: Dict[str, Dict]
    ) -> Optional[Dict[str, Any]]:
        """Find the value block associated with a key block"""
        relationships = key_block.get('Relationships', [])

        for relationship in relationships:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship.get('Ids', []):
                    return block_map.get(value_id)

        return None

    def _extract_tables(
        self,
        blocks: List[Dict[str, Any]],
        page_number: int
    ) -> List[TextractTable]:
        """Extract tables from Textract TABLES output"""
        tables = []
        block_map = {block['Id']: block for block in blocks}

        for block in blocks:
            if block['BlockType'] == 'TABLE':
                table_data = self._parse_table_block(block, block_map)
                if table_data:
                    tables.append(TextractTable(
                        rows=table_data,
                        page=page_number,
                        confidence=block.get('Confidence', 0.0)
                    ))

        return tables

    def _parse_table_block(
        self,
        table_block: Dict[str, Any],
        block_map: Dict[str, Dict]
    ) -> List[List[str]]:
        """Parse a TABLE block into rows and cells"""
        cells = {}
        relationships = table_block.get('Relationships', [])

        for relationship in relationships:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship.get('Ids', []):
                    cell_block = block_map.get(child_id, {})
                    if cell_block.get('BlockType') == 'CELL':
                        row_index = cell_block.get('RowIndex', 0)
                        col_index = cell_block.get('ColumnIndex', 0)
                        cell_text = self._get_text_from_relationships(
                            cell_block, block_map, 'CHILD'
                        )
                        cells[(row_index, col_index)] = cell_text

        if not cells:
            return []

        # Convert to list of lists
        max_row = max(r for r, c in cells.keys())
        max_col = max(c for r, c in cells.keys())

        table_data = []
        for row in range(1, max_row + 1):
            row_data = []
            for col in range(1, max_col + 1):
                row_data.append(cells.get((row, col), ''))
            table_data.append(row_data)

        return table_data

    def _extract_candidates(self, text: str) -> Dict[str, List[str]]:
        """Extract candidate values matching configured patterns"""
        candidates = {}

        for pattern_name, pattern in self.candidate_patterns.items():
            matches = re.findall(pattern, text)
            if matches:
                # Deduplicate while preserving order
                unique_matches = list(dict.fromkeys(matches))
                candidates[pattern_name] = unique_matches

        return candidates

    def format_reference_text_for_prompt(
        self,
        result: TextractResult,
        include_tables: bool = True,
        include_key_values: bool = True,
        include_candidates: bool = True
    ) -> str:
        """
        Format Textract result into reference text for LLM prompt.

        Args:
            result: TextractResult from analyze_page
            include_tables: Include formatted tables
            include_key_values: Include key-value pairs
            include_candidates: Include candidate values

        Returns:
            Formatted reference text string
        """
        sections = []

        # Structured text (line by line)
        sections.append("=== TEXT CONTENT (High Confidence OCR) ===")
        sections.append(result.structured_text)

        # Key-value pairs
        if include_key_values and result.key_value_pairs:
            sections.append("\n=== KEY-VALUE PAIRS DETECTED ===")
            for kv in result.key_value_pairs:
                sections.append(f"  {kv.key}: {kv.value}")

        # Tables
        if include_tables and result.tables:
            sections.append("\n=== TABLES DETECTED ===")
            for i, table in enumerate(result.tables):
                sections.append(f"Table {i + 1}:")
                for row in table.rows:
                    sections.append("  | " + " | ".join(row) + " |")

        # Candidate values
        if include_candidates and result.candidate_values:
            sections.append("\n=== CANDIDATE VALUES (Pattern Matches) ===")
            for pattern_name, values in result.candidate_values.items():
                sections.append(f"  {pattern_name}: {', '.join(values)}")

        return "\n".join(sections)
