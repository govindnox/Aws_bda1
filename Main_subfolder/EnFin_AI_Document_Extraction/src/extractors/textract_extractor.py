"""
Textract-based text extractor — fast OCR using AnalyzeDocument Layout.

Calls ``textract.analyze_document(FeatureTypes=['LAYOUT'])`` per page
image, then walks the LAYOUT blocks to produce structured markdown.

LINE blocks with ``Confidence < textract_min_confidence`` are
filtered out of the final result.

The boto3 client is lazy-initialised to avoid cold-start overhead.

Author: Reet Roy
Version: 1.0.0
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Lazy-loaded Textract client
_textract_client = None


def _get_client():
    """Lazy-initialise the Textract client."""
    global _textract_client
    if _textract_client is None:
        import boto3
        from config import config

        _textract_client = boto3.client("textract", region_name=config.aws.region)
    return _textract_client


# =========================================================================
# Public API
# =========================================================================


def extract_text_textract(
    page_images: Dict[int, bytes],
    min_confidence: float = 80.0,
) -> Any:
    """Extract text from page images using Textract AnalyzeDocument.

    Calls ``AnalyzeDocument(FeatureTypes=['LAYOUT'])`` per page image.
    LINE blocks below ``min_confidence`` are excluded from the result.

    Args:
        page_images: ``{page_number: png_bytes}`` from DocumentProcessor.
        min_confidence: Minimum LINE confidence threshold (0–100).
            Lines below this are dropped from the markdown output.

    Returns:
        ``TextExtractionResult`` (same as Docling path).
    """
    from extractors.text_extractor import TextExtractionResult

    all_page_texts: Dict[int, str] = {}
    full_sections: List[str] = []

    for page_num in sorted(page_images.keys()):
        image_bytes = page_images[page_num]
        logger.info(
            "Textract AnalyzeDocument: page %d (%d bytes, " "min_confidence=%.1f)",
            page_num,
            len(image_bytes),
            min_confidence,
        )

        response = _call_analyze_document(image_bytes)
        blocks = response.get("Blocks", [])

        page_markdown = _blocks_to_markdown(blocks, page_num, min_confidence)
        all_page_texts[page_num] = page_markdown
        full_sections.append(page_markdown)

    full_markdown = "\n\n---\n\n".join(full_sections)

    logger.info(
        "Textract extraction complete: %d pages, markdown_len=%d",
        len(all_page_texts),
        len(full_markdown),
    )

    return TextExtractionResult(
        full_markdown=full_markdown,
        page_texts=all_page_texts,
        has_images=False,
        page_count=len(all_page_texts),
    )


# =========================================================================
# Textract API call
# =========================================================================


def _call_analyze_document(image_bytes: bytes) -> Dict[str, Any]:
    """Call Textract AnalyzeDocument with LAYOUT feature.

    Args:
        image_bytes: Single page image bytes (PNG/JPEG).

    Returns:
        Textract response dict containing ``Blocks``.
    """
    client = _get_client()
    response = client.analyze_document(
        Document={"Bytes": image_bytes},
        FeatureTypes=["LAYOUT"],
    )
    return response


# =========================================================================
# Layout → Markdown conversion
# =========================================================================

# Markdown templates per LAYOUT block type
_LAYOUT_FORMATTERS = {
    "LAYOUT_TITLE": "## {text}",
    "LAYOUT_SECTION_HEADER": "### {text}",
    "LAYOUT_HEADER": "**{text}**",
    "LAYOUT_TEXT": "{text}",
    "LAYOUT_PAGE_NUMBER": "_{text}_",
    "LAYOUT_KEY_VALUE": "**{text}**",
}


def _blocks_to_markdown(
    blocks: List[Dict[str, Any]],
    page_num: int,
    min_confidence: float,
) -> str:
    """Convert Textract Layout blocks to structured markdown.

    Algorithm:
        1. Build ``block_map`` for O(1) lookup.
        2. Filter LINE blocks below ``min_confidence``.
        3. Collect LAYOUT blocks for this page, sorted by Top position.
        4. Walk each LAYOUT block, resolve child text, format as md.

    Args:
        blocks: All Textract Blocks from the response.
        page_num: Current page number (for logging).
        min_confidence: Min confidence for LINE inclusion.

    Returns:
        Markdown string for this page.
    """
    # Step 1: Create a lookup dictionary for O(1) resolution of child relationships
    # Textract uses IDs to link LAYOUT blocks to their constituent LINE/WORD blocks
    block_map = {b["Id"]: b for b in blocks}

    # Step 2: Pre-filter LINE blocks based on the confidence threshold
    # If a line is below the threshold, its ID won't be in this set, so it gets dropped
    # when resolving child texts.
    confident_line_ids = {
        b["Id"]
        for b in blocks
        if b["BlockType"] == "LINE" and b.get("Confidence", 0) >= min_confidence
    }

    # Step 3: Collect only LAYOUT blocks (structural elements)
    # The AnalyzeDocument Layout feature provides structural wrappers around LINE blocks.
    # Note: AnalyzeDocument operates on single images, so Page is always 1 or missing.
    layout_blocks = [
        b
        for b in blocks
        if b["BlockType"].startswith("LAYOUT") and b.get("Page", 1) == 1
    ]
    # Step 4: Sort LAYOUT blocks top-to-bottom for correct reading order
    # Textract does not guarantee the blocks list is sorted strictly by vertical position
    layout_blocks.sort(
        key=lambda x: x.get("Geometry", {}).get("BoundingBox", {}).get("Top", 0)
    )

    md_lines: List[str] = []

    for lb in layout_blocks:
        block_type = lb["BlockType"]
        child_ids = _get_child_ids(lb)

        # Branch based on block type for specialized formatting
        if block_type == "LAYOUT_TABLE":
            # Tables require pairing LINE children into | label | value | format
            table_md = _format_table(child_ids, block_map, confident_line_ids)
            if table_md:
                md_lines.append(table_md)

        elif block_type == "LAYOUT_LIST":
            # Lists require recursive resolution (LAYOUT_LIST -> LAYOUT_TEXT -> LINE)
            list_md = _format_list(child_ids, block_map, confident_line_ids)
            if list_md:
                md_lines.append(list_md)

        elif block_type == "LAYOUT_FIGURE":
            # Figures (images/charts) might have caption lines as children
            fig_text = _resolve_line_texts(child_ids, block_map, confident_line_ids)
            if fig_text:
                md_lines.append(f"[Figure: {fig_text}]")

        else:
            # Standard structural blocks (TEXT, TITLE, HEADER)
            # Fetch all child LINE text, joined by spaces
            text = _resolve_line_texts(child_ids, block_map, confident_line_ids)
            if not text:
                continue
            # Look up the markdown template (e.g. ## {text} for TITLE)
            template = _LAYOUT_FORMATTERS.get(block_type, "{text}")
            md_lines.append(template.format(text=text))

    return "\n\n".join(md_lines)


# =========================================================================
# Child resolution helpers
# =========================================================================


def _get_child_ids(block: Dict[str, Any]) -> List[str]:
    """Extract CHILD relationship IDs from a block.

    Args:
        block: A textract layout block containing relationships.

    Returns:
        A list of child block IDs.
    """
    for rel in block.get("Relationships", []):
        if rel["Type"] == "CHILD":
            return rel["Ids"]
    return []


def _resolve_line_texts(
    child_ids: List[str],
    block_map: Dict[str, Dict[str, Any]],
    confident_ids: set,
) -> str:
    """Resolve child IDs to concatenated LINE text.

    Handles both direct LINE children and nested LAYOUT children
    (e.g., LAYOUT_LIST → LAYOUT_TEXT → LINE).

    Args:
        child_ids: IDs of child blocks.
        block_map: Full block lookup map.
        confident_ids: Set of LINE IDs above confidence threshold.

    Returns:
        Concatenated text from qualifying LINE children.
    """
    texts: List[str] = []

    for cid in child_ids:
        child = block_map.get(cid)
        if not child:
            continue

        if child["BlockType"] == "LINE":
            # Direct LINE child — append only if it passed the confidence check
            if cid in confident_ids:
                texts.append(child.get("Text", ""))
        elif child["BlockType"].startswith("LAYOUT"):
            # Nested structure found (common in LISTs).
            # We must recursively fetch the LINEs inside this nested LAYOUT block.
            nested_ids = _get_child_ids(child)
            nested_text = _resolve_line_texts(nested_ids, block_map, confident_ids)
            if nested_text:
                texts.append(nested_text)

    # Join extracted pieces with a space
    return " ".join(texts)


def _format_table(
    child_ids: List[str],
    block_map: Dict[str, Dict[str, Any]],
    confident_ids: set,
) -> str:
    """Format LAYOUT_TABLE children as a markdown table.

    TABLE children are LINE blocks. They are paired as
    label–value rows (2 columns).

    Args:
        child_ids: IDs of child LINE blocks.
        block_map: Full block lookup.
        confident_ids: Confidence-filtered LINE IDs.

    Returns:
        Markdown table string or empty string.
    """
    lines: List[str] = []
    for cid in child_ids:
        child = block_map.get(cid)
        # Ensure the child is precisely a LINE block, ignoring invalid links
        if not child or child["BlockType"] != "LINE":
            continue
        # Drop the cell entirely if word confidence was too low
        if cid not in confident_ids:
            continue
        lines.append(child.get("Text", ""))

    if not lines:
        return ""

    # Attempt to pair linear lines as a 2-column (Label | Value) markdown table.
    # Utility bill tables extracted via AnalyzeDocument(Layout) often return
    # key-value pairs sequentially rather than as grid structures.
    if len(lines) >= 2:
        rows: List[str] = []
        rows.append("| Item | Value |")
        rows.append("|------|-------|")
        # Step through in chunks of 2 (label, value)
        for i in range(0, len(lines) - 1, 2):
            label = lines[i]
            value = lines[i + 1] if i + 1 < len(lines) else ""
            rows.append(f"| {label} | {value} |")
        # Handle trailing odd line (label without a value)
        if len(lines) % 2 == 1:
            rows.append(f"| {lines[-1]} | |")
        return "\n".join(rows)

    # Fallback: if only one cell exists, return it as plain text instead of a broken table
    return lines[0]


def _format_list(
    child_ids: List[str],
    block_map: Dict[str, Dict[str, Any]],
    confident_ids: set,
) -> str:
    """Format LAYOUT_LIST as markdown bullet points.

    LIST children are nested LAYOUT_TEXT blocks (not LINE).
    Each LAYOUT_TEXT is resolved recursively and prefixed with ``- ``.

    Args:
        child_ids: IDs of child blocks (usually LAYOUT_TEXT).
        block_map: Full block lookup.
        confident_ids: Confidence-filtered LINE IDs.

    Returns:
        Markdown list string or empty string.
    """
    items: List[str] = []
    for cid in child_ids:
        child = block_map.get(cid)
        if not child:
            continue

        if child["BlockType"].startswith("LAYOUT"):
            # Typical structure: LAYOUT_LIST has LAYOUT_TEXT elements as bullets
            # We recurse to get the actual text from the inner LINE blocks
            nested_ids = _get_child_ids(child)
            text = _resolve_line_texts(nested_ids, block_map, confident_ids)
            if text:
                items.append(f"- {text}")
        elif child["BlockType"] == "LINE" and cid in confident_ids:
            # Fallback: strict LINE children inside a list (rare in Layout mode)
            items.append(f"- {child.get('Text', '')}")

    return "\n".join(items)
