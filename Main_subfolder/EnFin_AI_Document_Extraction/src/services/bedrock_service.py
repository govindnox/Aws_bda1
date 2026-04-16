"""
AWS Bedrock service for LLM inference via the Converse API.

Supports multimodal (images + text) and text-only requests.
The boto3 client is lazy-initialised to avoid cold-start overhead.

Author: Reet Roy
Version: 1.0.0
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Lazy-loaded Bedrock Runtime client
_bedrock_client = None


def _get_client():
    """Lazy-initialise the Bedrock Runtime client."""
    global _bedrock_client
    if _bedrock_client is None:
        import boto3
        from config import config

        _bedrock_client = boto3.client("bedrock-runtime", region_name=config.aws.region)
    return _bedrock_client


# =========================================================================
# Response model
# =========================================================================


@dataclass
class LLMResponse:
    """Structured response from a Bedrock Converse call."""

    reasoning: str
    output: Dict[str, Any]
    raw_response: str
    model_used: str
    input_tokens: int
    output_tokens: int


# =========================================================================
# Invocation helpers
# =========================================================================


def invoke_with_images(
    prompt: str,
    images: List[Tuple[int, bytes]],
    app_no: str,
    process: str,
    model_id: Optional[str] = None,
    system_prompt: Optional[str] = None,
) -> LLMResponse:
    """Call Bedrock Converse API with page images + text prompt.

    Content order: ``[images first] → [text prompt last]``.

    Args:
        prompt: Text prompt (placed after images).
        images: ``[(page_number, png_bytes), ...]``.
        app_no: Application number for request metadata tracking.
        process: Process name for request metadata tracking.
        model_id: Override model (defaults to
            ``config.model.extraction_model``).
        system_prompt: Optional system-level instruction.

    Returns:
        Parsed ``LLMResponse``.
    """
    from config import config

    model = model_id or config.model.extraction_model
    client = _get_client()

    # Build content blocks: images first, then text
    content: List[Dict[str, Any]] = []
    for page_num, image_bytes in images:
        image_format = _detect_image_format(image_bytes)
        content.append({"text": f"[Page {page_num}]"})
        content.append(
            {
                "image": {
                    "format": image_format,
                    "source": {"bytes": image_bytes},
                }
            }
        )
    content.append({"text": prompt})

    messages = [{"role": "user", "content": content}]

    request_kwargs: Dict[str, Any] = {
        "modelId": model,
        "messages": messages,
        "inferenceConfig": {
            "maxTokens": config.model.max_tokens,
            "temperature": config.model.temperature,
        },
    }
    request_kwargs["requestMetadata"] = {"app_no": app_no, "process": process}
    if system_prompt:
        request_kwargs["system"] = [{"text": system_prompt}]

    logger.info(
        "Calling Converse API: model=%s, images=%d, prompt=\n%s\n------------------------",
        model,
        len(images),
        prompt,
    )
    response = client.converse(**request_kwargs)
    parsed = _parse_converse_response(response, model)

    logger.info(
        "Converse API response: model=%s, in_tokens=%d, out_tokens=%d\n"
        "--- LLM Output JSON ---\n%s\n----------------------------",
        model,
        parsed.input_tokens,
        parsed.output_tokens,
        json.dumps(parsed.output, indent=2),
    )
    return parsed


def invoke_text_only(
    prompt: str,
    app_no: str,
    process: str,
    model_id: Optional[str] = None,
    system_prompt: Optional[str] = None,
) -> LLMResponse:
    """Call Bedrock Converse API with text only (no images).

    Args:
        prompt: Text prompt.
        app_no: Application number for request metadata tracking.
        process: Process name for request metadata tracking.
        model_id: Override model.
        system_prompt: Optional system-level instruction.

    Returns:
        Parsed ``LLMResponse``.
    """
    from config import config

    model = model_id or config.model.extraction_model
    client = _get_client()

    messages = [{"role": "user", "content": [{"text": prompt}]}]

    request_kwargs: Dict[str, Any] = {
        "modelId": model,
        "messages": messages,
        "inferenceConfig": {
            "maxTokens": config.model.max_tokens,
            "temperature": config.model.temperature,
        },
    }
    request_kwargs["requestMetadata"] = {"app_no": app_no, "process": process}
    if system_prompt:
        request_kwargs["system"] = [{"text": system_prompt}]

    logger.info(
        "Calling Converse API (text-only): model=%s, prompt=\n%s\n-----------------------------------------",
        model,
        prompt,
    )
    response = client.converse(**request_kwargs)
    return _parse_converse_response(response, model)


# =========================================================================
# Internal helpers
# =========================================================================


def _detect_image_format(image_bytes: bytes) -> str:
    """Detect image format from magic bytes for the Converse API.

    Args:
        image_bytes: The raw image bytes.

    Returns:
        A string representing the format (e.g., "png", "jpeg").
    """
    if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if image_bytes[:2] == b"\xff\xd8":
        return "jpeg"
    if image_bytes[:4] == b"GIF8":
        return "gif"
    if image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "webp"
    # Default to PNG (fitz outputs PNG)
    return "png"


def _parse_converse_response(response: Dict[str, Any], model_id: str) -> LLMResponse:
    """Parse Converse API response into structured ``LLMResponse``.

    Args:
        response: The raw response dictionary from the Converse API.
        model_id: The ID of the model that generated the response.

    Returns:
        A parsed LLMResponse object.
    """

    output_message = response.get("output", {}).get("message", {})
    raw_text = ""
    for block in output_message.get("content", []):
        if "text" in block:
            raw_text += block["text"]

    usage = response.get("usage", {})
    input_tokens = usage.get("inputTokens", 0)
    output_tokens = usage.get("outputTokens", 0)

    # Extract reasoning section
    reasoning = _extract_section(raw_text, "reasoning")

    # Parse output JSON
    json_str = extract_json_from_response(raw_text)
    output_json = json.loads(json_str)

    return LLMResponse(
        reasoning=reasoning,
        output=output_json,
        raw_response=raw_text,
        model_used=model_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def _extract_section(text: str, tag: str) -> str:
    """Extract content between XML-style ``<tag>…</tag>``.

    Args:
        text: The raw text containing the XML-style extraction target.
        tag: The tag name to extract the inner content for.

    Returns:
        The extracted content as a string, stripped of leading/trailing whitespace.
    """
    pattern = rf"<{tag}>([\s\S]*?)</{tag}>"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def extract_json_from_response(text: str) -> str:
    """Extract JSON string from LLM response.

    Uses ``json.JSONDecoder.raw_decode`` to locate every valid JSON
    object in *text* and returns the **last** (largest-end) one.

    Args:
        text: Raw LLM response.

    Returns:
        Cleaned JSON string, or ``"{}"`` if no valid JSON is found.
    """
    decoder = json.JSONDecoder()
    candidates = []

    # Find all start indices of '{'
    start_indices = [i for i, char in enumerate(text) if char == "{"]

    for idx in start_indices:
        try:
            _, end_idx = decoder.raw_decode(text, idx=idx)
            candidates.append(
                {
                    "start": idx,
                    "end": end_idx,
                }
            )
        except json.JSONDecodeError:
            continue

    if not candidates:
        logger.warning("Could not parse any JSON from LLM response")
        return "{}"

    # Pick the last valid block: sort by end (desc), then start (asc)
    best = sorted(candidates, key=lambda x: (-x["end"], x["start"]))[0]
    json_str = text[best["start"]: best["end"]]
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", json_str)
