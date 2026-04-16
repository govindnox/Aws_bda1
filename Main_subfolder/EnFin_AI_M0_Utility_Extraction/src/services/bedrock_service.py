"""
AWS Bedrock service for LLM inference using the Converse API.

Uses the unified Converse API for multimodal (images + text) requests.
Images are passed as raw bytes (SDK handles base64 encoding).
"""

import json
import boto3
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import re
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Structured response from LLM"""
    reasoning: str
    output: Dict[str, Any]
    raw_response: str
    model_used: str
    input_tokens: int
    output_tokens: int


class BedrockService:
    """Service for interacting with AWS Bedrock via the Converse API"""

    def __init__(self):
        self.client = boto3.client(
            'bedrock-runtime',
            region_name=settings.REGION
        )
        self.max_tokens = settings.MODEL_MAX_TOKENS
        self.temperature = settings.MODEL_TEMPERATURE

    def invoke_with_images(
        self,
        prompt: str,
        images: List[Tuple[int, bytes]],  # List of (page_number, image_bytes)
        model_id: str,
        system_prompt: Optional[str] = None
    ) -> LLMResponse:
        """
        Invoke Bedrock model with images + text via Converse API.

        Content structure: [images first] → [text prompt last]

        Args:
            prompt: The prompt text (placed after images in content)
            images: List of (page_number, image_bytes) tuples
            model_id: Bedrock model identifier
            system_prompt: Optional system prompt

        Returns:
            LLMResponse with parsed reasoning and output
        """
        # Build content blocks: images first, then text
        content = []

        # Add images first (raw bytes - SDK handles base64)
        for page_num, image_bytes in images:
            image_format = self._detect_image_format(image_bytes)

            content.append({
                "text": f"[Page {page_num}]"
            })
            content.append({
                "image": {
                    "format": image_format,
                    "source": {
                        "bytes": image_bytes
                    }
                }
            })

        # Add prompt text last
        content.append({
            "text": prompt
        })

        # Build messages
        messages = [{"role": "user", "content": content}]

        # Build request kwargs
        request_kwargs = {
            "modelId": model_id,
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": self.max_tokens,
                "temperature": self.temperature,
            }
        }

        # Optional system prompt
        if system_prompt:
            request_kwargs["system"] = [{"text": system_prompt}]

        logger.info("Calling Converse API with model: %s, images: %d", model_id, len(images))

        # Invoke via Converse API
        response = self.client.converse(**request_kwargs)

        # Parse and log response
        parsed = self._parse_converse_response(response, model_id)
        logger.info(
            "Converse API raw response (model=%s, input_tokens=%d, output_tokens=%d):\n%s",
            model_id, parsed.input_tokens, parsed.output_tokens, parsed.raw_response
        )
        return parsed

    def invoke_text_only(
        self,
        prompt: str,
        model_id: str,
        system_prompt: Optional[str] = None
    ) -> LLMResponse:
        """
        Invoke Bedrock model with text only via Converse API.

        Args:
            prompt: The prompt text
            model_id: Bedrock model identifier
            system_prompt: Optional system prompt

        Returns:
            LLMResponse with parsed reasoning and output
        """
        messages = [{
            "role": "user",
            "content": [{"text": prompt}]
        }]

        request_kwargs = {
            "modelId": model_id,
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": self.max_tokens,
                "temperature": self.temperature,
            }
        }

        if system_prompt:
            request_kwargs["system"] = [{"text": system_prompt}]

        logger.info(f"Calling Converse API (text-only) with model: {model_id}")

        response = self.client.converse(**request_kwargs)
        return self._parse_converse_response(response, model_id)

    def _detect_image_format(self, image_bytes: bytes) -> str:
        """Detect image format from magic bytes (for Converse API format field)"""
        if image_bytes[:8] == b'\x89PNG\r\n\x1a\n':
            return "png"
        elif image_bytes[:2] == b'\xff\xd8':
            return "jpeg"
        elif image_bytes[:4] == b'GIF8':
            return "gif"
        elif image_bytes[:4] == b'RIFF' and image_bytes[8:12] == b'WEBP':
            return "webp"
        else:
            # Default to PNG
            return "png"

    def _parse_converse_response(
        self,
        response: Dict[str, Any],
        model_id: str
    ) -> LLMResponse:
        """Parse Converse API response and extract reasoning and output sections"""

        # Extract text from response
        output_message = response.get("output", {}).get("message", {})
        raw_text = ""
        for content_block in output_message.get("content", []):
            if "text" in content_block:
                raw_text += content_block["text"]

        # Extract usage info
        usage = response.get("usage", {})
        input_tokens = usage.get("inputTokens", 0)
        output_tokens = usage.get("outputTokens", 0)

        # Parse reasoning section
        reasoning = self._extract_section(raw_text, "reasoning")

        # Parse output section — try XML tags first, then markdown code blocks
        output_json = {}

        # Strategy 1: XML-style tags (<output>...</output>)
        for tag in ["output", "extraction", "validation_result", "decision"]:
            section = self._extract_section(raw_text, tag)
            if section:
                try:
                    output_json = json.loads(section)
                    break
                except json.JSONDecodeError:
                    json_match = re.search(r'\{[\s\S]*\}', section)
                    if json_match:
                        try:
                            output_json = json.loads(json_match.group())
                            break
                        except json.JSONDecodeError:
                            continue

        # Strategy 2: Markdown code blocks (```json ... ```)
        if not output_json:
            code_block_match = re.search(
                r'```(?:json)?\s*\n(\{[\s\S]*?\})\s*\n```', raw_text
            )
            if code_block_match:
                try:
                    output_json = json.loads(code_block_match.group(1))
                    logger.info("Parsed output from markdown code block")
                except json.JSONDecodeError:
                    pass

        # Strategy 3: Find any top-level JSON object in the raw text
        if not output_json:
            json_match = re.search(r'\{[\s\S]*\}', raw_text)
            if json_match:
                try:
                    output_json = json.loads(json_match.group())
                    logger.info("Parsed output from raw JSON in response")
                except json.JSONDecodeError:
                    logger.warning("Could not parse any JSON from LLM response")

        return LLMResponse(
            reasoning=reasoning,
            output=output_json,
            raw_response=raw_text,
            model_used=model_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens
        )

    def _extract_section(self, text: str, tag: str) -> str:
        """Extract content between XML-style tags"""
        pattern = rf'<{tag}>([\s\S]*?)</{tag}>'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
        return ""


class PromptBuilder:
    """Helper class for building prompts with proper formatting"""

    @staticmethod
    def build_docling_extraction_prompt(
        docling_text: str,
        utility_fields_guide: str,
        extraction_prompt_template: str
    ) -> str:
        """
        Build the extraction prompt with Docling text and utility guide.

        Args:
            docling_text: Full markdown text from Docling
            utility_fields_guide: Guide for all utility fields
            extraction_prompt_template: The extraction prompt template

        Returns:
            Formatted prompt string
        """
        result = extraction_prompt_template.replace("{{docling_text}}", docling_text)
        result = result.replace("{{utility_fields_guide}}", utility_fields_guide)
        return result

    @staticmethod
    def format_extracted_data(
        extraction_result: Dict[str, Any],
        include_reasoning: bool = False
    ) -> str:
        """Format extraction result for logging/debugging"""
        fields = extraction_result.get("fields", {})

        lines = []
        for field_name, field_data in fields.items():
            lines.append(f"**{field_name}**:")
            lines.append(f"  - Value: {field_data.get('value', 'N/A')}")
            lines.append(f"  - Page: {field_data.get('page', 'N/A')}")
            lines.append(f"  - Section: {field_data.get('section', 'N/A')}")
            lines.append(f"  - Confidence: {field_data.get('confidence', 'N/A')}")
            lines.append("")

        return "\n".join(lines)
