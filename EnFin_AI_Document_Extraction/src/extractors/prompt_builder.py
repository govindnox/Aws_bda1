"""
Prompt builder — dynamically constructs the LLM extraction prompt
from the DynamoDB process configuration.

The prompt uses the XML-tag structure:
``<persona>``, ``<goal>``, ``<task>``, ``<information>``,
``<guardrails>``, ``<steps_to_follow>``, ``<reasoning>``, ``<output>``

Merge fields:
    ``{docling_text}``   — replaced with per-page Docling markdown
    ``{field_details}``  — replaced with entity definitions from config

Author: Reet Roy
Version: 1.0.0
"""

import json
import logging
from typing import Dict, List

from models.data_models import ProcessConfig

logger = logging.getLogger(__name__)


def build_extraction_prompt(
    process_config: ProcessConfig,
    docling_page_texts: Dict[int, str],
) -> str:
    """Build the full extraction prompt for the LLM.

    Priority for prompt source:
    1. Retrieve from Bedrock if prompt_arn is set
    2. Fallback to extraction_prompt from DynamoDB
    3. Use default template

    If the prompt contains merge-field placeholders (``{docling_text}``
    and ``{field_details}`` or ``{{docling_text}}`` and ``{{field_details}}``),
    they are substituted.

    Args:
        process_config: Loaded process configuration.
        docling_page_texts: Per-page markdown text from Docling.

    Returns:
        Fully assembled prompt string.
    """
    # Build the field_details JSON from entity configs
    field_details_json = _build_field_details(process_config)

    # Build page-partitioned Docling text
    docling_text = _build_docling_text(docling_page_texts)

    # Try Bedrock Prompt Management first
    prompt_template = None
    if process_config.prompt_arn:
        try:
            from services.bedrock_prompt_service import get_prompt_by_arn

            prompt_template = get_prompt_by_arn(
                prompt_arn=process_config.prompt_arn,
                fallback_prompt=process_config.extraction_prompt,
            )
            logger.info(
                "Using Bedrock prompt: ARN=%s, version=%s",
                process_config.prompt_arn,
                process_config.prompt_version,
            )
        except Exception as e:
            logger.warning("Failed to load Bedrock prompt, using DynamoDB: %s", e)
            prompt_template = process_config.extraction_prompt

    # Fallback to DynamoDB prompt if Bedrock not configured
    if not prompt_template:
        prompt_template = process_config.extraction_prompt

    if not prompt_template:
        logger.warning(
            "No extraction prompt found for process '%s'; using default",
            process_config.process,
        )
        prompt_template = _get_default_prompt_template()

    # Substitute merge fields - support both {} and {{}} syntax
    prompt = prompt_template.replace("{{docling_text}}", docling_text)
    prompt = prompt.replace("{{field_details}}", field_details_json)
    prompt = prompt.replace("{docling_text}", docling_text)  # Backward compat
    prompt = prompt.replace("{field_details}", field_details_json)

    return prompt


def build_extraction_prompt_for_batch(
    process_config: ProcessConfig,
    page_nums: List[int],
    page_texts: Dict[int, str],
) -> str:
    """Build prompt for a specific batch of pages.

    Used when the number of page images exceeds the per-call LLM
    limit and pages are processed in batches.

    Args:
        process_config: Loaded process configuration.
        page_nums: Page numbers in this batch.
        page_texts: Per-page markdown text from Docling.

    Returns:
        Fully assembled prompt string for the batch.
    """
    batch_texts = {p: page_texts.get(p, "") for p in page_nums}
    return build_extraction_prompt(process_config, batch_texts)


# =========================================================================
# Internal helpers
# =========================================================================


def _build_field_details(process_config: ProcessConfig) -> str:
    """Serialise entity configs into a JSON block for the prompt.

    Args:
        process_config: The process configuration containing entity rules.

    Returns:
        A JSON string containing the structured field details.
    """
    details = {}
    for name, entity in process_config.entities.items():
        entry: Dict = {
            "identification": entity.identification,
        }
        if entity.expected_labels:
            entry["expected_labels"] = entity.expected_labels
        if entity.location_hints:
            entry["location_hints"] = entity.location_hints
        if entity.keywords:
            entry["keywords"] = entity.keywords
        details[name] = entry

    return json.dumps({"field_details": details}, indent=4)


def _build_docling_text(page_texts: Dict[int, str]) -> str:
    """Build page-partitioned Docling text with XML page tags.

    Args:
        page_texts: A dictionary mapping page numbers to their markdown text.

    Returns:
        A string of concatenated page texts wrapped in XML page tags.
    """
    sections = []
    for page_num in sorted(page_texts.keys()):
        text = page_texts.get(page_num, "").strip()
        content = text if text else "[No text extracted for this page]"
        sections.append(f"<page{page_num}>\n{content}\n</page{page_num}>")
    return "\n\n".join(sections)


def _get_default_prompt_template() -> str:
    """Fallback prompt template when none is configured in DynamoDB.

    Uses the XML-tag structure requested by the user.

    Returns:
        The default XML-structured prompt template string.
    """
    return """The page images of the document are provided above.

<persona>
You are an expert OCR extractor, capable of extracting information \
from pdf, images clearly and distinctly.
</persona>

<goal>
Your goal is to analyze the document, check whether it is a utility \
bill or not, if utility bill then provide as much information as you \
got from the document, as per the <output>.
</goal>

<task>
You are analyzing a document. You have TWO sources of information:
1. **Page Images** (above) - Use these to detect layout, identify \
sections, logos, and locate where each field is on the page and \
visual reading for exact digits and characters.
2. **Document Text** (below) - Use this for the exact text values. \
Document text is MORE RELIABLE than visual reading for exact digits \
and characters.
Also you can be provided with other language documents (e.g. Spanish), \
you have to decode the language, understand the meaning, and properly \
fill the value with that of our requirement.
</task>

<information>
Utility Bill is a monthly invoice or statement outlining charges for \
essential household services. This could include water bill, \
electricity bill, gas bill, or any document provided by public \
utility companies.
</information>

<guardrails>
- If for any field you are not able to find the value exactly, then \
leave that field as empty
- Do not create any kind of data by yourself, ONLY provide what is \
there in the document
</guardrails>

<steps_to_follow>
## STEP 1: IDENTIFY THE DOCUMENT TYPE
Examine the page images for logos, headers, and contact information.

## STEP 2: EXTRACT REQUIRED FIELDS
Based on the identified document type, extract the required fields.

{field_details}

## DOCUMENT TEXT (format-preserving, high accuracy):
{docling_text}

## EXTRACTION PROCESS:
For EACH required field:
1. **Visual Location**: Use the page images to identify WHERE the \
field is (which page, which section, near which label)
2. **Return value**: Return the required field values
</steps_to_follow>

<reasoning>
**Document identification**:
- What logo/company name do I see in the images?
- What company name appears in the document text?
- What state indicators are present?

**Field-by-Field Extraction**:
For each field:
1. Visual: I see [field label] on page [N] in section [section]. \
The value appears to be [visual_value].
2. Keywords: Any keywords present near which confirms the value
3. Format match: Does the value match the usual format
4. Final value: [value]

**Confidence Assessment**:
- CERTAIN: Value found confidently along with keywords which confirms it
- LIKELY: Value found but without keywords but matches the format
- UNCERTAIN: Value found but not confident based on keyword or format
- NOT_FOUND: Could not locate value
</reasoning>

<output>
{{
  "is_supported_document": true|false,
  "no_relevant_pages_reason": null,
  "fields": {{
    "field_name": {{
      "value": "extracted_value_or_null",
      "page": page_number,
      "section": "section/label where found",
      "confidence": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND",
      "reasoning": "brief explanation"
    }}
  }}
}}
</output>"""
