"""
Bedrock Prompt Management service for retrieving and caching prompts.

Handles:
- Prompt retrieval by ARN from AWS Bedrock Prompt Management
- In-memory caching with TTL (default 300 seconds)
- Placeholder replacement: {{}} syntax for Bedrock compatibility
- Fallback to DynamoDB prompt on Bedrock errors

Author: Reet Roy
Version: 1.0.0
"""

import logging
import re
import time
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Cache: {prompt_arn: (prompt_text, fetch_time)}
_prompt_cache: Dict[str, Tuple[str, float]] = {}

# Lazy-loaded Bedrock client
_bedrock_agent_client = None


def get_prompt_by_arn(
    prompt_arn: str, fallback_prompt: Optional[str] = None, cache_ttl: int = 300
) -> str:
    """
    Retrieve prompt from Bedrock Prompt Management.

    Args:
        prompt_arn: Full ARN with version (e.g., "arn:aws:bedrock:region:account:prompt/name:version")
        fallback_prompt: DynamoDB prompt for disaster recovery
        cache_ttl: Cache time-to-live in seconds (default: 300)

    Returns:
        Prompt template text with {{}} placeholders

    Raises:
        ValueError: If no prompt available (Bedrock failed and no fallback)
    """
    # Check cache
    cached = _prompt_cache.get(prompt_arn)
    if cached:
        prompt_text, fetch_time = cached
        if time.time() - fetch_time < cache_ttl:
            logger.debug("Returning cached prompt for ARN=%s", prompt_arn)
            return prompt_text

    # Fetch from Bedrock
    try:
        prompt_text = _fetch_prompt_from_bedrock(prompt_arn)

        # Cache the result
        _prompt_cache[prompt_arn] = (prompt_text, time.time())
        logger.info("Retrieved and cached prompt from Bedrock: ARN=%s", prompt_arn)

        return prompt_text

    except Exception as e:
        logger.error("Failed to retrieve prompt from Bedrock: %s", e)

        # Fallback to DynamoDB prompt
        if fallback_prompt:
            logger.warning("Using fallback prompt from DynamoDB")
            # Convert {} to {{}} for consistency
            return convert_placeholder_syntax(fallback_prompt)

        raise ValueError(f"No prompt available for ARN {prompt_arn}")


def _fetch_prompt_from_bedrock(prompt_arn: str) -> str:
    """
    Fetch prompt from Bedrock Prompt Management using boto3.

    Args:
        prompt_arn: Full prompt ARN with version

    Returns:
        Prompt text

    Raises:
        Exception: If Bedrock API fails or prompt is empty
    """
    global _bedrock_agent_client

    # Lazy-initialize Bedrock client
    if _bedrock_agent_client is None:
        import boto3
        from config import config

        _bedrock_agent_client = boto3.client(
            "bedrock-agent", region_name=config.aws.region
        )

    # Extract prompt ID and version from ARN
    # ARN format: arn:aws:bedrock:region:account:prompt/prompt-id:version
    # or: arn:aws:bedrock:region:account:prompt/prompt-id
    parts = prompt_arn.split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid prompt ARN format: {prompt_arn}")

    prompt_id_with_version = parts[-1]  # "prompt-id:version" or "prompt-id"

    # Parse prompt ID and version
    if ":" in prompt_id_with_version:
        prompt_id, version = prompt_id_with_version.split(":", 1)
    else:
        # No version specified, use LATEST
        prompt_id = prompt_id_with_version
        version = "$LATEST"

    logger.info("Fetching prompt: ID=%s, Version=%s", prompt_id, version)

    # Call Bedrock API
    response = _bedrock_agent_client.get_prompt(
        promptIdentifier=prompt_id, promptVersion=version
    )

    # Extract prompt text from response
    # Response structure:
    # {
    #   "variants": [
    #     {
    #       "templateConfiguration": {
    #         "text": {
    #           "text": "prompt text here"
    #         }
    #       }
    #     }
    #   ]
    # }
    variants = response.get("variants", [])
    if not variants:
        raise ValueError(f"No variants found in prompt: {prompt_arn}")

    template_config = variants[0].get("templateConfiguration", {})
    text_config = template_config.get("text", {})
    prompt_text = text_config.get("text", "")

    if not prompt_text:
        raise ValueError(f"Empty prompt returned from Bedrock: {prompt_arn}")

    logger.debug("Successfully fetched prompt: %d characters", len(prompt_text))

    return prompt_text


def convert_placeholder_syntax(prompt: str) -> str:
    """
    Convert {placeholder} to {{placeholder}} for Bedrock syntax.

    This is used for fallback prompts from DynamoDB that use {} syntax
    instead of Bedrock's {{}} syntax.

    Args:
        prompt: Prompt with {} placeholders

    Returns:
        Prompt with {{}} placeholders

    Examples:
        >>> convert_placeholder_syntax("Hello {name}")
        'Hello {{name}}'
        >>> convert_placeholder_syntax("{docling_text} and {field_details}")
        '{{docling_text}} and {{field_details}}'
    """
    # Replace {word} with {{word}}
    # Use word boundary to avoid matching JSON braces
    converted = re.sub(r"\{(\w+)\}", r"{{\1}}", prompt)

    return converted


def clear_cache():
    """
    Clear the in-memory prompt cache.

    Useful for testing or forcing a refresh of all prompts.
    """
    global _prompt_cache
    _prompt_cache.clear()
    logger.info("Prompt cache cleared")
