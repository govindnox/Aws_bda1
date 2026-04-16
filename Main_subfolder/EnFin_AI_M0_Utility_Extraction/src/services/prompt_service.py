"""
Bedrock Prompt Management service for retrieving prompts.
"""

import logging
import boto3
from typing import Optional, Dict, Any
from functools import lru_cache

from config.settings import settings

logger = logging.getLogger(__name__)


class PromptService:
    """Service for retrieving prompts from Bedrock Prompt Management"""

    def __init__(self):
        self.client = boto3.client(
            'bedrock-agent',
            region_name=settings.REGION
        )
        self._prompt_cache: Dict[str, str] = {}

    def get_prompt(self, prompt_id: str) -> str:
        """
        Fetch prompt from Bedrock Prompt Management using prompt ID.

        Args:
            prompt_id: ID of the prompt to retrieve

        Returns:
            str: Prompt text or empty string if error occurs
        """
        # Check cache first
        if prompt_id in self._prompt_cache:
            logger.debug(f"Returning cached prompt for {prompt_id}")
            return self._prompt_cache[prompt_id]

        try:
            response = self.client.get_prompt(promptIdentifier=prompt_id)
            variants = response.get("variants")
            if variants and len(variants):
                prompt_text = (
                    variants[0]
                    .get("templateConfiguration")
                    .get("text")
                    .get("text")
                )
                # Cache the prompt
                self._prompt_cache[prompt_id] = prompt_text
                logger.info(f"Successfully retrieved prompt: {prompt_id}")
                return prompt_text

            logger.warning(f"No variants found for prompt: {prompt_id}")
            return ""

        except Exception as e:
            logger.error(f"Error retrieving prompt {prompt_id}: {str(e)}")
            return ""

    def get_page_detection_prompt(self) -> str:
        """Get the page detection prompt"""
        return self.get_prompt(settings.PROMPT_ID_PAGE_DETECTION)

    def get_extraction_prompt(self) -> str:
        """Get the data extraction prompt"""
        return self.get_prompt(settings.PROMPT_ID_EXTRACTION)

    def get_confidence_scoring_prompt(self) -> str:
        """Get the confidence scoring prompt"""
        return self.get_prompt(settings.PROMPT_ID_CONFIDENCE_SCORING)

    def get_focused_retry_prompt(self) -> str:
        """Get the focused retry prompt"""
        return self.get_prompt(settings.PROMPT_ID_FOCUSED_RETRY)

    def clear_cache(self):
        """Clear the prompt cache"""
        self._prompt_cache.clear()
        logger.info("Prompt cache cleared")


# Singleton instance
_prompt_service: Optional[PromptService] = None


def get_prompt_service() -> PromptService:
    """Get or create the prompt service singleton"""
    global _prompt_service
    if _prompt_service is None:
        _prompt_service = PromptService()
    return _prompt_service
