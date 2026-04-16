"""
Rule-based confidence scoring for extracted utility bill fields.

Replaces LLM-based confidence scoring with deterministic rules:
1. Check if extracted value appears in Docling text → if absent, confidence = 0.75
2. Check if extracted value matches format regex → if mismatch, confidence = 0.75
"""

import re
import logging
from typing import Dict, Any

from config.prompts import FIELD_CONFIGURATIONS
from config.settings import settings

logger = logging.getLogger(__name__)


# Confidence level mappings from LLM output
CONFIDENCE_LEVEL_MAP = {
    "CERTAIN": 0.95,
    "LIKELY": 0.85,
    "UNCERTAIN": 0.70,
    "NOT_FOUND": 0.0
}


class RuleBasedScorer:
    """Rule-based confidence scorer for extracted utility bill fields."""

    def __init__(self):
        self.auto_accept_threshold = settings.AUTO_ACCEPT_THRESHOLD
        self.flag_threshold = settings.FLAG_THRESHOLD

    def score_fields(
        self,
        extracted_fields: Dict[str, Any],
        docling_text: str,
        utility: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Score each extracted field using rule-based validation.

        Rules:
        - If extracted value IS in docling text → confidence unchanged
        - If extracted value NOT in docling text → confidence = 0.75
        - If format regex matches → confidence unchanged
        - If format regex mismatches → confidence = 0.75

        Args:
            extracted_fields: Fields from LLM extraction output
            docling_text: Full markdown text from Docling
            utility: Detected utility provider name

        Returns:
            Scored fields dict with confidence_score, docling_match, format_match
        """
        scored_fields = {}
        utility_config = FIELD_CONFIGURATIONS.get(utility, {})
        field_details = utility_config.get("field_details", {})

        for field_name, field_data in extracted_fields.items():
            value = field_data.get("value")
            llm_confidence_str = field_data.get("confidence", "UNCERTAIN")

            # Convert LLM confidence level to numeric
            initial_confidence = CONFIDENCE_LEVEL_MAP.get(
                llm_confidence_str, 0.70
            )

            # Skip scoring if no value extracted
            if not value or llm_confidence_str == "NOT_FOUND":
                scored_fields[field_name] = {
                    **field_data,
                    "extracted_value": value,
                    "validated_value": value,
                    "confidence_score": 0.0,
                    "docling_match": False,
                    "format_match": False,
                    "validation_passed": False,
                    "validation_notes": "Value not found by LLM",
                    "value_source": "extraction"
                }
                continue

            confidence = initial_confidence
            docling_match = True
            format_match = True
            validation_notes = []

            # Rule 1: Check if value appears in Docling text
            # Strip spaces/dashes for comparison (some values like SDG&E accounts have spaces)
            value_normalized = value.replace(" ", "").replace("-", "")
            docling_normalized = docling_text.replace(" ", "").replace("-", "")

            if value_normalized not in docling_normalized:
                docling_match = False
                confidence = 0.75
                validation_notes.append(
                    f"Value '{value}' not found in Docling text"
                )
            else:
                validation_notes.append(
                    f"Value '{value}' confirmed in Docling text"
                )

            # Rule 2: Check format regex pattern
            field_config = field_details.get(field_name, {})
            format_pattern = field_config.get("format_pattern")

            if format_pattern:
                # For SDG&E accounts, strip spaces before regex check
                check_value = value_normalized if "SDG&E" in utility else value
                if not re.match(format_pattern, check_value):
                    format_match = False
                    confidence = 0.75
                    validation_notes.append(
                        f"Value '{value}' does not match expected format: "
                        f"{field_config.get('format_description', format_pattern)}"
                    )
                else:
                    validation_notes.append(
                        f"Value '{value}' matches expected format"
                    )

            scored_fields[field_name] = {
                **field_data,
                "extracted_value": value,
                "validated_value": value,
                "confidence_score": confidence,
                "docling_match": docling_match,
                "format_match": format_match,
                "validation_passed": docling_match and format_match,
                "validation_notes": "; ".join(validation_notes),
                "value_source": "docling" if docling_match else "extraction"
            }

            logger.info(
                f"Field '{field_name}': value='{value}', "
                f"docling_match={docling_match}, format_match={format_match}, "
                f"confidence={confidence:.2f}"
            )

        return scored_fields

    def calculate_overall_confidence(
        self,
        scored_fields: Dict[str, Dict[str, Any]]
    ) -> float:
        """
        Calculate overall confidence as average of all field confidences.

        Args:
            scored_fields: Fields with confidence scores

        Returns:
            Overall confidence score (0.0 to 1.0)
        """
        confidences = [
            field_data.get("confidence_score", 0.0)
            for field_data in scored_fields.values()
            if field_data.get("extracted_value")  # Only count fields with values
        ]

        if not confidences:
            return 0.0

        return sum(confidences) / len(confidences)

    def determine_recommendation(self, overall_confidence: float) -> str:
        """
        Determine recommendation based on overall confidence.

        Args:
            overall_confidence: Overall confidence score

        Returns:
            Recommendation string: auto_accept | flag_for_review | manual_required
        """
        if overall_confidence >= self.auto_accept_threshold:
            return "auto_accept"
        elif overall_confidence >= self.flag_threshold:
            return "flag_for_review"
        else:
            return "manual_required"
