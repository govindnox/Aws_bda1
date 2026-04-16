"""
Rule-based confidence scorer using per-field regex from DynamoDB config.

Scoring rules:
    1. If extracted value IS in Docling text → confidence unchanged.
    2. If extracted value NOT in Docling text → confidence = 0.75.
    3. If format regex matches → confidence unchanged.
    4. If format regex mismatches → confidence = 0.75.

Also handles conditional response keys (e.g. utility + state → program).

Author: Reet Roy
Version: 1.0.0
"""

import logging
import re
from typing import Any, Dict, List

from models.data_models import ProcessConfig

logger = logging.getLogger(__name__)


class ConfidenceScorer:
    """Configurable rule-based scorer driven by ``ProcessConfig``."""

    def __init__(self):
        """Initialise the ConfidenceScorer, loading thresholds from config."""
        from config import config

        self._auto_accept = config.confidence.auto_accept_threshold
        self._flag = config.confidence.flag_threshold
        self._normalization_replacements = config.normalization_rule_replacements

    # ------------------------------------------------------------------
    # Field scoring
    # ------------------------------------------------------------------

    def score_fields(
        self,
        raw_fields: Dict[str, Any],
        docling_text: str,
        process_config: ProcessConfig,
    ) -> Dict[str, Dict[str, Any]]:
        """Score each extracted field using configurable rules.

        Args:
            raw_fields: LLM extraction output ``fields`` dict.
            docling_text: Full Docling markdown for cross-validation.
            process_config: Process config with per-field regex.

        Returns:
            Dict of scored fields, each enriched with
            ``confidence_score``, ``docling_match``, ``format_match``,
            ``validation_passed``, ``validation_notes``.
        """
        scored: Dict[str, Dict[str, Any]] = {}
        # LLM confidence label → numeric score mapping
        _CONFIDENCE_MAP = process_config.confidence_configuration or {
            "CERTAIN": 0.95,
            "LIKELY": 0.85,
            "UNCERTAIN": 0.70,
            "NOT_FOUND": 0.0,
        }
        normalized_text = docling_text
        for change_character, replacement in self._normalization_replacements.items():
            normalized_text = normalized_text.replace(change_character, replacement)
        for field_name, field_data in raw_fields.items():
            if not isinstance(field_data.get("value"), str):
                field_data["value"] = (str(field_data.get("value"))
                                       if field_data.get("value") is not None else "")
            value = field_data.get("value", "")
            llm_confidence = field_data.get("confidence", "UNCERTAIN")

            initial = float(_CONFIDENCE_MAP.get(llm_confidence, 0.70))

            # No value extracted
            if not value or llm_confidence == "NOT_FOUND":
                scored[field_name] = {
                    **field_data,
                    "confidence_score": 0.0,
                    "docling_match": False,
                    "format_match": False,
                    "validation_passed": False,
                    "validation_notes": "Value not found by LLM",
                }
                continue

            confidence = initial
            docling_match = True
            format_match = True
            notes: List[str] = []

            # Rule 1 — Docling text match
            normalized_value = value
            for (
                change_character,
                replacement,
            ) in self._normalization_replacements.items():
                normalized_value = normalized_value.replace(
                    change_character, replacement
                )

            if normalized_value not in normalized_text:
                docling_match = False
                confidence = confidence - 0.20
                notes.append(f"Value '{value}' not found in document text")
            else:
                notes.append(f"Value '{value}' confirmed in document text")

            # Rule 2 — Per-field regex validation from config
            entity_cfg = process_config.entities.get(field_name)
            if entity_cfg and entity_cfg.regex:
                check_val = normalized_value
                if not re.match(entity_cfg.regex, check_val):
                    format_match = False
                    confidence = confidence - 0.10
                    notes.append(
                        f"Value '{value}' does not match expected "
                        f"format: {entity_cfg.regex}"
                    )
                else:
                    notes.append(f"Value '{value}' matches expected format")

            scored[field_name] = {
                **field_data,
                "confidence_score": confidence,
                "docling_match": docling_match,
                "format_match": format_match,
                "validation_passed": docling_match and format_match,
                "validation_notes": "; ".join(notes),
            }

            logger.info(
                "Field '%s': value='%s', docling=%s, format=%s, " "confidence=%.2f",
                field_name,
                value,
                docling_match,
                format_match,
                confidence,
            )

        return scored

    # ------------------------------------------------------------------
    # Overall confidence / recommendation
    # ------------------------------------------------------------------

    def calculate_overall_confidence(
        self, scored_fields: Dict[str, Dict[str, Any]]
    ) -> float:
        """Weighted average of field confidences (0.0–1.0).

        Args:
            scored_fields: A dictionary of fields with their individual scores.

        Returns:
            The calculated overall confidence score as a float.
        """
        scores = [
            fd.get("confidence_score", 0.0)
            for fd in scored_fields.values()
            if fd.get("value")
        ]
        return sum(scores) / len(scores) if scores else 0.0

    def determine_recommendation(self, overall: float) -> str:
        """Map overall confidence to a recommendation string.

        Args:
            overall: The calculated overall confidence score.

        Returns:
            A recommendation string ("auto_accept", "flag_for_review", etc.).
        """
        if overall >= self._auto_accept:
            return "auto_accept"
        if overall >= self._flag:
            return "flag_for_review"
        return "manual_required"

    # ------------------------------------------------------------------
    # Conditional response keys
    # ------------------------------------------------------------------

    @staticmethod
    def apply_conditional_responses(
        raw_fields: Dict[str, Any],
        process_config: ProcessConfig,
    ) -> Dict[str, str]:
        """Evaluate conditional response rules and return extra keys.

        For example, if ``utility_name == "PG&E"`` and
        ``state == "CA"`` then return ``{ "program": "ELRP" }``.

        Args:
            raw_fields: Extracted field values.
            process_config: Process config with conditional rules.

        Returns:
            Dict of additional response keys to include.
        """
        additional: Dict[str, str] = {}

        # Build a lookup of current values for condition matching
        context: Dict[str, str] = {}
        for name, fdata in raw_fields.items():
            if isinstance(fdata, dict) and fdata.get("value"):
                context[name] = fdata["value"]

        for rule in process_config.conditional_responses:
            if _conditions_match(rule.conditions, context):
                additional.update(rule.additional_fields)
                logger.info(
                    "Conditional response matched: conditions=%s → %s",
                    rule.conditions,
                    rule.additional_fields,
                )

        return additional


# =========================================================================
# Internal helpers
# =========================================================================


def _conditions_match(conditions: Dict[str, str], context: Dict[str, str]) -> bool:
    """Check if all conditions are satisfied by the context.

    Args:
        conditions: A dictionary of required conditions and values.
        context: A dictionary representing the current context to check against.

    Returns:
        True if all conditions match the context, False otherwise.
    """
    for key, expected in conditions.items():
        actual = context.get(key, "")
        if actual.lower() != expected.lower():
            return False
    return True
