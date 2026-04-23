"""
Data models for the document extraction pipeline.

Defines typed structures for SQS messages, process configuration
loaded from DynamoDB, extraction results, and the DynamoDB storage
record shape.

Author: Reet Roy
Version: 1.1.0

Modification History:
    2026-04-21 - CR-12: Added canonical FileState enum.
                 CR-07: Fixed confidence_configuration merge logic.
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default confidence label → numeric score mapping.
# Used as fallback when DynamoDB process config is missing or empty.
_DEFAULT_CONFIDENCE_MAP: Dict[str, float] = {
    "CERTAIN": 0.95,
    "LIKELY": 0.85,
    "UNCERTAIN": 0.70,
    "NOT_FOUND": 0.0,
}


# =========================================================================
# File processing states (canonical definition — import from here)
# =========================================================================


class FileState(Enum):
    """File processing states.

    This is the single authoritative definition.  Both
    ``dynamodb_service`` and ``file_state_service`` import from here.
    """

    TO_BE_PROCESSED = "To be Processed"
    IN_PROCESS = "In Process"
    PROCESSED = "Processed"
    FAILED = "Failed"


# =========================================================================
# SQS Input Message
# =========================================================================


@dataclass
class SQSInputMessage:
    """Parsed SQS input message.

    Expected payload::

        {
            "path": "TPO/{app_no}/rest_of_path",
            "process": "m0_utility_bill"
        }

    ``app_no`` is extracted from the **second** segment of ``path``.
    """

    path: str
    process: str
    app_no: str
    message_id: Optional[str] = None
    receipt_handle: Optional[str] = None

    @classmethod
    def from_sqs_record(cls, record: Dict[str, Any]) -> "SQSInputMessage":
        """Parse a raw SQS record into a typed message.

        Args:
            record: Single SQS record from the Lambda event.

        Returns:
            Populated ``SQSInputMessage``.

        Raises:
            ValueError: When required fields are missing or ``path``
                does not contain enough segments to extract ``app_no``.
        """
        body = json.loads(record.get("body", "{}"))
        path = body.get("path", "")
        process = body.get("process", "")

        if not path:
            raise ValueError("SQS message missing required 'path' field")
        if not process:
            raise ValueError("SQS message missing required 'process' field")

        # Extract app_no from path: TPO/{app_no}/rest_of_path
        app_no = cls._extract_app_no(path)

        return cls(
            path=path,
            process=process,
            app_no=app_no,
            message_id=record.get("messageId"),
            receipt_handle=record.get("receiptHandle"),
        )

    @staticmethod
    def _extract_app_no(path: str) -> str:
        """Extract application number from the file path.

        The path format is ``TPO/{app_no}/rest_of_path``.
        ``app_no`` is the **second** segment (index 1).

        Args:
            path: S3 object key.

        Returns:
            Extracted application number.

        Raises:
            ValueError: When the path has fewer than 2 segments.
        """
        segments = [s for s in path.split("/") if s]
        if len(segments) < 2:
            raise ValueError(
                f"Cannot extract app_no from path '{path}'. "
                "Expected format: TPO/{{app_no}}/rest_of_path"
            )
        return segments[1]


# =========================================================================
# SQS Output Message
# =========================================================================


@dataclass
class SQSOutputMessage:
    """Parsed SQS output message for Salesforce push.

    Expected payload::

        {
            "path": "TPO/{app_no}/utility_bills/bill.pdf",
            "process": "m0_utility_bill"
        }

    This message is sent to the Salesforce output queue after extraction
    is complete. The SF push Lambda reads this message, queries DynamoDB
    for the extraction result (using global extraction table from env var),
    and pushes to Salesforce.
    """

    path: str
    process: str
    message_id: Optional[str] = None

    @classmethod
    def from_sqs_record(cls, record: Dict[str, Any]) -> "SQSOutputMessage":
        """Parse a raw SQS record into a typed message.

        Args:
            record: Single SQS record from the Lambda event.

        Returns:
            Populated ``SQSOutputMessage``.

        Raises:
            ValueError: When required fields are missing.
        """
        body = json.loads(record.get("body", "{}"))
        path = body.get("path", "")
        process = body.get("process", "")

        if not path:
            raise ValueError("SQS message missing required 'path' field")
        if not process:
            raise ValueError("SQS message missing required 'process' field")

        return cls(
            path=path,
            process=process,
            message_id=record.get("messageId"),
        )


# =========================================================================
# Process Configuration (loaded from DynamoDB config table)
# =========================================================================


@dataclass
class EntityConfig:
    """Configuration for a single entity to extract.

    Loaded from the DynamoDB config table's ``entities`` map.

    Attributes:
        name: Entity field name (e.g. ``"meter_id"``).
        identification: Human-readable description of the entity.
        expected_labels: Label keywords to search near.
        location_hints: Where in the document to find the entity.
        regex: Validation regex pattern (per-field).
        keywords: Multi-language keywords for the entity.
    """

    name: str
    identification: str = ""
    expected_labels: List[str] = field(default_factory=list)
    location_hints: List[str] = field(default_factory=list)
    regex: str = ""
    keywords: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> "EntityConfig":
        """Build from a DynamoDB entity map entry.

        Args:
            name: The entity name (dict key).
            data: The entity configuration dictionary.

        Returns:
            An instance of EntityConfig.
        """
        return cls(
            name=name,
            identification=data.get("identification", ""),
            expected_labels=data.get("expected_labels", []),
            location_hints=data.get("location_hints", []),
            regex=data.get("regex", ""),
            keywords=data.get("keywords", []),
        )


@dataclass
class ConditionalResponse:
    """Rule for adding extra response fields conditionally.

    Example: if ``utility_name == "PG&E"`` and ``state == "CA"``
    then add ``{ "program": "ELRP" }`` to the output.
    """

    conditions: Dict[str, str] = field(default_factory=dict)
    additional_fields: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConditionalResponse":
        """Build from a DynamoDB conditional_responses list entry.

        Args:
            data: A dictionary containing conditions and additional_fields.

        Returns:
            An instance of ConditionalResponse.
        """
        return cls(
            conditions=data.get("conditions", {}),
            additional_fields=data.get("additional_fields", {}),
        )


@dataclass
class ProgramConfig:
    """State/utility → program mapping with mandatory field validation.

    Mirrors ``M0_State_Program_Config__mdt`` from Salesforce.

    Attributes:
        key: Config key (e.g. ``"CA_ELRP"``).
        states: List of US state codes this config applies to.
        utilities: List of utility provider names (or ``["ANY"]``).
        program: Program identifier (e.g. ``"ELRP"``, ``"MISO"``).
        mandatory_fields: Entity names required for this program.
    """

    key: str
    states: List[str] = field(default_factory=list)
    utilities: List[str] = field(default_factory=list)
    program: str = ""
    mandatory_fields: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, key: str, data: Dict[str, Any]) -> "ProgramConfig":
        """Build from a DynamoDB program config entry.

        Args:
            key: Config key (e.g. ``"CA_ELRP"``).
            data: Configuration dictionary.

        Returns:
            An instance of ProgramConfig.
        """
        return cls(
            key=key,
            states=data.get("states", []),
            utilities=data.get("utilities", []),
            program=data.get("program", ""),
            mandatory_fields=data.get("mandatory_fields", []),
        )


@dataclass
class AggregationConfig:
    """Configuration for the aggregation layer.

    Mirrors ``Confidence_Value_Settings__mdt`` aggregation fields.

    Attributes:
        enabled: Whether aggregation is enabled for this process.
        conflict_threshold: Confidence score difference below which
            two competing values are flagged as a conflict (default 0.05).
        submission_window_minutes: Time window in minutes for
            determining which files are part of the latest submission.
        high_confidence_threshold: Above this → auto-accept.
        medium_confidence_threshold: Above this → review recommended.
        field_mappings: Entity name → Salesforce field API name map.
        program_lookup_enabled: Whether to resolve state→program.
        program_configs: Named program configuration rules.
    """
    object_name: str = ""
    enabled: bool = False
    conflict_threshold: float = 0.05
    submission_window_minutes: int = 30
    high_confidence_threshold: float = 0.90
    medium_confidence_threshold: float = 0.80
    field_mappings: Dict[str, str] = field(default_factory=dict)
    program_lookup_enabled: bool = False
    program_configs: Dict[str, ProgramConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AggregationConfig":
        """Build from a DynamoDB aggregation_config map.

        Args:
            data: Aggregation configuration dictionary.

        Returns:
            An instance of AggregationConfig.
        """
        if not data:
            return cls()

        program_configs_raw = data.get("program_configs", {})
        program_configs = {
            key: ProgramConfig.from_dict(key, cfg)
            for key, cfg in program_configs_raw.items()
        }

        return cls(
            object_name=data.get("object_name", ""),
            enabled=data.get("enabled", False),
            conflict_threshold=float(data.get("conflict_threshold", 0.05)),
            submission_window_minutes=int(data.get("submission_window_minutes", 30)),
            high_confidence_threshold=float(
                data.get("high_confidence_threshold", 0.90)
            ),
            medium_confidence_threshold=float(
                data.get("medium_confidence_threshold", 0.80)
            ),
            field_mappings=data.get("field_mappings", {}),
            program_lookup_enabled=data.get("program_lookup_enabled", False),
            program_configs=program_configs,
        )


@dataclass
class SQSAggregationMessage:
    """Parsed SQS message for aggregation trigger.

    Expected payload::

        {
            "app_no": "APP-12345",
            "process": "m0_utility_bill"
        }
    """

    app_no: str
    process: str
    message_id: Optional[str] = None

    @classmethod
    def from_sqs_record(cls, record: Dict[str, Any]) -> "SQSAggregationMessage":
        """Parse a raw SQS record into a typed message.

        Args:
            record: Single SQS record from the Lambda event.

        Returns:
            Populated ``SQSAggregationMessage``.

        Raises:
            ValueError: When required fields are missing.
        """
        body = json.loads(record.get("body", "{}"))
        app_no = body.get("app_no", "")
        process = body.get("process", "")

        if not app_no:
            raise ValueError("SQS aggregation message missing required 'app_no' field")
        if not process:
            raise ValueError("SQS aggregation message missing required 'process' field")

        return cls(
            app_no=app_no,
            process=process,
            message_id=record.get("messageId"),
        )


@dataclass
class ProcessConfig:
    """Complete per-process configuration loaded from DynamoDB.

    Partition key in config table is the ``process`` name.

    Attributes:
        process: Process identifier (e.g. ``"m0_utility_bill"``).
        description: Human-readable process description.
        extraction_prompt: Full prompt template with merge-field
            placeholders ``{docling_text}`` and ``{field_details}``.
            Used as fallback when Bedrock Prompt Management is unavailable.
        prompt_arn: AWS Bedrock Prompt Management ARN (if using Bedrock prompts).
        prompt_version: Prompt version for tracking (e.g., "1", "2").
        ocr_engine: str = "docling"  # "docling" | "textract" | "bda"
        entities: Ordered dict of entities to extract.
        conditional_responses: Rules for adding extra response keys.
        aggregation_config: Aggregation layer configuration.
    """

    process: str
    description: str = ""
    extraction_prompt: str = ""  # Fallback prompt for disaster recovery
    prompt_arn: str = ""  # Bedrock Prompt Management ARN
    prompt_version: str = ""  # Bedrock prompt version for tracking
    ocr_engine: str = "docling"  # "docling" | "textract" | "bda"
    bda_blueprint_arn: str = ""  # Per-process BDA blueprint ARN — required when ocr_engine == "bda"
    textract_min_confidence: float = 50.0  # filter LINE blocks below this
    entities: Dict[str, EntityConfig] = field(default_factory=dict)
    conditional_responses: List[ConditionalResponse] = field(default_factory=list)
    confidence_configuration: Dict[str, float] = field(
        default_factory=lambda: {
            "CERTAIN": 0.95,
            "LIKELY": 0.85,
            "UNCERTAIN": 0.70,
            "NOT_FOUND": 0.0,
        }
    )
    aggregation_config: AggregationConfig = field(default_factory=AggregationConfig)
    # DEPRECATED: extraction_table is now global (DYNAMODB_EXTRACTION_TABLE env var)
    extraction_table: str = ""  # Legacy field, kept for backward compatibility

    @classmethod
    def from_dynamo_item(cls, item: Dict[str, Any]) -> "ProcessConfig":
        """Build from a raw DynamoDB item.

        Args:
            item: DynamoDB item dict (already de-serialised by
                ``boto3.resource('dynamodb')``).

        Returns:
            Typed ``ProcessConfig``.
        """
        # Parse entities
        entities_raw = item.get("entities", {})
        entities = {
            name: EntityConfig.from_dict(name, data)
            for name, data in entities_raw.items()
        }

        # Parse conditional responses
        cond_raw = item.get("conditional_responses", [])
        conditional_responses = [ConditionalResponse.from_dict(c) for c in cond_raw]

        # Parse aggregation config
        agg_config = AggregationConfig.from_dict(item.get("aggregation_config", {}))

        # CR-07: Merge loaded config with defaults so an empty DynamoDB
        # map does not silently discard threshold values.
        raw_confidence = item.get("confidence_configuration") or {}
        confidence_configuration = {
            **_DEFAULT_CONFIDENCE_MAP,
            **raw_confidence,
        }

        return cls(
            process=item.get("process", ""),
            description=item.get("description", ""),
            extraction_prompt=item.get("extraction_prompt", ""),
            prompt_arn=item.get("prompt_arn", ""),
            prompt_version=item.get("prompt_version", ""),
            ocr_engine=item.get("ocr_engine", "docling"),
            bda_blueprint_arn=item.get("bda_blueprint_arn", ""), #bda arn
            textract_min_confidence=float(item.get("textract_min_confidence", 80.0)),
            entities=entities,
            conditional_responses=conditional_responses,
            confidence_configuration=confidence_configuration,
            aggregation_config=agg_config,
            extraction_table=item.get("extraction_table", ""),
        )


# =========================================================================
# Extraction Result (per-file output from the pipeline)
# =========================================================================


@dataclass
class ExtractedField:
    """Single extracted field with confidence metadata."""

    name: str
    value: Optional[str] = None
    confidence: str = "NOT_FOUND"
    confidence_score: float = 0.0
    page: Optional[int] = None
    section: Optional[str] = None
    reasoning: str = ""
    docling_match: bool = False
    format_match: bool = False
    validation_passed: bool = False
    validation_notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to dict for JSON / DynamoDB storage.

        Returns:
            A dictionary representation of the extracted field.
        """
        return {
            "name": self.name,
            "value": self.value,
            "confidence": self.confidence,
            "confidence_score": self.confidence_score,
            "page": self.page,
            "section": self.section,
            "reasoning": self.reasoning,
            "docling_match": self.docling_match,
            "format_match": self.format_match,
            "validation_passed": self.validation_passed,
            "validation_notes": self.validation_notes,
        }


@dataclass
class ExtractionResult:
    """Complete extraction result for a single document."""

    # Classification
    is_supported_document: bool = True
    no_relevant_pages_reason: Optional[str] = None

    # Extracted fields
    fields: Dict[str, ExtractedField] = field(default_factory=dict)

    # Confidence / recommendation
    overall_confidence: float = 0.0
    recommendation: str = "manual_required"

    # Conditional response keys (e.g. program: ELRP)
    additional_response: Dict[str, str] = field(default_factory=dict)

    # Processing metadata
    llm_calls: int = 0
    processing_time_ms: int = 0
    file_type: str = "pdf"
    docling_processed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full result for storage.

        Returns:
            A dictionary representation of the extraction result.
            Fields are returned as a list of dicts (not a dict of dicts).
        """
        return {
            "is_supported_document": self.is_supported_document,
            "no_relevant_pages_reason": self.no_relevant_pages_reason,
            "fields": {name: fld.to_dict() for name, fld in self.fields.items()},
            "overall_confidence": self.overall_confidence,
            "recommendation": self.recommendation,
            "additional_response": self.additional_response,
            "llm_calls": self.llm_calls,
            "processing_time_ms": self.processing_time_ms,
            "file_type": self.file_type,
            "docling_processed": self.docling_processed,
        }


# =========================================================================
# DynamoDB Extraction Record (stored in results table)
# =========================================================================


@dataclass
class ExtractionRecord:
    """Single entry in the ``extractions`` list stored per ``app_no``.

    Attributes:
        path: S3 object key of the processed document.
        extracted_json: JSON string of the extraction result with
            confidence metadata.
        timestamp: Processing timestamp in PT, format ``MM-DD-YYYY``.
    """

    path: str
    extracted_json: str
    timestamp: str

    def to_dict(self) -> Dict[str, str]:
        """Convert to DynamoDB-compatible dict.

        Returns:
            A dictionary containing the extraction record data.
        """
        return {
            "path": self.path,
            "extracted_json_string_with_confidence": self.extracted_json,
            "timestamp": self.timestamp,
        }
