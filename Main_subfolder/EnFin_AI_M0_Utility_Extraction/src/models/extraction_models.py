"""
Data models for extraction results.

Updated for the Docling-based pipeline with rule-based confidence scoring.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum


class ExtractionStatus(Enum):
    """Status of extraction"""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    PENDING_REVIEW = "pending_review"


class ConfidenceLevel(Enum):
    """Confidence level categories"""
    HIGH = "high"      # >= 0.95
    MEDIUM = "medium"  # 0.80 - 0.94
    LOW = "low"        # < 0.80


class RecommendationType(Enum):
    """Recommendation for handling extraction result"""
    AUTO_ACCEPT = "auto_accept"
    FLAG_FOR_REVIEW = "flag_for_review"
    MANUAL_REQUIRED = "manual_required"


@dataclass
class FieldScore:
    """Score breakdown for a single field (rule-based scoring)"""
    docling_match: bool = False
    format_match: bool = False
    final_confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "docling_match": self.docling_match,
            "format_match": self.format_match,
            "final_confidence": self.final_confidence
        }


@dataclass
class ExtractedField:
    """Represents a single extracted field with validation"""
    field_name: str
    value: Optional[str]
    confidence_score: float
    validation_passed: bool
    validation_notes: str
    score_breakdown: FieldScore

    # Extraction metadata
    page_found: Optional[int] = None
    section_found: Optional[str] = None

    def get_confidence_level(self) -> ConfidenceLevel:
        if self.confidence_score >= 0.95:
            return ConfidenceLevel.HIGH
        elif self.confidence_score >= 0.80:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "value": self.value,
            "confidence_score": self.confidence_score,
            "validation_passed": self.validation_passed,
            "validation_notes": self.validation_notes,
            "score_breakdown": self.score_breakdown.to_dict(),
            "page_found": self.page_found,
            "section_found": self.section_found,
            "confidence_level": self.get_confidence_level().value
        }


@dataclass
class UtilityClassification:
    """Classification of the utility provider"""
    provider: str
    state: str
    program: str
    confidence: str


@dataclass
class ProcessingMetadata:
    """Metadata about the extraction process"""
    extraction_id: str
    timestamp: str
    source_file: str
    total_pages: int
    pages_processed: List[int]
    llm_calls: int
    docling_processed: bool
    processing_time_ms: int
    models_used: Dict[str, str] = field(default_factory=dict)


@dataclass
class ExtractionOutput:
    """Complete extraction output for Salesforce integration"""
    classification: UtilityClassification
    fields: Dict[str, ExtractedField]
    overall_confidence: float
    overall_status: ExtractionStatus
    recommendation: RecommendationType
    requires_review: bool
    review_reasons: List[str]
    metadata: ProcessingMetadata

    def get_salesforce_payload(self) -> Dict[str, Any]:
        """
        Generate payload for Salesforce Apex REST POST.

        Returns field values and metadata for the Apex endpoint.
        """
        fields_data = {}
        for field_name, field_data in self.fields.items():
            if field_data.value:
                fields_data[field_name] = {
                    "value": field_data.value,
                    "confidence": field_data.confidence_score
                }

        return {
            "utilityProvider": self.classification.provider,
            "state": self.classification.state,
            "program": self.classification.program,
            "fields": fields_data,
            "overallConfidence": self.overall_confidence,
            "recommendation": self.recommendation.value,
            "extractionId": self.metadata.extraction_id,
            "requiresReview": self.requires_review,
            "reviewReasons": self.review_reasons
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "classification": asdict(self.classification),
            "fields": {
                name: field.to_dict()
                for name, field in self.fields.items()
            },
            "overall_confidence": self.overall_confidence,
            "overall_status": self.overall_status.value,
            "recommendation": self.recommendation.value,
            "requires_review": self.requires_review,
            "review_reasons": self.review_reasons,
            "metadata": asdict(self.metadata)
        }


@dataclass
class SQSMessage:
    """
    Represents an SQS message triggering extraction.

    Payload structure:
    {
        "filePath": "s3://bucket/path/to/file.pdf",
        "app_no": "APP-123456",
        "process": "m0_utility_bill"
    }
    """
    file_path: str
    app_no: Optional[str] = None
    process: str = "m0_utility_bill"
    timestamp: Optional[str] = None
    source: Optional[str] = None
    message_id: Optional[str] = None
    receipt_handle: Optional[str] = None

    @classmethod
    def from_sqs_record(cls, record: Dict[str, Any]) -> "SQSMessage":
        """Create from SQS record"""
        import json

        body = json.loads(record.get("body", "{}"))

        return cls(
            file_path=body.get("filePath", ""),
            app_no=body.get("app_no"),
            process=body.get("process", "m0_utility_bill"),
            timestamp=body.get("timestamp"),
            source=body.get("source"),
            message_id=record.get("messageId"),
            receipt_handle=record.get("receiptHandle")
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "filePath": self.file_path,
            "app_no": self.app_no,
            "process": self.process,
            "timestamp": self.timestamp,
            "source": self.source
        }
