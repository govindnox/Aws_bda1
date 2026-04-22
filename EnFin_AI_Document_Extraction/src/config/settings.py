"""
Configuration management for Document Extraction Lambda.

Centralised configuration using nested dataclasses.  Every value is
sourced from an environment variable so nothing is hard-coded except
sensible *defaults*.  The module-level ``config`` singleton is the
single entry-point used by the rest of the application.

Tags:
    Application: Document-Extraction
    Environment: Dev

Author: Reet Roy
Version: 1.1.0

Modification History:
    2026-04-21 - CR-21: Removed duplicate AWS_REGION module constant.
                 CR-22: Guarded ast.literal_eval with try/except.
"""

from typing import Dict
import ast
import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default normalization replacements (used if env var is malformed)
# ---------------------------------------------------------------------------
_DEFAULT_NORMALIZATION_REPLACEMENTS: Dict[str, str] = {
    " ": "",
    "-": "",
    "&amp;": "&",
}


# ---------------------------------------------------------------------------
# Nested config sections
# ---------------------------------------------------------------------------


@dataclass
class AWSConfig:
    """Core AWS settings."""

    region: str = field(default_factory=lambda: os.environ.get("REGION", "us-west-2"))
    s3_bucket: str = field(default_factory=lambda: os.environ.get("S3_BUCKET", ""))


@dataclass
class DynamoDBConfig:
    """DynamoDB table references.

    Attributes:
        config_table:  Stores per-process definitions  (PK: process).
            The process config also holds the ``extraction_table`` name
            for the merged file-state + extraction-results table.
    """

    config_table: str = field(
        default_factory=lambda: os.environ.get("DYNAMODB_CONFIG_TABLE", "")
    )


@dataclass
class SQSConfig:
    """Output SQS queue for the aggregator Lambda.

    Attributes:
        output_queue_url: URL of the FIFO queue that receives
            ``{ "app_no": "..." }`` messages.
        delay_seconds: Message delivery delay (default 300 = 5 min).
    """

    output_queue_url: str = field(
        default_factory=lambda: os.environ.get("OUTPUT_SQS_QUEUE_URL", "")
    )
    delay_seconds: int = field(
        default_factory=lambda: int(os.environ.get("OUTPUT_SQS_DELAY_SECONDS", "300"))
    )


@dataclass
class ModelConfig:
    """Bedrock LLM model parameters."""

    extraction_model: str = field(
        default_factory=lambda: os.environ.get(
            "EXTRACTION_MODEL",
            "us.meta.llama4-maverick-17b-instruct-v1:0",
        )
    )
    max_tokens: int = field(
        default_factory=lambda: int(os.environ.get("MODEL_MAX_TOKENS", "4096"))
    )
    temperature: float = field(
        default_factory=lambda: float(os.environ.get("MODEL_TEMPERATURE", "0.1"))
    )
    max_images_per_call: int = field(
        default_factory=lambda: int(os.environ.get("MAX_IMAGES_PER_LLM_CALL", "3"))
    )


@dataclass
class ImageConfig:
    """PDF-to-image rendering settings."""

    pdf_dpi: int = field(
        default_factory=lambda: int(os.environ.get("PDF_IMAGE_DPI", "150"))
    )
    max_dimension: int = field(
        default_factory=lambda: int(os.environ.get("MAX_IMAGE_DIMENSION", "2000"))
    )


@dataclass
class ConfidenceConfig:
    """Confidence / recommendation thresholds (0-1 scale)."""

    auto_accept_threshold: float = field(
        default_factory=lambda: float(os.environ.get("AUTO_ACCEPT_THRESHOLD", "0.95"))
    )
    flag_threshold: float = field(
        default_factory=lambda: float(os.environ.get("FLAG_THRESHOLD", "0.80"))
    )


@dataclass
class OCRConfig:
    """Docling / OCR engine settings.

    Attributes:
        engine: ``"easyocr"`` or ``"rapidocr"``.
        languages: Comma-separated language codes.
        force_ocr: When True, ignore embedded text layers and OCR
            every page (useful for corrupt PDFs).
    """

    engine: str = field(default_factory=lambda: os.environ.get("OCR_ENGINE", "easyocr"))
    languages: list = field(
        default_factory=lambda: os.environ.get("OCR_LANG", "en").split(",")
    )
    force_ocr: bool = field(
        default_factory=lambda: os.environ.get("FORCE_OCR", "false").lower() == "true"
    )


@dataclass
class AggregationSQSConfig:
    """Aggregation SQS queue settings.

    Controls whether the extraction Lambda enqueues aggregation
    trigger messages after per-file processing completes.

    Attributes:
        enabled: Global toggle for the aggregation layer.
        queue_url: URL of the aggregation input FIFO queue.
    """

    enabled: bool = field(
        default_factory=lambda: os.environ.get("AGGREGATION_ENABLED", "false").lower()
        == "true"
    )
    queue_url: str = field(
        default_factory=lambda: os.environ.get("AGGREGATION_SQS_QUEUE_URL", "")
    )


@dataclass
class SalesforceConfig:
    """Salesforce integration settings (used by aggregator Lambda)."""

    enabled: bool = field(
        default_factory=lambda: os.environ.get("SF_ENABLED", "false").lower() == "true"
    )
    host: str = field(default_factory=lambda: os.environ.get("SF_HOST", ""))
    auth_path: str = field(
        default_factory=lambda: os.environ.get("SF_AUTH_PATH", "/services/oauth2/token")
    )
    token_table: str = field(
        default_factory=lambda: os.environ.get("SF_TOKEN_TABLE", "")
    )
    diff_time: int = field(
        default_factory=lambda: int(os.environ.get("SF_TOKEN_DIFF_TIME", "300"))
    )
    username: str = field(default_factory=lambda: os.environ.get("SF_USERNAME", ""))
    secret_name: str = field(
        default_factory=lambda: os.environ.get("SF_SECRET_NAME", "")
    )
    instance_url: str = field(
        default_factory=lambda: os.environ.get("SF_INSTANCE_URL", "")
    )
    apex_path: str = field(default_factory=lambda: os.environ.get("SF_APEX_PATH", ""))


# ---------------------------------------------------------------------------
# Helper for safe env-var parsing
# ---------------------------------------------------------------------------


def _parse_normalization_replacements() -> Dict[str, str]:
    """Safely parse the NORMALIZATION_RULE_REPLACEMENTS env variable.

    Falls back to sensible defaults when the env var is absent or
    contains a malformed Python literal — prevents Lambda cold-start
    crashes from a misconfigured environment variable (CR-22).

    Returns:
        Dict mapping characters to their normalized replacements.
    """
    raw = os.environ.get("NORMALIZATION_RULE_REPLACEMENTS", "")
    if raw:
        try:
            parsed = ast.literal_eval(raw)
            if isinstance(parsed, dict):
                return parsed
            logger.warning(
                "NORMALIZATION_RULE_REPLACEMENTS is not a dict; " "using defaults"
            )
        except (ValueError, SyntaxError):
            logger.warning(
                "Failed to parse NORMALIZATION_RULE_REPLACEMENTS; " "using defaults"
            )
    return dict(_DEFAULT_NORMALIZATION_REPLACEMENTS)


# ---------------------------------------------------------------------------
# Root configuration — aggregates all sections
# ---------------------------------------------------------------------------

@dataclass
class BDAConfig:
    """Bedrock Data Automation settings.

    The profile ARN is account-wide, so it lives in env vars. The
    blueprint ARN is per-process and lives on each ProcessConfig
    item in the DynamoDB config table.

    Attributes:
        profile_arn: Data automation profile ARN. For us-west-2 this
            is typically of the form
            ``arn:aws:bedrock:us-west-2:{account}:data-automation-profile/us.data-automation-v1``
    """

    profile_arn: str = field(
        default_factory=lambda: os.environ.get("BDA_PROFILE_ARN", "")
    )

@dataclass
class Config:
    """Main configuration aggregating all sections.

    Every value is sourced from environment variables with sensible
    defaults.  Access the module-level ``config`` singleton.
    """

    aws: AWSConfig = field(default_factory=AWSConfig)
    dynamodb: DynamoDBConfig = field(default_factory=DynamoDBConfig)
    sqs: SQSConfig = field(default_factory=SQSConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    image: ImageConfig = field(default_factory=ImageConfig)
    confidence: ConfidenceConfig = field(default_factory=ConfidenceConfig)
    ocr: OCRConfig = field(default_factory=OCRConfig)
    salesforce: SalesforceConfig = field(default_factory=SalesforceConfig)
    aggregation: AggregationSQSConfig = field(
        default_factory=AggregationSQSConfig
    )
    bda: BDAConfig = field(default_factory=BDAConfig)   # <-- bda config

    # General settings
    environment: str = field(
        default_factory=lambda: os.environ.get("ENVIRONMENT", "Dev")
    )
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    timezone: str = field(
        default_factory=lambda: os.environ.get("TIMEZONE", "US/Pacific")
    )
    timestamp_format: str = field(
        default_factory=lambda: os.environ.get("TIMESTAMP_FORMAT", "%m-%d-%Y")
    )

    # Bedrock Prompt Management IDs (optional — fallback is DynamoDB prompt)
    prompt_id_extraction: str = field(
        default_factory=lambda: os.environ.get("PROMPT_ID_EXTRACTION", "")
    )

    # Config cache TTL in seconds (how long to cache DynamoDB process config)
    config_cache_ttl: int = field(
        default_factory=lambda: int(os.environ.get("CONFIG_CACHE_TTL_SECONDS", "300"))
    )

    normalization_rule_replacements: Dict[str, str] = field(
        default_factory=lambda: _parse_normalization_replacements()
    )


# =========================================================================
# Global Configuration Instance
# =========================================================================
config = Config()
