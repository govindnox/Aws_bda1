"""
Configuration settings for the utility bill extraction pipeline.
All settings can be overridden via environment variables.

Supports multiple extraction processes, each with its own prompts and field configurations.
"""

import os
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class ProcessConfig:
    """Configuration for a specific extraction process"""
    name: str
    description: str
    prompt_id_extraction: str
    required_fields: List[str]
    field_patterns: Dict[str, str]  # field_name -> regex pattern for validation
    utility_mappings: Dict[str, Dict[str, Any]]  # utility -> field requirements


class Settings:
    """Configuration settings loaded from environment variables with defaults"""

    # ==========================================================================
    # AWS Settings
    # ==========================================================================
    REGION: str = os.environ.get("REGION", "us-west-2")
    S3_BUCKET: str = os.environ.get("S3_BUCKET", "")

    # ==========================================================================
    # DynamoDB Settings
    # ==========================================================================
    DYNAMODB_TABLE_NAME: str = os.environ.get(
        "DYNAMODB_TABLE_NAME", "utility-extraction-results"
    )

    # ==========================================================================
    # Bedrock Model Settings
    # ==========================================================================
    # Single extraction model (handles utility detection + field extraction)
    EXTRACTION_MODEL: str = os.environ.get(
        "EXTRACTION_MODEL",
        "us.meta.llama4-maverick-17b-instruct-v1:0"
    )

    # Model inference parameters
    MODEL_MAX_TOKENS: int = int(os.environ.get("MODEL_MAX_TOKENS", "4096"))
    MODEL_TEMPERATURE: float = float(os.environ.get("MODEL_TEMPERATURE", "0.1"))

    # ==========================================================================
    # Salesforce Enable Flag
    # ==========================================================================
    SF_ENABLED: bool = os.environ.get("SF_ENABLED", "true").lower() == "true"

    # ==========================================================================
    # Process Configuration
    # ==========================================================================
    # Default process if none specified in payload
    DEFAULT_PROCESS: str = os.environ.get("DEFAULT_PROCESS", "m0_utility_bill")

    # Process-specific prompt IDs (JSON string from env, or defaults)
    # Format: {"process": {"extraction": "id"}}
    PROCESS_PROMPT_IDS: Dict[str, Dict[str, str]] = {}

    # ==========================================================================
    # Bedrock Prompt Management - Prompt ID for extraction
    # ==========================================================================
    PROMPT_ID_EXTRACTION: str = os.environ.get(
        "PROMPT_ID_EXTRACTION", ""
    )

    # ==========================================================================
    # Final Confidence Thresholds
    # ==========================================================================
    # Threshold for auto-accepting to Salesforce
    AUTO_ACCEPT_THRESHOLD: float = float(
        os.environ.get("AUTO_ACCEPT_THRESHOLD", "0.95")
    )

    # Threshold for accepting with verification flag
    FLAG_THRESHOLD: float = float(os.environ.get("FLAG_THRESHOLD", "0.80"))

    # Below FLAG_THRESHOLD → route to manual review

    # ==========================================================================
    # OCR Configuration
    # ==========================================================================
    # OCR engine: "easyocr" or "rapidocr"
    OCR_ENGINE: str = os.environ.get("OCR_ENGINE", "easyocr")

    # OCR languages (comma-separated, e.g. "en" or "en,es")
    OCR_LANG: list = os.environ.get("OCR_LANG", "en,es").split(",")

    # Force OCR on all pages (even those with embedded text)
    FORCE_OCR: bool = os.environ.get("FORCE_OCR", "false").lower() == "true"

    # Max images per LLM Converse call (model-specific limit, e.g. Llama 4 Maverick = 3)
    MAX_IMAGES_PER_LLM_CALL: int = int(os.environ.get("MAX_IMAGES_PER_LLM_CALL", "3"))

    # ==========================================================================
    # Image Processing Settings
    # ==========================================================================
    # DPI for PDF to image conversion (balance quality vs size)
    PDF_IMAGE_DPI: int = int(os.environ.get("PDF_IMAGE_DPI", "150"))

    # Maximum image dimension (resize if larger)
    MAX_IMAGE_DIMENSION: int = int(os.environ.get("MAX_IMAGE_DIMENSION", "2000"))

    # ==========================================================================
    # Salesforce Settings (TokenManager)
    # ==========================================================================
    SF_TOKEN_TABLE_NAME: str = os.environ.get("SF_TOKEN_TABLE_NAME", "")
    SF_HOST: str = os.environ.get("SF_HOST", "")
    SF_AUTH_PATH: str = os.environ.get("SF_AUTH_PATH", "/services/oauth2/token")
    SF_TOKEN_DIFF_TIME: int = int(os.environ.get("SF_TOKEN_DIFF_TIME", "300"))
    SF_USERNAME: str = os.environ.get("SF_USERNAME", "")
    SF_SECRET_NAME: str = os.environ.get("SF_SECRET_NAME", "")
    SF_INSTANCE_URL: str = os.environ.get("SF_INSTANCE_URL", "")
    SF_APEX_PATH: str = os.environ.get("SF_APEX_PATH", "")
    SF_QSS_QUEUE_ID: str = os.environ.get("SF_QSS_QUEUE_ID", "")

    # Salesforce field mappings
    SALESFORCE_FIELD_MAPPINGS: Dict[str, str] = {
        "meter_id": os.environ.get("SF_FIELD_METER_ID", "Meter_ID__c"),
        "account_number": os.environ.get("SF_FIELD_ACCOUNT_NUMBER", "Utility_Account_Number__c"),
        "service_agreement_id": os.environ.get("SF_FIELD_SERVICE_AGREEMENT_ID", "Service_Agreement_ID__c"),
        "electric_choice_id": os.environ.get("SF_FIELD_ELECTRIC_CHOICE_ID", "Electric_Choice_ID__c"),
        "meter_number": os.environ.get("SF_FIELD_METER_NUMBER", "Meter_Number__c"),
        "extraction_confidence": os.environ.get("SF_FIELD_CONFIDENCE", "Extraction_Confidence__c"),
        "needs_verification": os.environ.get("SF_FIELD_NEEDS_VERIFICATION", "Needs_Manual_Verification__c"),
        "extraction_timestamp": os.environ.get("SF_FIELD_TIMESTAMP", "Last_Extraction_Date__c"),
        "extraction_status": os.environ.get("SF_FIELD_STATUS", "Extraction_Status__c")
    }

    # ==========================================================================
    # Logging Settings
    # ==========================================================================
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")

    # Whether to log full prompts and responses (useful for debugging)
    LOG_FULL_PROMPTS: bool = os.environ.get(
        "LOG_FULL_PROMPTS", "false"
    ).lower() == "true"

    # ==========================================================================
    # Utility Validation
    # ==========================================================================

    @classmethod
    def get_process_prompt_ids(cls, process: str) -> Dict[str, str]:
        """
        Get prompt IDs for a specific extraction process.

        Args:
            process: Process name (e.g., 'm0_utility_bill')

        Returns:
            Dictionary with 'extraction' prompt ID
        """
        # First check if process-specific IDs are configured
        if process in cls.PROCESS_PROMPT_IDS:
            return cls.PROCESS_PROMPT_IDS[process]

        # Check environment for process-specific prompt IDs
        env_key = f"PROMPT_IDS_{process.upper()}"
        env_value = os.environ.get(env_key, "")
        if env_value:
            try:
                return json.loads(env_value)
            except json.JSONDecodeError:
                pass

        # Fall back to default prompt IDs
        return {
            "extraction": cls.PROMPT_ID_EXTRACTION
        }

    @classmethod
    def load_process_configs(cls) -> None:
        """Load process configurations from environment"""
        # Try to load PROCESS_PROMPT_IDS from environment as JSON
        config_json = os.environ.get("PROCESS_PROMPT_IDS", "")
        if config_json:
            try:
                cls.PROCESS_PROMPT_IDS = json.loads(config_json)
            except json.JSONDecodeError:
                pass

    @classmethod
    def validate(cls) -> Dict[str, Any]:
        """Validate settings and return any issues"""
        issues = []
        warnings = []

        if not cls.S3_BUCKET:
            issues.append("S3_BUCKET is required")

        if not cls.DYNAMODB_TABLE_NAME:
            issues.append("DYNAMODB_TABLE_NAME is required")

        if cls.FLAG_THRESHOLD >= cls.AUTO_ACCEPT_THRESHOLD:
            issues.append("FLAG_THRESHOLD must be less than AUTO_ACCEPT_THRESHOLD")

        # Validate OCR engine
        if cls.OCR_ENGINE not in ("easyocr", "rapidocr"):
            issues.append(f"OCR_ENGINE must be 'easyocr' or 'rapidocr', got '{cls.OCR_ENGINE}'")

        # Warn if prompt ID is not set for default process
        if not cls.PROMPT_ID_EXTRACTION:
            warnings.append("PROMPT_ID_EXTRACTION is not set - will use fallback prompt")

        # Warn if Salesforce is enabled but settings are incomplete
        if cls.SF_ENABLED:
            sf_required = [cls.SF_TOKEN_TABLE_NAME, cls.SF_HOST, cls.SF_SECRET_NAME]
            if not all(sf_required):
                warnings.append("Salesforce is enabled but configuration is incomplete")
            if not cls.SF_APEX_PATH:
                warnings.append("SF_APEX_PATH is not set - Salesforce POST will fail")
        else:
            warnings.append("Salesforce integration is disabled (SF_ENABLED=false)")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings
        }

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Export settings as dictionary (for logging/debugging)"""
        return {
            "aws_region": cls.REGION,
            "s3_bucket": cls.S3_BUCKET,
            "dynamodb_table": cls.DYNAMODB_TABLE_NAME,
            "default_process": cls.DEFAULT_PROCESS,
            "models": {
                "extraction": cls.EXTRACTION_MODEL
            },
            "prompt_ids": {
                "extraction": cls.PROMPT_ID_EXTRACTION[:20] + "..." if cls.PROMPT_ID_EXTRACTION else "NOT SET"
            },
            "confidence_thresholds": {
                "auto_accept": cls.AUTO_ACCEPT_THRESHOLD,
                "flag": cls.FLAG_THRESHOLD
            },
            "ocr": {
                "engine": cls.OCR_ENGINE,
                "lang": cls.OCR_LANG,
                "force_ocr": cls.FORCE_OCR
            },
            "image_processing": {
                "dpi": cls.PDF_IMAGE_DPI,
                "max_dimension": cls.MAX_IMAGE_DIMENSION
            },
            "salesforce": {
                "enabled": cls.SF_ENABLED,
                "configured": bool(cls.SF_HOST and cls.SF_TOKEN_TABLE_NAME),
                "apex_path": cls.SF_APEX_PATH
            }
        }


# Create a singleton instance
settings = Settings()
