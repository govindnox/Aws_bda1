"""
DynamoDB service for extraction table and process configuration.

Extraction Table (File-Level Design):
    PK: ``path`` (S3 object key)
    Attributes:
        - app_no: Application number
        - process: Process identifier
        - state: "To be Processed" | "In Process" | "Processed" | "Failed"
        - file_type: "pdf" | "image"
        - submission_date: YYYY-MM-DD
        - submission_timestamp: ISO timestamp
        - timestamp: ISO timestamp (last update)
        - retry_count: int
        - extracted_json_string_with_confidence: JSON (when Processed)
        - extraction_timestamp: timestamp (when Processed)
        - sf_response: Salesforce API response (when pushed)
        - sf_push_timestamp: timestamp (when pushed to SF)
        - error_message: string (when Failed)

Config table:
    PK: ``process``  (e.g. ``"m0_utility_bill"``)
    Contains entity definitions, prompt template, conditional responses,
    extraction_table name, and SF endpoint configuration.

Author: Reet Roy
Version: 3.0.0 (File-level design)
"""

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from enum import Enum

logger = logging.getLogger(__name__)

# Lazy-loaded DynamoDB resource and table references
_dynamodb_resource = None
_extraction_table = None
_config_table = None

# In-memory cache for process configs  {process: (config_dict, fetch_time)}
_config_cache: Dict[str, tuple] = {}


class FileState(Enum):
    """File processing states."""

    TO_BE_PROCESSED = "To be Processed"
    IN_PROCESS = "In Process"
    PROCESSED = "Processed"
    FAILED = "Failed"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _convert_floats(obj: Any) -> Any:
    """Recursively convert ``float`` → ``Decimal`` for DynamoDB.

    Args:
        obj: Any Python object (dict, list, float, etc.).

    Returns:
        The object with all floats converted to Decimal instances.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_floats(i) for i in obj]
    return obj


def _get_resource():
    """Lazy-initialise the DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        import boto3
        from config import config

        _dynamodb_resource = boto3.resource("dynamodb", region_name=config.aws.region)
    return _dynamodb_resource


def _get_extraction_table():
    """Get the extraction table reference from environment variable.

    The table name is read from DYNAMODB_EXTRACTION_TABLE environment variable.
    """
    global _extraction_table
    if _extraction_table is None:
        import os
        table_name = os.environ.get("DYNAMODB_EXTRACTION_TABLE")
        if table_name:
            _extraction_table = _get_resource().Table(table_name)
            logger.info("Extraction table initialized: %s", table_name)
        else:
            logger.warning("DYNAMODB_EXTRACTION_TABLE environment variable not set")
    return _extraction_table


def _get_config_table():
    """Lazy-initialise the config DynamoDB table reference."""
    global _config_table
    if _config_table is None:
        from config import config

        _config_table = _get_resource().Table(config.dynamodb.config_table)
    return _config_table


# ---------------------------------------------------------------------------
# Extraction table operations (file-level storage)
# ---------------------------------------------------------------------------


def create_file_record(
    path: str,
    app_no: str,
    process: str,
    file_type: str,
    submission_timestamp: str,
    state: FileState = FileState.TO_BE_PROCESSED,
) -> None:
    """Create initial file state record in the extraction table.

    Args:
        path: S3 object key (partition key).
        app_no: Application number.
        process: Process identifier.
        file_type: "pdf" or "image".
        submission_timestamp: ISO timestamp when file was uploaded.
        state: Initial state (default: To be Processed).
    """
    table = _get_extraction_table()
    if table is None:
        raise RuntimeError(
            "Extraction table not configured — call "
            "set_extraction_table_name() first"
        )

    # Extract submission date from timestamp (YYYY-MM-DD)
    try:
        submission_date = submission_timestamp.split("T")[0]
    except Exception:
        submission_date = datetime.utcnow().strftime("%Y-%m-%d")

    item = _convert_floats(
        {
            "path": path,
            "app_no": app_no,
            "process": process,
            "state": state.value,
            "file_type": file_type,
            "timestamp": datetime.utcnow().isoformat(),
            "submission_timestamp": submission_timestamp,
            "submission_date": submission_date,
            "retry_count": 0,
        }
    )

    try:
        table.put_item(Item=item)
        logger.info(
            "Created file record: path=%s, app_no=%s, state=%s",
            path,
            app_no,
            state.value,
        )
    except Exception as e:
        logger.error("Failed to create file record: %s", e)
        raise


def update_file_state(
    path: str,
    process: str,
    new_state: FileState,
    error_message: Optional[str] = None,
    extracted_json: Optional[str] = None,
    extraction_timestamp: Optional[str] = None,
    sf_response: Optional[str] = None,
    sf_push_timestamp: Optional[str] = None,
) -> None:
    """Update file state and optionally store extraction results and SF response.

    Args:
        path: S3 object key (partition key).
        process: Process identifier (sort key).
        new_state: New state.
        error_message: Error details if Failed.
        extracted_json: Extraction result JSON if Processed.
        extraction_timestamp: Extraction timestamp if Processed.
        sf_response: Salesforce API response JSON.
        sf_push_timestamp: Timestamp when pushed to SF.
    """
    table = _get_extraction_table()
    if table is None:
        raise RuntimeError(
            "Extraction table not configured — call "
            "set_extraction_table_name() first"
        )

    update_expr = "SET #state = :state, #ts = :ts"
    expr_attr_names = {"#state": "state", "#ts": "timestamp"}
    expr_attr_values = _convert_floats(
        {":state": new_state.value, ":ts": datetime.utcnow().isoformat()}
    )

    if error_message:
        update_expr += ", error_message = :error_message"
        expr_attr_values[":error_message"] = error_message

    if extracted_json:
        update_expr += ", extracted_json_string_with_confidence = :result"
        expr_attr_values[":result"] = extracted_json

    if extraction_timestamp:
        update_expr += ", extraction_timestamp = :ext_ts"
        expr_attr_values[":ext_ts"] = extraction_timestamp

    if sf_response:
        update_expr += ", sf_response = :sf_resp"
        expr_attr_values[":sf_resp"] = sf_response

    if sf_push_timestamp:
        update_expr += ", sf_push_timestamp = :sf_ts"
        expr_attr_values[":sf_ts"] = sf_push_timestamp

    try:
        table.update_item(
            Key={"path": path, "process": process},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
        )
        logger.info("Updated file state: path=%s, process=%s, new_state=%s", path, process, new_state.value)
    except Exception as e:
        logger.error("Failed to update file state: %s", e)
        raise


def get_file_record(path: str, process: str) -> Optional[Dict[str, Any]]:
    """Fetch a single file record by path and process.

    Args:
        path: S3 object key (partition key).
        process: Process identifier (sort key).

    Returns:
        DynamoDB item dict or None if not found.
    """
    table = _get_extraction_table()
    if table is None:
        logger.error(
            "Extraction table not configured — "
            "call set_extraction_table_name() first"
        )
        return None

    try:
        response = table.get_item(Key={"path": path, "process": process})
        item = response.get("Item")
        if item:
            logger.info("Retrieved file record: path=%s, process=%s", path, process)
        else:
            logger.warning("No file record found: path=%s, process=%s", path, process)
        return item
    except Exception as e:
        logger.error("Failed to fetch file record for %s/%s: %s", path, process, e)
        return None


# ---------------------------------------------------------------------------
# Config table operations
# ---------------------------------------------------------------------------


def get_process_config(process: str) -> Optional[Dict[str, Any]]:
    """Fetch process configuration from DynamoDB (with caching).

    The config is cached in-memory for ``config_cache_ttl`` seconds
    to avoid repeated DynamoDB reads on warm Lambda invocations.

    Args:
        process: Process name (partition key in config table).

    Returns:
        Raw DynamoDB item dict, or ``None`` if not found.
    """
    from config import config as app_config

    # Check cache
    cached = _config_cache.get(process)
    if cached:
        config_dict, fetch_time = cached
        if time.time() - fetch_time < app_config.config_cache_ttl:
            logger.debug("Returning cached config for process=%s", process)
            return config_dict

    # Fetch from DynamoDB
    table = _get_config_table()
    try:
        response = table.get_item(Key={"process": process})
        item = response.get("Item")
        if item:
            _config_cache[process] = (item, time.time())
            logger.info("Loaded process config for '%s' from DynamoDB", process)
        else:
            logger.warning("No config found for process '%s'", process)
        return item
    except Exception:
        logger.exception("Failed to fetch config for process '%s'", process)
        return None
