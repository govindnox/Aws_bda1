"""
DynamoDB service for extraction aggregation.

Provides query functions for aggregating extraction results across
multiple files for a given ``app_no`` and ``process``.

Uses the ``app_no-index`` GSI on the Result table to efficiently
query all files belonging to an application.

Author: Reet Roy
Version: 1.0.0
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import boto3

logger = logging.getLogger(__name__)

# Lazy-loaded DynamoDB resource and table references
_dynamodb_resource = None
_extraction_table = None
_config_table = None

# In-memory cache for process configs  {process: (config_dict, fetch_time)}
_config_cache: Dict[str, tuple] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_resource():
    """Lazy-initialise the DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        region = os.environ.get("REGION", "us-west-2")
        _dynamodb_resource = boto3.resource("dynamodb", region_name=region)
    return _dynamodb_resource


def _get_extraction_table():
    """Get the extraction table reference from environment variable."""
    global _extraction_table
    if _extraction_table is None:
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
        table_name = os.environ.get("DYNAMODB_CONFIG_TABLE")
        if table_name:
            _config_table = _get_resource().Table(table_name)
    return _config_table


# ---------------------------------------------------------------------------
# Config table operations
# ---------------------------------------------------------------------------


def get_process_config(process: str) -> Optional[Dict[str, Any]]:
    """Fetch process configuration from DynamoDB (with caching).

    The config is cached in-memory for ``CONFIG_CACHE_TTL`` seconds
    to avoid repeated DynamoDB reads on warm Lambda invocations.

    Args:
        process: Process name (partition key in config table).

    Returns:
        Raw DynamoDB item dict, or ``None`` if not found.
    """
    ttl = int(os.environ.get("CONFIG_CACHE_TTL_SECONDS", "300"))

    # Check cache
    cached = _config_cache.get(process)
    if cached:
        config_dict, fetch_time = cached
        if time.time() - fetch_time < ttl:
            logger.debug("Returning cached config for process=%s", process)
            return config_dict

    # Fetch from DynamoDB
    table = _get_config_table()
    if table is None:
        logger.error("Config table not configured")
        return None

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


# ---------------------------------------------------------------------------
# Extraction table operations — aggregation queries
# ---------------------------------------------------------------------------


def query_processed_files(app_no: str, process: str) -> List[Dict[str, Any]]:
    """Query all processed files for an app_no and process.

    Uses the ``app_no-process-index`` GSI on the Result table with ``process``
    as the sort key.

    Args:
        app_no: Application number.
        process: Process identifier.

    Returns:
        List of DynamoDB items for processed files.
    """
    table = _get_extraction_table()
    if table is None:
        logger.error("Extraction table not configured")
        return []

    try:
        items = []
        last_evaluated_key = None

        while True:
            query_kwargs = {
                "IndexName": "app_no-process-index",
                "KeyConditionExpression": ("app_no = :app_no AND process = :process"),
                "FilterExpression": "#state = :state",
                "ExpressionAttributeNames": {
                    "#state": "state",
                },
                "ExpressionAttributeValues": {
                    ":app_no": app_no,
                    ":process": process,
                    ":state": "Processed",
                },
                "ScanIndexForward": False,
            }

            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = table.query(**query_kwargs)
            items.extend(response.get("Items", []))

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        logger.info(
            "Found %d processed files for app_no=%s, process=%s",
            len(items),
            app_no,
            process,
        )
        return items

    except Exception:
        logger.exception(
            "Failed to query processed files for app_no=%s, process=%s",
            app_no,
            process,
        )
        return []


def update_latest_submission_flag(path: str, process: str, is_latest: bool) -> None:
    """Update the latest_submission flag for a file record.

    Args:
        path: S3 object key (partition key).
        process: Process identifier (sort key).
        is_latest: Whether this file is part of the latest submission.
    """
    table = _get_extraction_table()
    if table is None:
        logger.error("Extraction table not configured")
        return

    try:
        table.update_item(
            Key={"path": path, "process": process},
            UpdateExpression="SET latest_submission = :latest",
            ExpressionAttributeValues={":latest": is_latest},
        )
        logger.debug(
            "Updated latest_submission=%s for path=%s, process=%s",
            is_latest,
            path,
            process,
        )
    except Exception:
        logger.exception("Failed to update latest_submission for path=%s", path)
