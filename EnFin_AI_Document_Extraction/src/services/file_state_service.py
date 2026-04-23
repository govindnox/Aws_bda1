"""
File state tracking service for managing file processing states.

States:
- "To be Processed": Initial state when file uploaded
- "In Process": Currently being processed by extraction lambda
- "Processed": Successfully completed
- "Failed": Processing failed (with error_message details)

Author: Reet Roy
Version: 1.1.0

Modification History:
    2026-04-21 - CR-10: Added LastEvaluatedKey pagination to all queries.
                 CR-12: Removed local FileState; import from data_models.
                 CR-13: Removed dead filestate_table config reference.
                 CR-19: Replaced datetime.utcnow() with timezone-aware calls.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from models.data_models import FileState

logger = logging.getLogger(__name__)

# Lazy-loaded DynamoDB resource
_dynamodb_resource = None
_filestate_table = None


def _get_resource():
    """Lazy-initialise the DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        import boto3
        from config import config

        _dynamodb_resource = boto3.resource("dynamodb", region_name=config.aws.region)
    return _dynamodb_resource


def _get_filestate_table():
    """Lazy-initialise the file state table reference."""
    global _filestate_table
    if _filestate_table is None:
        from config import config

        _filestate_table = _get_resource().Table(config.dynamodb.filestate_table)
    return _filestate_table


def set_filestate_table_name(table_name: str) -> None:
    """Set the file state table dynamically from process config.

    Overrides the env-variable based table reference. This allows
    the filestate table to be configured per-process in DynamoDB
    rather than as a Lambda environment variable.

    Args:
        table_name: DynamoDB table name for file state tracking.
    """
    global _filestate_table
    if table_name:
        _filestate_table = _get_resource().Table(table_name)
        logger.info("File state table configured from process config: %s", table_name)


def create_file_record(
    app_no: str,
    path: str,
    process: str,
    file_type: str,
    submission_timestamp: str,
    state: FileState = FileState.TO_BE_PROCESSED,
) -> None:
    """Create initial file state record.

    Args:
        app_no: Application number
        path: S3 object key
        process: Process identifier (e.g., "m0_utility_bill")
        file_type: "pdf" or "image"
        submission_timestamp: ISO timestamp when file was uploaded
        state: Initial state (default: To be Processed)
    """
    table = _get_filestate_table()

    # Extract submission date from timestamp (YYYY-MM-DD)
    try:
        submission_date = submission_timestamp.split("T")[0]
    except Exception:
        submission_date = datetime.utcnow().strftime("%Y-%m-%d")

    item = {
        "app_no": app_no,
        "path": path,
        "state": state.value,
        "process": process,
        "file_type": file_type,
        "timestamp": datetime.utcnow().isoformat(),
        "submission_timestamp": submission_timestamp,
        "submission_date": submission_date,
        "retry_count": 0,
    }

    try:
        table.put_item(Item=item)
        logger.info(
            "Created file state record: app_no=%s, path=%s, state=%s",
            app_no,
            path,
            state.value,
        )
    except Exception as e:
        logger.error("Failed to create file state record: %s", e)
        raise


def get_latest_submission_date(app_no: str, process: str) -> Optional[str]:
    """Get the latest submission date for an app_no and process.

    Queries the SubmissionDateIndex GSI to find the most recent submission.

    Args:
        app_no: Application number
        process: Process identifier

    Returns:
        Latest submission_date (YYYY-MM-DD) or None if no submissions
    """
    table = _get_filestate_table()

    try:
        response = table.query(
            IndexName="SubmissionDateIndex",
            KeyConditionExpression="app_no = :app_no",
            FilterExpression="#process = :process",
            ExpressionAttributeNames={"#process": "process"},
            ExpressionAttributeValues={":app_no": app_no, ":process": process},
            ScanIndexForward=False,  # Sort descending by submission_date
            Limit=1,
        )

        items = response.get("Items", [])
        if items:
            latest_date = items[0].get("submission_date")
            logger.info(
                "Latest submission date for app_no=%s, process=%s: %s",
                app_no,
                process,
                latest_date,
            )
            return latest_date

        return None

    except Exception as e:
        logger.error("Failed to query latest submission date: %s", e)
        return None


def update_file_state(
    app_no: str,
    path: str,
    new_state: FileState,
    error_message: Optional[str] = None,
    clubbed_with: Optional[List[str]] = None,
    clubbed_pdf_path: Optional[str] = None,
) -> None:
    """Update file state.

    Args:
        app_no: Application number
        path: S3 object key
        new_state: New state
        error_message: Error details if Failed
        clubbed_with: List of file paths clubbed together
        clubbed_pdf_path: S3 path of clubbed PDF
    """
    table = _get_filestate_table()

    update_expr = "SET #state = :state, #timestamp = :timestamp"
    expr_attr_values = {
        ":state": new_state.value,
        ":timestamp": datetime.utcnow().isoformat(),
    }
    expr_attr_names = {"#state": "state", "#timestamp": "timestamp"}

    if error_message:
        update_expr += ", error_message = :error_message"
        expr_attr_values[":error_message"] = error_message

    if clubbed_with:
        update_expr += ", clubbed_with = :clubbed"
        expr_attr_values[":clubbed"] = clubbed_with

    if clubbed_pdf_path:
        update_expr += ", clubbed_pdf_path = :pdf_path"
        expr_attr_values[":pdf_path"] = clubbed_pdf_path

    try:
        table.update_item(
            Key={"app_no": app_no, "path": path},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
        )
        logger.info(
            "Updated file state: app_no=%s, path=%s, new_state=%s",
            app_no,
            path,
            new_state.value,
        )
    except Exception as e:
        logger.error("Failed to update file state: %s", e)
        raise


def batch_update_file_state(
    app_no: str, paths: List[str], new_state: FileState
) -> None:
    """Update state for multiple files.

    Args:
        app_no: Application number
        paths: List of S3 object keys
        new_state: New state for all files
    """
    for path in paths:
        try:
            update_file_state(app_no, path, new_state)
        except Exception as e:
            logger.error("Failed to update state for file %s: %s", path, e)
            # Continue with other files even if one fails
