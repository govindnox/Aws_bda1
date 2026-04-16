"""
File state tracking service for managing file processing states.

States:
- "To be Processed": Initial state when file uploaded
- "In Process": Currently being processed by extraction lambda
- "Processed": Successfully completed
- "Failed": Processing failed (with error_message details)

Author: Reet Roy
Version: 1.0.0
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)

# Lazy-loaded DynamoDB resource
_dynamodb_resource = None
_filestate_table = None


class FileState(Enum):
    """File processing states."""

    TO_BE_PROCESSED = "To be Processed"
    IN_PROCESS = "In Process"
    PROCESSED = "Processed"
    FAILED = "Failed"


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


def get_files_to_process(
    app_no: str, process: str, submission_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Query all files for app_no marked as 'To be Processed'.

    Args:
        app_no: Application number
        process: Process identifier
        submission_date: Optional submission date filter (YYYY-MM-DD)

    Returns:
        List of file state records
    """
    table = _get_filestate_table()

    try:
        filter_expr = "#state = :state AND #process = :process"
        expr_attr_names = {"#state": "state", "#process": "process"}
        expr_attr_values = {
            ":app_no": app_no,
            ":state": FileState.TO_BE_PROCESSED.value,
            ":process": process,
        }

        # Add submission_date filter if provided
        if submission_date:
            filter_expr += " AND submission_date = :submission_date"
            expr_attr_values[":submission_date"] = submission_date

        response = table.query(
            KeyConditionExpression="app_no = :app_no",
            FilterExpression=filter_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
        )

        items = response.get("Items", [])
        logger.info(
            "Found %d files to process for app_no=%s, process=%s, submission_date=%s",
            len(items),
            app_no,
            process,
            submission_date,
        )
        return items

    except Exception as e:
        logger.error("Failed to query files to process: %s", e)
        return []


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


def check_submission_complete(app_no: str, submission_date: str) -> bool:
    """Check if all files for a submission are in terminal state.

    A terminal state is either "Processed" or "Failed".

    Args:
        app_no: Application number
        submission_date: Submission date (YYYY-MM-DD)

    Returns:
        True if all files are in terminal state, False otherwise
    """
    table = _get_filestate_table()

    try:
        response = table.query(
            KeyConditionExpression="app_no = :app_no",
            FilterExpression="submission_date = :submission_date",
            ExpressionAttributeValues={
                ":app_no": app_no,
                ":submission_date": submission_date,
            },
        )

        items = response.get("Items", [])
        if not items:
            logger.warning(
                "No files found for app_no=%s, submission_date=%s",
                app_no,
                submission_date,
            )
            return False

        # Check if all files are in terminal state
        terminal_states = [FileState.PROCESSED.value, FileState.FAILED.value]
        all_complete = all(item.get("state") in terminal_states for item in items)

        logger.info(
            "Submission completion check: app_no=%s, submission_date=%s, "
            "total_files=%d, all_complete=%s",
            app_no,
            submission_date,
            len(items),
            all_complete,
        )

        return all_complete

    except Exception as e:
        logger.error("Failed to check submission completion: %s", e)
        return False


def get_failed_files(app_no: str, submission_date: str) -> List[str]:
    """Get list of files that failed processing for a submission.

    Args:
        app_no: Application number
        submission_date: Submission date (YYYY-MM-DD)

    Returns:
        List of file paths that failed
    """
    table = _get_filestate_table()

    try:
        response = table.query(
            KeyConditionExpression="app_no = :app_no",
            FilterExpression="submission_date = :submission_date AND #state = :state",
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues={
                ":app_no": app_no,
                ":submission_date": submission_date,
                ":state": FileState.FAILED.value,
            },
        )

        items = response.get("Items", [])
        failed_files = [item.get("path") for item in items]

        logger.info(
            "Found %d failed files for app_no=%s, submission_date=%s",
            len(failed_files),
            app_no,
            submission_date,
        )

        return failed_files

    except Exception as e:
        logger.error("Failed to query failed files: %s", e)
        return []


def get_all_files_for_submission(
    app_no: str, submission_date: str
) -> List[Dict[str, Any]]:
    """Get all file state records for a submission.

    Args:
        app_no: Application number
        submission_date: Submission date (YYYY-MM-DD)

    Returns:
        List of all file state records for the submission
    """
    table = _get_filestate_table()

    try:
        response = table.query(
            KeyConditionExpression="app_no = :app_no",
            FilterExpression="submission_date = :submission_date",
            ExpressionAttributeValues={
                ":app_no": app_no,
                ":submission_date": submission_date,
            },
        )

        items = response.get("Items", [])
        logger.info(
            "Retrieved %d file records for app_no=%s, submission_date=%s",
            len(items),
            app_no,
            submission_date,
        )

        return items

    except Exception as e:
        logger.error("Failed to get files for submission: %s", e)
        return []
