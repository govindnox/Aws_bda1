"""
Salesforce Push Controller for Document Extraction.

Consumes messages from the Salesforce output SQS FIFO queue,
reads extraction results from DynamoDB, and pushes to Salesforce.

Flow:
    1. Parse SQS message (path, process, extraction_table)
    2. Read file record from DynamoDB
    3. Validate extraction result exists and is in Processed state
    4. Push to Salesforce using TokenManager and existing logic
    5. Update DynamoDB with SF response and timestamp
    6. On error_message: Update DynamoDB with error_message, raise exception for SQS retry

Message Format:
    {
        "path": "S3 object key",
        "process": "process identifier",
        "extraction_table": "DynamoDB table name"
    }

Author: Reet Roy
Version: 1.0.0
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from token_manager import TokenManager

from config import config
from models.data_models import SQSOutputMessage
from services import dynamodb_service
from services.dynamodb_service import FileState

logger = logging.getLogger(__name__)


class SalesforcePushController:
    """Controller for Salesforce push operations."""

    def __init__(self):
        """Initialize the controller."""
        pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def handle_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle SQS Lambda event for SF push.

        Processes each record independently and returns SQS-compatible
        partial batch failure response.

        Args:
            event: SQS Lambda event with Records array.

        Returns:
            Dict with batchItemFailures for failed records.
        """
        batch_failures = []

        for record in event.get("Records", []):
            message_id = record.get("messageId", "unknown")
            try:
                self._process_record(record)
            except Exception:
                logger.exception("Failed to process SF push for record %s", message_id)
                batch_failures.append({"itemIdentifier": message_id})

        if batch_failures:
            logger.warning("Returning %d batch failures", len(batch_failures))

        return {"batchItemFailures": batch_failures}

    # ------------------------------------------------------------------
    # Private Methods
    # ------------------------------------------------------------------

    def _process_record(self, record: Dict[str, Any]) -> None:
        """Process a single SQS record.

        Args:
            record: Single SQS record from Lambda event.

        Raises:
            ValueError: If file record not found or invalid state.
            Exception: If SF push fails (triggers SQS retry).
        """
        # 1 — Parse message
        message = SQSOutputMessage.from_sqs_record(record)
        logger.info(
            "Processing SF push: path=%s, process=%s",
            message.path,
            message.process,
        )

        # 2 — Read file record from DynamoDB
        file_record = dynamodb_service.get_file_record(message.path, message.process)
        if not file_record:
            raise ValueError(f"File record not found: {message.path}")

        # 4 — Validate state
        if (file_record.get("state") != FileState.PROCESSED.value and
                file_record.get("state") != FileState.FAILED.value):
            logger.warning(
                "File not in Processed or Failed state: %s (state=%s)",
                message.path,
                file_record.get("state"),
            )
            # Don't retry - file hasn't been extracted yet or failed
            return

        # 5 — Parse extraction result
        extracted_json = file_record.get("extracted_json_string_with_confidence")
        try:
            extraction_result = json.loads(extracted_json)
        except Exception as e:
            logger.exception(f"Failed to parse extraction result: {e}")
            extraction_result = {}

        # 6 - Parse Error if exists
        error_message = file_record.get("error_message")

        # 7 — Push to Salesforce
        try:
            sf_response = self._push_to_salesforce(
                app_no=file_record.get("app_no"),
                path=message.path,
                process=message.process,
                extraction_result=extraction_result,
                error_message=error_message,
                submission_timestamp=file_record.get("submission_timestamp", ""),
            )

            # 8 — Update DynamoDB with SF response
            if sf_response:
                dynamodb_service.update_file_state(
                    path=message.path,
                    process=message.process,
                    new_state=FileState.PROCESSED,  # Keep state as Processed
                    sf_response=json.dumps(sf_response),
                    sf_push_timestamp=datetime.utcnow().isoformat(),
                )
                logger.info("SF push successful: %s", message.path)
            else:
                raise ValueError("SF push returned empty response")

        except Exception as e:
            logger.exception("SF push failed: %s", message.path)
            # Update DynamoDB with error_message (but keep state as Processed)
            dynamodb_service.update_file_state(
                path=message.path,
                process=message.process,
                new_state=FileState.PROCESSED,
                sf_response=json.dumps({"error_message": str(e)}),
                sf_push_timestamp=datetime.utcnow().isoformat(),
            )
            raise  # Re-raise for SQS retry

    def _push_to_salesforce(
        self,
        app_no: str,
        path: str,
        process: str,
        extraction_result: Dict[str, Any],
        error_message: Optional[str],
        submission_timestamp: str,
    ) -> Optional[Dict[str, Any]]:
        """Push extraction result to Salesforce.

        This logic is extracted from controller.py:_push_to_salesforce()
        with minimal modifications.

        Args:
            app_no: Application number.
            path: S3 object key.
            process: Process identifier.
            extraction_result: Extraction result dict.
            submission_timestamp: Submission timestamp for conflict resolution.

        Returns:
            Salesforce API response dict or None if SF disabled/failed.
        """
        # Check if SF is enabled
        if not config.salesforce.enabled:
            logger.info("Salesforce push disabled — skipping")
            return None

        try:
            sf_endpoint = config.salesforce.apex_path

            if not sf_endpoint:
                logger.warning("SF endpoint not configured — skipping push")
                return None

            # Build per-file payload for SF
            # Convert ExtractedField dicts to format expected by Salesforce
            fields_list = []
            for field_name, field in extraction_result.get("fields", {}).items():
                fields_list.append({
                    "name": field_name,
                    "value": field.get("value"),
                    "confidence": field.get("confidence"),
                    "confidence_score": field.get("confidence_score"),
                    "reasoning": field.get("reasoning"),
                    "page": field.get("page"),
                    "section": field.get("section"),
                })

            # Create per-file data payload
            file_data = {
                "process": process,
                "applicationNo": app_no,
                "filePath": path,
                "submissionTimestamp": submission_timestamp,
                "fields": fields_list,
                "error_message": error_message,
                "overall_confidence": extraction_result.get("overall_confidence"),
                "recommendation": extraction_result.get("recommendation"),
                "is_supported_document": extraction_result.get("is_supported_document"),
                "additional_response": extraction_result.get("additional_response", {}),
            }

            # Wrap in Salesforce Request format
            payload = {"process": process, "data": json.dumps(file_data)}

            # Get SF token
            access_token = self._get_sf_token()
            if not access_token:
                logger.warning("Failed to get SF token — skipping push")
                return None

            # Call SF Apex REST API
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            full_url = f"{config.salesforce.instance_url}{sf_endpoint}"
            logger.info("Calling Salesforce API: %s", full_url)

            response = requests.post(
                full_url, headers=headers, json=payload, timeout=30
            )

            if response.status_code not in [200, 201]:
                error_msg = (
                    f"Salesforce API returned {response.status_code}: "
                    f"{response.text}"
                )
                logger.error(error_msg)
                return {"error_message": error_msg, "status_code": response.status_code}

            sf_response = response.json()
            logger.info("Salesforce push successful: %s", sf_response)
            return sf_response

        except Exception as e:
            logger.exception("Failed to push to Salesforce: %s", e)
            return {"error_message": str(e)}

    def _get_sf_token(self) -> Optional[str]:
        """Get Salesforce OAuth token using TokenManager.

        Returns:
            Access token or None.
        """
        session_object = {
            "token_table_name": config.salesforce.token_table,
            "host": config.salesforce.host,
            "auth_path": config.salesforce.auth_path,
            "diff_time": config.salesforce.diff_time,
            "contact_center_username": config.salesforce.username,
            "region_name": config.aws.region,
            "secret_name": config.salesforce.secret_name,
        }
        token_manager = TokenManager(session_object)
        token = token_manager.get_access_token()
        return token
