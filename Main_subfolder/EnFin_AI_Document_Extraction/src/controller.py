"""
Controller for Document Extraction.

Simplified file-level processing flow:
    1. Parse SQS message (extract ``path``, ``process``, ``app_no``)
    2. Load process config and set extraction table
    3. Update file state to "In Process"
    4. Process the single file (PDF or image)
    5. Store extraction result in DynamoDB
    6. Push extraction result to Salesforce
    7. Update file state to "Processed" with SF response
    8. On error_message: Update file state to "Failed" with error_message message

Tags:
    Application: Document-Extraction
    Environment: Dev

Author: Reet Roy
Version: 3.0.0 (File-level processing)
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict
import pytz

from config import config
from models.data_models import (
    ExtractionResult,
    ProcessConfig,
    SQSInputMessage,
)
from services import dynamodb_service, s3_service
from services.dynamodb_service import FileState

logger = logging.getLogger(__name__)


class ExtractionController:
    """Controller for processing SQS extraction events."""

    def __init__(self):
        """Initialise the controller.

        No heavy packages are loaded here — everything is
        lazy-initialised on first use inside ``_process_record``.
        """
        self._pipeline_cache: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def handle_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle the SQS Lambda event.

        Processes each record independently and returns SQS-compatible
        ``batchItemFailures`` for partial-batch retry.

        Args:
            event: Lambda event with ``Records``.

        Returns:
            ``{ "batchItemFailures": [...] }``
        """
        batch_failures: list = []

        for record in event.get("Records", []):
            message_id = record.get("messageId", "unknown")
            try:
                self._process_record(record)
            except Exception:
                logger.exception("Failed to process record %s", message_id)
                batch_failures.append({"itemIdentifier": message_id})

        if batch_failures:
            logger.warning("Returning %d batch failures", len(batch_failures))

        return {"batchItemFailures": batch_failures}

    # ------------------------------------------------------------------
    # Per-record processing
    # ------------------------------------------------------------------

    def _process_record(self, record: Dict[str, Any]) -> None:
        """Process a single SQS record (one file).

        Args:
            record: Raw SQS record dict.

        Returns:
            None
        """
        # 1 — Parse message
        message = SQSInputMessage.from_sqs_record(record)
        logger.info(
            "Processing: app_no=%s, path=%s, process=%s",
            message.app_no,
            message.path,
            message.process,
        )

        # 2 — Load process config
        process_config = self._load_process_config(message.process)

        # 3 — Update file state to "In Process"
        dynamodb_service.update_file_state(
            path=message.path,
            process=message.process,
            new_state=FileState.IN_PROCESS,
        )

        try:
            # 4 — Process the file
            result = self._process_single_file(
                path=message.path,
                app_no=message.app_no,
                process_config=process_config,
            )

            # 5 — Store result in DynamoDB
            pt_tz = pytz.timezone(config.timezone)
            timestamp = datetime.now(pt_tz).strftime(config.timestamp_format)

            dynamodb_service.update_file_state(
                path=message.path,
                process=message.process,
                new_state=FileState.PROCESSED,
                extracted_json=json.dumps(result.to_dict()),
                extraction_timestamp=timestamp,
            )

            # 6 — Enqueue to Salesforce output queue (if SF enabled)
            if config.salesforce.enabled:
                self._enqueue_for_salesforce_push(
                    path=message.path,
                    process=message.process,
                    process_config=process_config,
                )

            # 7 — Enqueue aggregation trigger (if aggregation enabled)
            if (
                config.aggregation.enabled
                and process_config.aggregation_config.enabled
            ):
                self._enqueue_for_aggregation(
                    app_no=message.app_no,
                    process=message.process,
                )

            logger.info(
                "File processed successfully: %s (confidence=%.2f, sf_enqueued=%s, agg_enqueued=%s)",
                message.path,
                result.overall_confidence,
                config.salesforce.enabled,
                config.aggregation.enabled
                and process_config.aggregation_config.enabled,
            )

        except Exception as e:
            logger.exception("Failed to process file: %s", message.path)
            dynamodb_service.update_file_state(
                path=message.path,
                process=message.process,
                new_state=FileState.FAILED,
                error_message=str(e),
            )
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _process_single_file(
        self,
        path: str,
        app_no: str,
        process_config: ProcessConfig,
    ) -> ExtractionResult:
        """Process a single file through the extraction pipeline.

        Args:
            path: S3 object key.
            app_no: Application number.
            process_config: Process configuration.

        Returns:
            ExtractionResult with confidence scores.

        Raises:
            Exception: If download or extraction fails.
        """
        # Download document from S3
        file_bytes = s3_service.download_document(path)

        # Run extraction pipeline
        pipeline = self._get_pipeline(process_config)
        result: ExtractionResult = pipeline.run(
            file_bytes=file_bytes,
            path=path,
            app_no=app_no,
        )

        logger.info(
            "Extraction complete: path=%s, confidence=%.2f, recommendation=%s",
            path,
            result.overall_confidence,
            result.recommendation,
        )

        return result

    def _enqueue_for_salesforce_push(
        self,
        path: str,
        process: str,
        process_config: ProcessConfig,
    ) -> None:
        """Enqueue extraction result for Salesforce push.

        Args:
            path: S3 object key.
            process: Process identifier.
            process_config: Process configuration.

        Raises:
            Exception: If SQS send fails (logged but doesn't fail extraction).
        """
        # Get output SQS URL from global config
        output_sqs_url = config.sqs.output_queue_url

        if not output_sqs_url:
            logger.warning(
                "Output SQS URL not configured — skipping SF enqueue for process '%s'",
                process,
            )
            return

        # Build message payload (extraction_table is now global env var)
        message_body = {
            "path": path,
            "process": process,
        }

        # Get message group ID and app_no for FIFO
        app_no = self._extract_app_no_from_path(path)
        message_group_id = f"{app_no}+{process}"

        # Use path as deduplication ID (prevents duplicate pushes)
        import boto3

        sqs = boto3.client("sqs", region_name=config.aws.region)

        try:
            sqs.send_message(
                QueueUrl=output_sqs_url,
                MessageBody=json.dumps(message_body),
                MessageGroupId=message_group_id,
            )
            logger.info(
                "Enqueued to SF output queue: path=%s, group=%s",
                path,
                message_group_id,
            )
        except Exception as e:
            logger.exception("Failed to enqueue to SF output queue: %s", e)
            # Don't fail the extraction - SF push will be missing but extraction succeeded
            # Could add a retry mechanism or alert here

    def _enqueue_for_aggregation(
        self,
        app_no: str,
        process: str,
    ) -> None:
        """Enqueue aggregation trigger to the aggregation SQS FIFO queue.

        Uses ``app_no`` as the ``MessageGroupId`` so all files for the same
        application are aggregated sequentially.  Content-based deduplication
        handles duplicates automatically.

        Args:
            app_no: Application number.
            process: Process identifier.
        """
        aggregation_sqs_url = config.aggregation.queue_url

        if not aggregation_sqs_url:
            logger.warning(
                "Aggregation SQS URL not configured — skipping aggregation "
                "enqueue for app_no='%s', process='%s'",
                app_no,
                process,
            )
            return

        message_body = {
            "app_no": app_no,
            "process": process,
        }

        import boto3

        sqs = boto3.client("sqs", region_name=config.aws.region)

        try:
            sqs.send_message(
                QueueUrl=aggregation_sqs_url,
                MessageBody=json.dumps(message_body),
                MessageGroupId=app_no,
                MessageDeduplicationId=f"{app_no}+{process}",
            )
            logger.info(
                "Enqueued aggregation trigger: app_no=%s, process=%s",
                app_no,
                process,
            )
        except Exception as e:
            logger.exception(
                "Failed to enqueue aggregation trigger: %s", e
            )
            # Don't fail the extraction — aggregation can be retried
            # independently

    @staticmethod
    def _extract_app_no_from_path(path: str) -> str:
        """Extract app_no from path (TPO/{app_no}/...).

        Args:
            path: S3 object key.

        Returns:
            Extracted application number or "unknown".
        """
        segments = [s for s in path.split("/") if s]
        return segments[1] if len(segments) >= 2 else "unknown"

    @staticmethod
    def _load_process_config(process: str) -> ProcessConfig:
        """Fetch process config from DynamoDB.

        Args:
            process: Process identifier.

        Returns:
            Typed ``ProcessConfig``.

        Raises:
            ValueError: If no config is found for the process.
        """
        item = dynamodb_service.get_process_config(process)
        if not item:
            raise ValueError(
                f"No configuration found for process '{process}' "
                "in the config DynamoDB table"
            )
        return ProcessConfig.from_dynamo_item(item)

    def _get_pipeline(self, process_config: ProcessConfig):
        """Get or create an ExtractionPipeline for the process.

        Lazy-imports the pipeline module to defer heavy-package
        loading (Docling, torch, etc.).

        Args:
            process_config: Parsed ProcessConfig object containing the
                extraction rules and settings for the target process.

        Returns:
            An instance of ExtractionPipeline configured for the
            given process.
        """
        cache_key = process_config.process
        if cache_key not in self._pipeline_cache:
            from extractors.extraction_pipeline import ExtractionPipeline

            self._pipeline_cache[cache_key] = ExtractionPipeline(process_config)
        return self._pipeline_cache[cache_key]
