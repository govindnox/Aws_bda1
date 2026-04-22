"""
Controller for BDA Result Handling.

Picks up where the extraction controller left off for BDA files.
BDA is async — the extraction Lambda kicks it off and exits, and BDA
writes its result to S3 about 50 seconds later. This controller is
triggered by that S3 write event and finishes the downstream work:
convert BDA output to ExtractionResult shape, save to DynamoDB, fire
SF push, fire aggregation trigger.

Processing flow:
    1. Parse S3 event, filter for custom_output/result.json
    2. Decode app_no + process + original path from the S3 key
    3. Read BDA result.json from S3
    4. Load process config from DynamoDB
    5. Map BDA output to ExtractionResult shape
    6. Update file state to "Processed" with extracted JSON
    7. Enqueue SF push
    8. Enqueue aggregation trigger

Tags:
    Application: Document-Extraction-BDA-Results
    Environment: Dev

Author: Govind Pandey
Version: 0.1.0
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

import pytz

from config import config
from models.data_models import ProcessConfig
from services import dynamodb_service, s3_service
from services.dynamodb_service import FileState

logger = logging.getLogger(__name__)


# Confidence classification thresholds — mirror the labels that
# ExtractionPipeline produces so downstream code treats both paths
# identically
CERTAIN_THRESHOLD = 0.90
LIKELY_THRESHOLD = 0.75

# SQS client is lazy — reused across warm invocations
_sqs_client = None


def _get_sqs_client():
    """Lazy-initialise the SQS boto3 client.

    Returns:
        boto3 SQS client scoped to the configured AWS region.
    """
    global _sqs_client
    if _sqs_client is None:
        import boto3

        _sqs_client = boto3.client("sqs", region_name=config.aws.region)
    return _sqs_client


class BDAResultController:
    """Processes S3 events emitted when BDA writes result.json."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def handle_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle the S3 Lambda event.

        One S3 event can contain multiple records. Each record represents
        one file BDA wrote. We only act on custom_output/result.json keys
        and ignore everything else BDA writes (job_metadata.json,
        standard_output, etc.).

        Args:
            event: Lambda event with ``Records``.

        Returns:
            ``{ "batchItemFailures": [...] }``.
        """
        batch_failures: list = []

        for record in event.get("Records", []):
            # S3 events don't have a simple messageId like SQS — use the
            # request ID if available, otherwise fall back to the key
            request_id = (
                record.get("responseElements", {}).get("x-amz-request-id")
                or record.get("s3", {}).get("object", {}).get("key", "unknown")
            )
            try:
                self._process_record(record)
            except Exception:
                logger.exception("Failed to process S3 record %s", request_id)
                batch_failures.append({"itemIdentifier": request_id})

        if batch_failures:
            logger.warning("Returning %d batch failures", len(batch_failures))

        return {"batchItemFailures": batch_failures}

    # ------------------------------------------------------------------
    # Per-record processing
    # ------------------------------------------------------------------

    def _process_record(self, record: Dict[str, Any]) -> None:
        """Handle one S3 event record (one BDA result file).

        Args:
            record: Single S3 event record.
        """
        # 1 — Parse S3 event for bucket + key
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = unquote_plus(s3_info.get("object", {}).get("key", ""))

        # Ignore anything that isn't the real result file. BDA writes
        # job_metadata.json and standard_output/ too — we only want
        # custom_output/.../result.json
        if not (key.endswith("result.json") and "custom_output" in key):
            logger.info("Ignoring non-result S3 object: %s", key)
            return

        logger.info("Processing BDA result: s3://%s/%s", bucket, key)

        # 2 — Decode identifiers from the S3 key
        app_no, process, original_path = _decode_identifiers_from_key(key)
        logger.info(
            "Decoded from key: app_no=%s, process=%s, path=%s",
            app_no,
            process,
            original_path,
        )

        # 3 — Read the BDA result JSON from S3
        bda_result = s3_service.read_json(key, bucket=bucket)

        # 4 — Load process config (needed for confidence mapping and the
        # aggregation_config.enabled check)
        process_config = self._load_process_config(process)

        # 5 — Map BDA output to the ExtractionResult shape downstream
        # code expects
        extraction_result_dict = _map_bda_to_extraction_result(
            bda_result=bda_result,
            process_config=process_config,
        )

        # 6 — Save to DynamoDB with state "Processed"
        pt_tz = pytz.timezone(config.timezone)
        timestamp = datetime.now(pt_tz).strftime(config.timestamp_format)

        dynamodb_service.update_file_state(
            path=original_path,
            process=process,
            new_state=FileState.PROCESSED,
            extracted_json=json.dumps(extraction_result_dict),
            extraction_timestamp=timestamp,
        )

        # 7 — Enqueue Salesforce push (if SF enabled)
        if config.salesforce.enabled:
            self._enqueue_for_salesforce_push(
                path=original_path,
                process=process,
                app_no=app_no,
            )

        # 8 — Enqueue aggregation trigger (if aggregation enabled)
        if (
            config.aggregation.enabled
            and process_config.aggregation_config.enabled
        ):
            self._enqueue_for_aggregation(
                app_no=app_no,
                process=process,
            )

        logger.info(
            "BDA result processed: path=%s, app_no=%s, confidence=%.2f",
            original_path,
            app_no,
            extraction_result_dict.get("overall_confidence", 0.0),
        )

    # ------------------------------------------------------------------
    # Helpers — these mirror ExtractionController's private helpers,
    # kept separate so changes to one controller don't break the other
    # ------------------------------------------------------------------

    @staticmethod
    def _load_process_config(process: str) -> ProcessConfig:
        """Fetch process config from DynamoDB.

        Args:
            process: Process identifier.

        Returns:
            Typed ``ProcessConfig``.

        Raises:
            ValueError: If no config is found.
        """
        item = dynamodb_service.get_process_config(process)
        if not item:
            raise ValueError(
                f"No configuration found for process '{process}' "
                "in the config DynamoDB table"
            )
        return ProcessConfig.from_dynamo_item(item)

    def _enqueue_for_salesforce_push(
        self,
        path: str,
        process: str,
        app_no: str,
    ) -> None:
        """Enqueue extraction result for Salesforce push.

        Args:
            path: S3 object key of the original document.
            process: Process identifier.
            app_no: Application number.
        """
        output_sqs_url = config.sqs.output_queue_url

        if not output_sqs_url:
            logger.warning(
                "Output SQS URL not configured — skipping SF enqueue "
                "for process '%s'",
                process,
            )
            return

        message_body = {"path": path, "process": process}
        message_group_id = f"{app_no}+{process}"

        sqs = _get_sqs_client()

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

    def _enqueue_for_aggregation(
        self,
        app_no: str,
        process: str,
    ) -> None:
        """Enqueue aggregation trigger to the aggregation SQS FIFO queue.

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

        message_body = {"app_no": app_no, "process": process}

        sqs = _get_sqs_client()

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
            logger.exception("Failed to enqueue aggregation trigger: %s", e)


# ---------------------------------------------------------------------------
# Module-level helpers — no state, safe to unit test independently
# ---------------------------------------------------------------------------


def _decode_identifiers_from_key(key: str) -> Tuple[str, str, str]:
    """Reverse the encoding bda_pipeline.py applied to the output prefix.

    Expected key shape:
        bda-output/{app_no}/{process}/{encoded_path}/{job_uuid}/0/custom_output/0/result.json

    The original path was encoded by swapping '/' -> '__' so it fits
    as a single segment.

    Args:
        key: The S3 object key of BDA's result.json.

    Returns:
        Tuple of (app_no, process, original_path).

    Raises:
        ValueError: If the key does not match the expected shape.
    """
    segments = key.split("/")
    # Expected: ['bda-output', app_no, process, encoded_path, job_uuid,
    #            '0', 'custom_output', '0', 'result.json'] — 9 segments
    if len(segments) < 9 or segments[0] != "bda-output":
        raise ValueError(
            f"Unexpected BDA result key shape: {key} "
            f"(got {len(segments)} segments, expected at least 9)"
        )

    app_no = segments[1]
    process = segments[2]
    encoded_path = segments[3]
    original_path = encoded_path.replace("__", "/")
    return app_no, process, original_path


def _map_bda_to_extraction_result(
    bda_result: Dict[str, Any],
    process_config: ProcessConfig,
) -> Dict[str, Any]:
    """Convert BDA's result.json into the ExtractionResult dict shape.

    Produces the same JSON that ExtractionPipeline's ``result.to_dict()``
    would produce. Downstream code (aggregation Lambda, SF push Lambda)
    reads this shape, so both paths must agree on it.

    Args:
        bda_result: Parsed BDA result.json.
        process_config: Parsed ProcessConfig from DynamoDB.

    Returns:
        A dict matching ExtractionResult.to_dict() output shape.
    """
    inference = bda_result.get("inference_result", {})
    explainability = bda_result.get("explainability_info", [])

    # Flatten explainability_info into {field_name: confidence_score}
    # for O(1) lookup while building the fields map
    confidence_by_field = _flatten_explainability(explainability)

    # Build fields dict matching ExtractedField.to_dict() shape
    fields: Dict[str, Dict[str, Any]] = {}
    confidences: List[float] = []

    for name, value in inference.items():
        # Only take top-level string values — BDA also emits nested
        # section objects (e.g. "YOUR DETAILS"/"YOUR PREMIUM") which are
        # groupings, not real fields
        if not isinstance(value, str):
            continue

        conf_score = confidence_by_field.get(name, 0.0)
        confidences.append(conf_score)

        fields[name] = {
            "name": name,
            "value": value.strip() if value else None,
            "confidence": _classify_confidence(conf_score),
            "confidence_score": conf_score,
            "page": None,
            "section": None,
            "reasoning": "",
            # BDA doesn't use Docling — these fields are intentionally
            # False to make the source obvious in DynamoDB
            "docling_match": False,
            "format_match": False,
            "validation_passed": conf_score >= LIKELY_THRESHOLD,
            "validation_notes": "",
        }

    overall_confidence = (
        sum(confidences) / len(confidences) if confidences else 0.0
    )
    recommendation = _recommendation_from_confidence(overall_confidence)

    return {
        "is_supported_document": True,
        "no_relevant_pages_reason": None,
        "fields": fields,
        "overall_confidence": overall_confidence,
        "recommendation": recommendation,
        "additional_response": {},
        "llm_calls": 0,           # BDA doesn't do separate LLM calls
        "processing_time_ms": 0,  # BDA result.json doesn't carry this
        "file_type": "pdf",
        "docling_processed": False,
    }


def _flatten_explainability(explainability: List[Any]) -> Dict[str, float]:
    """Extract per-field confidence from BDA's explainability_info.

    The exact shape varies by blueprint but the common pattern is a list
    of dicts keyed by field name, with a ``confidence`` float inside.
    We defensively handle nested section shapes too.

    Args:
        explainability: The explainability_info list from BDA output.

    Returns:
        Dict of {field_name: confidence_score}.
    """
    confidences: Dict[str, float] = {}
    if not isinstance(explainability, list):
        return confidences

    for entry in explainability:
        if not isinstance(entry, dict):
            continue
        for key, val in entry.items():
            if isinstance(val, dict) and "confidence" in val:
                confidences[key] = float(val["confidence"])
            elif isinstance(val, dict):
                # Nested section — dig one level deeper
                for inner_key, inner_val in val.items():
                    if (
                        isinstance(inner_val, dict)
                        and "confidence" in inner_val
                    ):
                        confidences[inner_key] = float(inner_val["confidence"])

    return confidences


def _classify_confidence(score: float) -> str:
    """Map a 0-1 float to the labels ExtractionPipeline uses."""
    if score >= CERTAIN_THRESHOLD:
        return "CERTAIN"
    if score >= LIKELY_THRESHOLD:
        return "LIKELY"
    if score > 0:
        return "UNCERTAIN"
    return "NOT_FOUND"


def _recommendation_from_confidence(score: float) -> str:
    """Map overall confidence to a recommendation string."""
    if score >= 0.95:
        return "auto_accept"
    if score >= 0.80:
        return "flag_for_review"
    return "manual_required"