"""
Controller for Extraction Aggregation.

Port of ``UtilityAggregationBatch.cls`` from Apex to Python.

Processing flow:
    1. Parse SQS message (extract ``app_no``, ``process``)
    2. Load process config from DynamoDB
    3. Query all processed files for app_no + process (via app_no-index GSI)
    4. Determine latest submission window
    5. Flag files as latest submission
    6. Aggregate entities across files with confidence-based conflict resolution
    7. Resolve program from state + utility (if enabled)
    8. Validate mandatory fields
    9. Push aggregated result to Salesforce (inline, no output queue)

Tags:
    Application: Document-Extraction-Aggregation
    Environment: Dev

Author: Reet Roy
Version: 1.1.0

Modification History:
    2026-04-21 - CR-05: _determine_submission_window iterates ALL files.
                 CR-18: DynamoDB updates batched after loop.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

import aggregation_service

logger = logging.getLogger(__name__)


# =========================================================================
# Inner data classes for aggregation (mirrors Apex inner classes)
# =========================================================================


@dataclass
class ExtractionCandidate:
    """Candidate extraction value for aggregation sorting.

    Sorts by confidence DESC, then submission timestamp DESC.
    Mirrors the Apex ``ExtractionCandidate`` inner class.
    """

    value: Optional[str]
    confidence_score: float
    source_file: str
    submission_timestamp: Optional[str]

    def sort_key(self):
        """Return tuple for sorting: (-confidence, -timestamp)."""
        return (-self.confidence_score)


@dataclass
class ConflictValue:
    """Conflicting value from a different source file."""

    value: Optional[str]
    confidence: float
    source: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to dict."""
        return {
            "value": self.value,
            "confidence": self.confidence,
            "source": self.source,
        }


@dataclass
class ConflictInfo:
    """Conflict information for a field."""

    top_value: Optional[str] = None
    top_confidence: float = 0.0
    top_source: str = ""
    conflicting_values: List[ConflictValue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to dict."""
        return {
            "topValue": self.top_value,
            "topConfidence": self.top_confidence,
            "topSource": self.top_source,
            "conflictingValues": [cv.to_dict() for cv in self.conflicting_values],
        }


@dataclass
class AggregationResult:
    """Aggregation result container.

    Mirrors the Apex ``AggregationResult`` inner class.
    """

    aggregated_fields: Dict[str, Optional[str]] = field(default_factory=dict)
    overall_confidence: float = 0.0
    recommendation: str = "manual_required"
    fields_requiring_review: List[str] = field(default_factory=list)
    conflict_details: Dict[str, ConflictInfo] = field(default_factory=dict)
    program: Optional[str] = None
    missing_mandatory_fields: List[str] = field(default_factory=list)


# =========================================================================
# Aggregation Config (parsed from DynamoDB process config)
# =========================================================================


@dataclass
class AggregationConfig:
    """Parsed aggregation configuration from DynamoDB process config."""

    object_name: str = ""
    enabled: bool = False
    conflict_threshold: float = 0.05
    submission_window_minutes: int = 30
    high_confidence_threshold: float = 0.90
    medium_confidence_threshold: float = 0.80
    field_mappings: Dict[str, str] = field(default_factory=dict)
    program_lookup_enabled: bool = False
    program_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AggregationConfig":
        """Build from a DynamoDB aggregation_config map."""
        if not data:
            return cls()

        return cls(
            enabled=data.get("enabled", False),
            conflict_threshold=float(data.get("conflict_threshold", 0.05)),
            submission_window_minutes=int(data.get("submission_window_minutes", 30)),
            high_confidence_threshold=float(
                data.get("high_confidence_threshold", 0.90)
            ),
            medium_confidence_threshold=float(
                data.get("medium_confidence_threshold", 0.80)
            ),
            field_mappings=data.get("field_mappings", {}),
            program_lookup_enabled=data.get("program_lookup_enabled", False),
            program_configs=data.get("program_configs", {}),
            object_name=data.get("object_name", ""),
        )


# =========================================================================
# Controller
# =========================================================================


class AggregationController:
    """Controller for processing SQS aggregation events."""

    def __init__(self):
        """Initialise the controller."""
        pass

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
                logger.exception("Failed to process aggregation record %s", message_id)
                batch_failures.append({"itemIdentifier": message_id})

        if batch_failures:
            logger.warning("Returning %d batch failures", len(batch_failures))

        return {"batchItemFailures": batch_failures}

    # ------------------------------------------------------------------
    # Per-record processing
    # ------------------------------------------------------------------

    def _process_record(self, record: Dict[str, Any]) -> None:
        """Process a single SQS aggregation record.

        Args:
            record: Raw SQS record dict.
        """
        # 1 — Parse message
        body = json.loads(record.get("body", "{}"))
        app_no = body.get("app_no", "")
        process = body.get("process", "")

        if not app_no or not process:
            raise ValueError(
                "SQS aggregation message missing required fields: "
                f"app_no='{app_no}', process='{process}'"
            )

        logger.info(
            "Processing aggregation: app_no=%s, process=%s",
            app_no,
            process,
        )

        # 2 — Load process config
        config_item = aggregation_service.get_process_config(process)
        if not config_item:
            raise ValueError(
                f"No configuration found for process '{process}' "
                "in the config DynamoDB table"
            )

        agg_config = AggregationConfig.from_dict(
            config_item.get("aggregation_config", {})
        )

        if not agg_config.enabled:
            logger.info(
                "Aggregation disabled for process '%s' — skipping",
                process,
            )
            return

        # 3 — Query all processed files for this app_no + process
        all_files = aggregation_service.query_processed_files(app_no, process)

        if not all_files:
            logger.info(
                "No processed files found for app_no=%s, process=%s",
                app_no,
                process,
            )
            return
        logger.info(f"All files : {all_files}")
        # 4 — Determine latest submission window
        latest_timestamp, cutoff_timestamp = self._determine_submission_window(
            all_files, agg_config.submission_window_minutes
        )

        # 5 — Flag files as latest submission
        files_in_window = self._flag_latest_submissions(
            all_files, cutoff_timestamp, process
        )

        if not files_in_window:
            logger.warning(
                "No files within submission window for app_no=%s",
                app_no,
            )
            return

        # 6 — Extract and group entities from files in window
        all_extractions = self._extract_entities_from_files(files_in_window)

        if not all_extractions:
            logger.info("No extractable entities found for app_no=%s", app_no)
            return

        # 7 — Aggregate with confidence-based conflict resolution
        agg_result = self._aggregate_extractions(all_extractions, agg_config)

        # 8 — Program determination (if enabled)
        if agg_config.program_lookup_enabled:
            self._resolve_program(agg_result, agg_config)

        # 9 — Build and push aggregated result to Salesforce
        self._push_aggregated_to_salesforce(
            app_no=app_no,
            process=process,
            agg_result=agg_result,
            agg_config=agg_config,
            latest_timestamp=latest_timestamp,
            files_in_window=files_in_window,
        )

        logger.info(
            "Aggregation complete: app_no=%s, process=%s, "
            "overall_confidence=%.2f, recommendation=%s, "
            "conflicts=%d, files_aggregated=%d",
            app_no,
            process,
            agg_result.overall_confidence,
            agg_result.recommendation,
            len(agg_result.fields_requiring_review),
            len(files_in_window),
        )

    # ------------------------------------------------------------------
    # Submission window
    # ------------------------------------------------------------------

    def _determine_submission_window(
        self,
        files: List[Dict[str, Any]],
        window_minutes: int,
    ) -> tuple:
        """Determine the latest submission window.

        Finds the latest ``submission_timestamp`` across all files and
        calculates the cutoff time.

        Args:
            files: List of DynamoDB file items.
            window_minutes: Time window in minutes.

        Returns:
            Tuple of (latest_timestamp_str, cutoff_timestamp_str).
        """
        latest_ts = None
            
        try:
            ts_str = files[0].get("submission_timestamp", "")
            ts = datetime.fromisoformat(
                ts_str.replace("Z", "+00:00")
            )
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
        except (ValueError, TypeError):
            latest_ts = None

        if latest_ts is None:
            logger.warning(
                "No valid submission_timestamp found in %d files; "
                "using current time as latest.",
                len(files),
            )
            latest_ts = datetime.utcnow()

        cutoff = latest_ts - timedelta(minutes=window_minutes)
        latest_str = latest_ts.isoformat()
        cutoff_str = cutoff.isoformat()

        logger.info(
            "Submission window: latest=%s, cutoff=%s (window=%d min)",
            latest_str,
            cutoff_str,
            window_minutes,
        )
        return latest_str, cutoff_str

    # ------------------------------------------------------------------
    # Latest submission flagging
    # ------------------------------------------------------------------

    def _flag_latest_submissions(
        self,
        files: List[Dict[str, Any]],
        cutoff_timestamp: str,
        process: str,
    ) -> List[Dict[str, Any]]:
        """Flag files as latest submission based on the time window.

        Updates DynamoDB with ``latest_submission = true/false``.

        Args:
            files: All file records.
            cutoff_timestamp: ISO timestamp cutoff.
            process: Process identifier.

        Returns:
            List of files within the submission window.
        """
        files_in_window = []

        for f in files:
            ts_str = f.get("submission_timestamp", "")
            is_latest = False

            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    cutoff = datetime.fromisoformat(
                        cutoff_timestamp.replace("Z", "+00:00")
                    )
                    is_latest = ts >= cutoff
                except (ValueError, TypeError):
                    pass

            # Update DynamoDB
            # to be done out of loop to optimize the number of calls to DynamoDB
            aggregation_service.update_latest_submission_flag(
                path=f.get("path", ""),
                process=process,
                is_latest=is_latest,
            )

            if is_latest:
                files_in_window.append(f)

        logger.info(
            "Flagged %d/%d files as latest submission",
            len(files_in_window),
            len(files),
        )
        return files_in_window

    # ------------------------------------------------------------------
    # Entity extraction from files
    # ------------------------------------------------------------------

    def _extract_entities_from_files(
        self,
        files: List[Dict[str, Any]],
    ) -> Dict[str, List[ExtractionCandidate]]:
        """Extract and group entities from file extraction results.

        Parses each file's ``extracted_json_string_with_confidence``
        and groups values by entity name.

        Args:
            files: List of DynamoDB file items with extraction results.

        Returns:
            Map of entity name → list of extraction candidates.
        """
        grouped: Dict[str, List[ExtractionCandidate]] = {}

        for f in files:
            extracted_json = f.get("extracted_json_string_with_confidence", "")
            if not extracted_json:
                continue

            try:
                result = json.loads(extracted_json)
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    "Invalid extraction JSON for path=%s",
                    f.get("path", ""),
                )
                continue

            # Skip unsupported documents
            if not result.get("is_supported_document", True):
                continue

            fields = result.get("fields", {})

            for entity_name, field_data in fields.items():
                if not isinstance(field_data, dict):
                    continue

                value = field_data.get("value")
                confidence = float(field_data.get("confidence_score", 0.0))

                candidate = ExtractionCandidate(
                    value=value,
                    confidence_score=confidence,
                    source_file=f.get("path", ""),
                    submission_timestamp=f.get("submission_timestamp", ""),
                )

                if entity_name not in grouped:
                    grouped[entity_name] = []
                grouped[entity_name].append(candidate)

        return grouped

    # ------------------------------------------------------------------
    # Aggregation with conflict resolution
    # ------------------------------------------------------------------

    def _aggregate_extractions(
        self,
        all_extractions: Dict[str, List[ExtractionCandidate]],
        agg_config: AggregationConfig,
    ) -> AggregationResult:
        """Aggregate extractions using confidence-based conflict resolution.

        Port of ``UtilityAggregationBatch.aggregateExtractions()``.

        Sort by confidence DESC, then submission timestamp DESC.
        Flag conflicts within configurable threshold for manual review.

        Args:
            all_extractions: Map of entity → candidates.
            agg_config: Aggregation configuration.

        Returns:
            AggregationResult with aggregated fields and conflicts.
        """
        result = AggregationResult()
        conflict_threshold = agg_config.conflict_threshold

        total_confidence = 0.0
        field_count = 0

        for entity, candidates in all_extractions.items():
            if not candidates:
                continue

            if entity not in agg_config.field_mappings:
                continue

            # Sort by confidence DESC, then submission timestamp DESC
            candidates.sort(key=lambda c: c.sort_key())

            top_candidate = candidates[0]
            total_confidence += top_candidate.confidence_score
            field_count += 1

            # Check for conflicts
            if len(candidates) > 1:
                second_candidate = candidates[1]
                confidence_diff = (
                    top_candidate.confidence_score - second_candidate.confidence_score
                )

                # Flag for review if within threshold
                if confidence_diff <= conflict_threshold:
                    result.fields_requiring_review.append(entity)

                    # Store conflict details
                    conflict = ConflictInfo(
                        top_value=top_candidate.value,
                        top_confidence=top_candidate.confidence_score,
                        top_source=top_candidate.source_file,
                    )

                    # Include up to 3 conflicting values
                    for i in range(1, len(candidates)):
                        conflict.conflicting_values.append(
                            ConflictValue(
                                value=candidates[i].value,
                                confidence=candidates[i].confidence_score,
                                source=candidates[i].source_file,
                            )
                        )

                    result.conflict_details[entity] = conflict

            # Use top value (highest confidence, or latest timestamp
            # if tied)
            result.aggregated_fields[entity] = top_candidate.value

        # Calculate overall confidence and recommendation
        result.overall_confidence = (
            total_confidence / field_count if field_count > 0 else 0
        )
        result.recommendation = self._determine_recommendation(
            result.overall_confidence,
            len(result.fields_requiring_review),
            agg_config,
        )

        return result

    # ------------------------------------------------------------------
    # Recommendation determination
    # ------------------------------------------------------------------

    @staticmethod
    def _determine_recommendation(
        avg_confidence: float,
        conflict_count: int,
        agg_config: AggregationConfig,
    ) -> str:
        """Determine recommendation based on confidence and conflicts.

        Port of ``UtilityAggregationBatch.determineRecommendation()``.

        Args:
            avg_confidence: Average confidence across fields.
            conflict_count: Number of conflicting fields.
            agg_config: Aggregation configuration.

        Returns:
            Recommendation string.
        """
        if conflict_count > 0:
            return "manual_review_required"
        elif avg_confidence >= agg_config.high_confidence_threshold:
            return "auto_accept"
        elif avg_confidence >= agg_config.medium_confidence_threshold:
            return "review_recommended"
        else:
            return "manual_required"

    # ------------------------------------------------------------------
    # Program resolution
    # ------------------------------------------------------------------

    def _resolve_program(
        self,
        agg_result: AggregationResult,
        agg_config: AggregationConfig,
    ) -> None:
        """Resolve program from aggregated state + utility provider.

        Port of ``UtilityAggregationBatch.getStateProgramConfig()``
        and ``validateMandatoryFields()``.

        Args:
            agg_result: Aggregation result (modified in place).
            agg_config: Aggregation configuration.
        """
        state = agg_result.aggregated_fields.get("state", "")
        utility = agg_result.aggregated_fields.get("utility_provider", "")

        if not state:
            logger.info("No state extracted — skipping program lookup")
            return

        state_upper = state.upper().strip()
        utility_upper = utility.upper().strip() if utility else ""

        # Search program configs
        for _key, prog_cfg in agg_config.program_configs.items():
            cfg_states = [s.upper().strip() for s in prog_cfg.get("states", [])]
            cfg_utilities = [u.upper().strip() for u in prog_cfg.get("utilities", [])]

            if state_upper not in cfg_states:
                continue

            # Check utility match
            if "ANY" in cfg_utilities:
                matched = True
            elif utility_upper and utility_upper in cfg_utilities:
                matched = True
            else:
                matched = False

            if matched:
                agg_result.program = prog_cfg.get("program", "")

                # Validate mandatory fields
                mandatory = prog_cfg.get("mandatory_fields", [])
                missing = []
                for mf in mandatory:
                    val = agg_result.aggregated_fields.get(mf, "")
                    if not val or (isinstance(val, str) and not val.strip()):
                        missing.append(mf)

                if missing:
                    agg_result.missing_mandatory_fields = missing
                    agg_result.recommendation = "manual_review_required"
                    logger.info(
                        "Missing mandatory fields for program '%s': %s",
                        agg_result.program,
                        missing,
                    )

                logger.info(
                    "Resolved program: %s for state=%s, utility=%s",
                    agg_result.program,
                    state,
                    utility,
                )
                return

        logger.info(
            "No matching program config for state=%s, utility=%s",
            state,
            utility,
        )

    # ------------------------------------------------------------------
    # Salesforce push (inline — no output queue)
    # ------------------------------------------------------------------

    def _push_aggregated_to_salesforce(
        self,
        app_no: str,
        process: str,
        agg_result: AggregationResult,
        agg_config: AggregationConfig,
        latest_timestamp: str,
        files_in_window: List[Dict[str, Any]],
    ) -> None:
        """Push aggregated result to Salesforce.

        Uses the same Apex endpoint as per-file pushes, with
        'process' field having '_aggregated' appended.

        The ``data`` payload uses ``field_api__c: value`` format for
        the aggregated fields section.

        Args:
            app_no: Application number.
            process: Process identifier.
            agg_result: Aggregation result.
            agg_config: Aggregation configuration.
            latest_timestamp: Latest submission timestamp.
            files_in_window: Files used in aggregation.
        """
        sf_enabled = os.environ.get("SF_ENABLED", "false").lower() == "true"
        if not sf_enabled:
            logger.info("Salesforce push disabled — skipping")
            return

        sf_endpoint = os.environ.get("SF_APEX_PATH", "")
        if not sf_endpoint:
            logger.warning("SF endpoint not configured — skipping aggregation push")
            return

        # Build aggregated fields using field_api__c : value format
        aggregated_sf_fields = {}
        for entity, value in agg_result.aggregated_fields.items():
            sf_field = agg_config.field_mappings.get(entity, entity)
            aggregated_sf_fields[sf_field] = value

        # Build file metadata
        files_used = []
        for f in files_in_window:
            files_used.append(
                {
                    "path": f.get("path", ""),
                    "submissionTimestamp": f.get("submission_timestamp", ""),
                    "isLatestSubmission": True,
                }
            )

        # Build conflict details dict
        conflict_details_dict = {
            entity: info.to_dict()
            for entity, info in agg_result.conflict_details.items()
        }

        # Build the data payload
        data_payload = {
            "applicationNo": app_no,
            "aggregatedFields": aggregated_sf_fields,
            "object_name": agg_config.object_name,
            "overallConfidence": agg_result.overall_confidence,
            "recommendation": agg_result.recommendation,
            "program": agg_result.program,
            "missingMandatoryFields": (agg_result.missing_mandatory_fields),
            "fieldsRequiringReview": (agg_result.fields_requiring_review),
            "conflictDetails": conflict_details_dict,
            "latestSubmissionTimestamp": latest_timestamp,
            "totalFilesAggregated": len(files_in_window),
            "filesUsed": files_used,
        }

        # Use same endpoint with process_aggregated
        aggregated_process = f"{process}_aggregated"
        payload = {
            "process": aggregated_process,
            "data": json.dumps(data_payload),
        }

        # Get SF token and push
        try:
            access_token = self._get_sf_token()
            if not access_token:
                logger.warning("Failed to get SF token — skipping aggregation push")
                return

            sf_instance_url = os.environ.get("SF_INSTANCE_URL", "")
            full_url = f"{sf_instance_url}{sf_endpoint}"

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            logger.info(
                "Calling Salesforce API for aggregation: %s, %s", full_url, payload
            )

            response = requests.post(
                full_url,
                headers=headers,
                json=payload,
                timeout=30,
            )

            if response.status_code not in [200, 201]:
                logger.error(
                    "Salesforce API returned %d: %s",
                    response.status_code,
                    response.text,
                )
                raise ValueError(f"Salesforce API error: {response.status_code}")

            sf_response = response.json()
            logger.info(
                "Salesforce aggregation push successful: %s",
                sf_response,
            )

        except Exception:
            logger.exception("Failed to push aggregated result to Salesforce")
            raise

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_sf_token(self) -> Optional[str]:
        """Get Salesforce OAuth token.

        Uses the same token management pattern as the per-file SF push.

        Returns:
            Access token or None.
        """
        import boto3

        region = os.environ.get("REGION", "us-west-2")
        token_table_name = os.environ.get("SF_TOKEN_TABLE", "")
        sf_username = os.environ.get("SF_USERNAME", "")

        if not token_table_name or not sf_username:
            logger.error("SF_TOKEN_TABLE or SF_USERNAME not configured")
            return None

        dynamodb = boto3.resource("dynamodb", region_name=region)
        token_table = dynamodb.Table(token_table_name)

        # Try cached token
        try:
            response = token_table.get_item(Key={"username": sf_username})
            if "Item" in response:
                token_data = response["Item"]
                token = token_data.get("token", "")
                issued_at = token_data.get("issued_at", "")

                if token and issued_at:
                    diff_time = int(os.environ.get("SF_TOKEN_DIFF_TIME", "300"))
                    issued_at_sec = int(issued_at) / 1000
                    import time

                    current_ts = int(time.time())

                    if current_ts - issued_at_sec <= diff_time:
                        return token

            # Token expired or not found — fetch new
            logger.info("Fetching new SF token for aggregation")
            return self._fetch_new_sf_token(token_table, sf_username)

        except Exception:
            logger.exception("Failed to get SF token")
            return None

    def _fetch_new_sf_token(self, token_table, sf_username: str) -> Optional[str]:
        """Fetch a new Salesforce token and cache it.

        Args:
            token_table: DynamoDB table resource for token storage.
            sf_username: Salesforce username.

        Returns:
            Access token or None.
        """
        import boto3

        region = os.environ.get("REGION", "us-west-2")
        secret_name = os.environ.get("SF_SECRET_NAME", "")

        if not secret_name:
            logger.error("SF_SECRET_NAME not configured")
            return None

        # Get credentials from Secrets Manager
        session = boto3.Session()
        sm_client = session.client("secretsmanager", region_name=region)
        secret_response = sm_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(secret_response["SecretString"])

        # Build auth request
        sf_host = os.environ.get("SF_HOST", "")
        auth_path = os.environ.get("SF_AUTH_PATH", "/services/oauth2/token")
        auth_url = f"{sf_host}{auth_path}"

        response = requests.post(auth_url, data=secret, timeout=30)

        if response.status_code != 200:
            logger.error(
                "SF auth failed: %d - %s",
                response.status_code,
                response.text,
            )
            return None

        token_response = response.json()
        access_token = token_response.get("access_token", "")
        issued_at = token_response.get("issued_at", "")

        # Cache the token
        if access_token:
            token_table.put_item(
                Item={
                    "username": sf_username,
                    "token": access_token,
                    "issued_at": issued_at,
                }
            )

        return access_token
