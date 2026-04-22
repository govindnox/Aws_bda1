"""
BDA extraction pipeline — async path.

Unlike ExtractionPipeline which runs the full extraction synchronously,
this pipeline only kicks off a Bedrock Data Automation job and returns
immediately. The actual result is written to S3 by BDA 40-60 seconds
later, where a separate S3-triggered Lambda (BDA Result Handler) picks
it up and finishes the downstream work.

Flow:
    1. Encode app_no + path into an output prefix so the result-handler
       Lambda can recover them from the S3 key
    2. Call bda_service.invoke_async
    3. Return a BDAPendingResult sentinel so the controller knows to
       skip the sync downstream steps (save to DDB, SF push, aggregation)

Author: Govind Pandey
Version: 0.1.0 
"""

import logging
from dataclasses import dataclass

from models.data_models import ProcessConfig
from services import bda_service
from config import config

logger = logging.getLogger(__name__)


# Signal object returned by BDAPipeline.run() to tell the controller
# that extraction is in-flight and the sync flow must not continue.
@dataclass
class BDAPendingResult:
    """Marker returned when a BDA job has been kicked off but not yet finished."""

    invocation_arn: str
    output_prefix: str


class BDAPipeline:
    """Kicks off a BDA job for one document, does not wait for it."""

    def __init__(self, process_config: ProcessConfig):
        """Initialise with a process configuration.

        Args:
            process_config: Per-process config from the DynamoDB config table.
                Must carry the BDA blueprint and profile ARNs.
        """
        self._config = process_config

    # ------------------------------------------------------------------
    # Public API — same signature as ExtractionPipeline.run()
    # ------------------------------------------------------------------

    def run(
        self,
        file_bytes: bytes,
        path: str,
        app_no: str,
    ) -> BDAPendingResult:
        """Kick off an async BDA job and return a pending sentinel.

        Note: file_bytes is accepted to match the ExtractionPipeline
        signature but is unused here — BDA reads the file directly from
        S3, so we only need the path.

        Args:
            file_bytes: Raw file bytes (unused for BDA).
            path: S3 object key of the input document.
            app_no: Application number.

        Returns:
            BDAPendingResult carrying the invocation ARN and output prefix.
        """
        del file_bytes  # explicit: we don't need these, BDA reads from S3

        # Build the output prefix. This gets encoded with app_no + path
        # so Lambda B can recover them from the S3 key after BDA writes
        # its result. BDA will append /{job_uuid}/0/custom_output/0/result.json
        # to whatever prefix we give it.
        encoded_path = _encode_path_for_s3_key(path)
        output_prefix = f"bda-output/{app_no}/{self._config.process}/{encoded_path}"

        # Pull BDA config from the process config (DynamoDB) with env-var
        # fallback. Blueprint ARN comes from the process config; the
        # data automation profile ARN is account-wide, so it lives in env.
        blueprint_arn = self._config.bda_blueprint_arn
        profile_arn = config.bda.profile_arn

        if not blueprint_arn:
            raise ValueError(
                f"Process '{self._config.process}' is configured for BDA "
                "but has no bda_blueprint_arn set in the config table"
            )
        if not profile_arn:
            raise ValueError(
                "BDA profile ARN not configured — set BDA_PROFILE_ARN env var"
            )

        invocation_arn = bda_service.invoke_async(
            input_bucket=config.aws.s3_bucket,
            input_key=path,
            output_bucket=config.aws.s3_bucket,
            output_prefix=output_prefix,
            blueprint_arn=blueprint_arn,
            profile_arn=profile_arn,
            region=config.aws.region,
        )

        logger.info(
            "BDA job kicked off: app_no=%s, path=%s, invocation=%s",
            app_no,
            path,
            invocation_arn,
        )

        return BDAPendingResult(
            invocation_arn=invocation_arn,
            output_prefix=output_prefix,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _encode_path_for_s3_key(path: str) -> str:
    """Encode the original S3 path so it can sit as one segment in the output key.

    The output key becomes:
        bda-output/{app_no}/{encoded_path}/{job_uuid}/0/custom_output/0/result.json

    We swap '/' for '__' so the original path occupies exactly one segment.
    Lambda B reverses this when recovering the original path.
    """
    return path.replace("/", "__")