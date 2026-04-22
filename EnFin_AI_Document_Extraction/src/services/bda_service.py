"""
BDA service wrapper.

Thin wrapper around Bedrock Data Automation boto3 calls. Only two things
happen here: kick off an async BDA job, and read the result JSON back
from S3 once BDA has finished writing it.

Author: Govind Pandey
Version: 0.1.0 
"""

import json
import logging
from typing import Dict, Any, Optional

import boto3

logger = logging.getLogger(__name__)


# Singletons so we reuse boto3 clients across Lambda warm invocations
_bda_runtime = None


def _get_bda_runtime(region: str):
    """Get (and cache) the Bedrock Data Automation runtime client."""
    global _bda_runtime
    if _bda_runtime is None:
        _bda_runtime = boto3.client("bedrock-data-automation-runtime", region_name=region)
    return _bda_runtime




# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def invoke_async(
    input_bucket: str,
    input_key: str,
    output_bucket: str,
    output_prefix: str,
    blueprint_arn: str,
    profile_arn: str,
    region: str = "us-west-2",
) -> str:
    """Kick off a BDA job for a single document.

    Does not wait for completion. BDA runs in the background and writes
    its result to S3 under ``output_prefix`` when done.

    Args:
        input_bucket: S3 bucket holding the input PDF.
        input_key: S3 key of the input PDF.
        output_bucket: S3 bucket where BDA should write results.
        output_prefix: S3 key prefix under which BDA nests its output.
            BDA appends ``{job_uuid}/0/custom_output/0/result.json``
            to this prefix.
        blueprint_arn: ARN of the BDA blueprint to run.
        profile_arn: ARN of the BDA data automation profile.
        region: AWS region (default us-west-2).

    Returns:
        The invocation ARN. Useful for logging and status lookups.
    """
    client = _get_bda_runtime(region)

    input_uri = f"s3://{input_bucket}/{input_key}"
    output_uri = f"s3://{output_bucket}/{output_prefix}"

    logger.info("Invoking BDA: input=%s, output_prefix=%s", input_uri, output_uri)

    response = client.invoke_data_automation_async(
        inputConfiguration={"s3Uri": input_uri},
        outputConfiguration={"s3Uri": output_uri},
        dataAutomationProfileArn=profile_arn,
        blueprints=[{"blueprintArn": blueprint_arn}],
    )

    invocation_arn = response["invocationArn"]
    logger.info("BDA job started: %s", invocation_arn)
    return invocation_arn


def read_result(output_prefix: str) -> Optional[Dict[str, Any]]:
    """Find and read the BDA result JSON from S3.

    BDA nests its real output under a job UUID like:
        {output_prefix}/{job_uuid}/0/custom_output/0/result.json

    We don't know the job UUID ahead of time, so we list the prefix
    and pick the first key that matches BDA's custom_output path shape.

    Args:
        output_prefix: The prefix we passed to invoke_async.

    Returns:
        Parsed result.json dict, or None if BDA hasn't finished writing yet.
    """
    from services import s3_service

    keys = s3_service.list_keys(output_prefix)
    for key in keys:
        if "custom_output" in key and key.endswith("result.json"):
            return s3_service.read_json(key)

    logger.warning("No result.json found yet under prefix %s", output_prefix)
    return None