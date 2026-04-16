"""
Standalone Salesforce Push Lambda Handler.

Lightweight Lambda function that:
1. Consumes messages from SQS FIFO queue
2. Reads extraction results from DynamoDB
3. Transforms data to Salesforce-compatible format
4. Pushes to Salesforce REST API
5. Updates DynamoDB with response

This Lambda is independent of the extraction Lambda and can be reused
by any process with appropriate configuration.

Message Format:
    {
        "path": "S3 object key",
        "process": "process identifier"
    }

Environment Variables:
    DYNAMODB_EXTRACTION_TABLE: DynamoDB table name (global for all processes)

Author: Reet Roy
Version: 2.0.0 (Standalone, reusable)
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("REGION", "us-west-2"))
secrets_manager = boto3.client("secretsmanager", region_name=os.environ.get("REGION", "us-west-2"))


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Entry point for Salesforce push Lambda.

    Args:
        event: SQS Lambda event with Records array.
        context: Lambda context.

    Returns:
        Dict with batchItemFailures for failed records.
    """
    logger.info(f"Received event with {len(event.get('Records', []))} records")

    batch_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            process_record(record)
        except Exception as e:
            logger.exception(f"Failed to process record {message_id}: {e}")
            batch_failures.append({"itemIdentifier": message_id})

    if batch_failures:
        logger.warning(f"Returning {len(batch_failures)} batch failures")

    return {"batchItemFailures": batch_failures}


def process_record(record: Dict[str, Any]) -> None:
    """Process a single SQS record.

    Args:
        record: Single SQS record from Lambda event.

    Raises:
        Exception: If processing fails (triggers SQS retry).
    """
    # Parse SQS message
    body = json.loads(record.get("body", "{}"))
    path = body.get("path", "")
    process = body.get("process", "")

    if not all([path, process]):
        raise ValueError("Missing required fields in SQS message")

    logger.info(f"Processing SF push: path={path}, process={process}")

    # Get extraction table from environment variable
    extraction_table = os.environ.get("DYNAMODB_EXTRACTION_TABLE")
    if not extraction_table:
        raise ValueError("DYNAMODB_EXTRACTION_TABLE environment variable not set")

    # Read file record from DynamoDB (using composite key)
    table = dynamodb.Table(extraction_table)
    response = table.get_item(Key={"path": path, "process": process})

    if "Item" not in response:
        raise ValueError(f"File record not found: {path}")

    file_record = response["Item"]

    # Validate state
    if file_record.get("state") != "Processed" and file_record.get("state") != "Failed":
        logger.warning(f"File not in Processed or Failed state: {path} (state={file_record.get('state')})")
        return

    # Parse extraction result
    extracted_json = file_record.get("extracted_json_string_with_confidence")
    try:
        extraction_result = json.loads(extracted_json)
    except Exception as e:
        logger.exception(f"Failed to parse extraction result: {e}")
        extraction_result = {}

    error_message = file_record.get("error_message")

    # Push to Salesforce
    sf_response = push_to_salesforce(
        app_no=file_record.get("app_no"),
        path=path,
        process=process,
        extraction_result=extraction_result,
        error_message=error_message,
        submission_timestamp=file_record.get("submission_timestamp", ""),
    )

    # Update DynamoDB with SF response
    update_file_record(
        table=table,
        path=path,
        process=process,
        sf_response=sf_response,
    )

    logger.info(f"SF push successful: {path}")


def push_to_salesforce(
    app_no: str,
    path: str,
    process: str,
    extraction_result: Dict[str, Any],
    error_message: Optional[str],
    submission_timestamp: str,
) -> Dict[str, Any]:
    """Push extraction result to Salesforce.

    Args:
        app_no: Application number.
        path: S3 object key.
        process: Process identifier.
        extraction_result: Extraction result dict.
        submission_timestamp: Submission timestamp.

    Returns:
        Salesforce API response dict.

    Raises:
        Exception: If SF push fails.
    """
    # Check if SF is enabled
    sf_enabled = os.environ.get("SF_ENABLED", "false").lower() == "true"
    if not sf_enabled:
        logger.info("Salesforce push disabled — skipping")
        return {"status": "skipped", "reason": "SF_ENABLED=false"}

    sf_endpoint = os.environ.get("SF_APEX_PATH", "")
    if not sf_endpoint:
        raise ValueError("SF_APEX_PATH not configured")

    # Get fields (already in list format from to_dict())
    fields_data = extraction_result.get("fields", {})

    # Ensure fields are in list format (handle both list and dict for backward compatibility)
    # fields_list = transform_fields_to_list(fields_data)

    # Build Salesforce payload
    file_data = {
        "process": process,
        "applicationNo": app_no,
        "filePath": path,
        "submissionTimestamp": submission_timestamp,
        "fields": fields_data,
        "error_message": error_message,
        "overallConfidence": extraction_result.get("overall_confidence", 0.0),
        "recommendation": extraction_result.get("recommendation", "manual_required"),
        "isSupportedDocument": extraction_result.get("is_supported_document", True),
        "additionalResponse": extraction_result.get("additional_response", {}),
        "processingMetadata": {
            "llmCalls": extraction_result.get("llm_calls", 0),
            "processingTimeMs": extraction_result.get("processing_time_ms", 0),
            "fileType": extraction_result.get("file_type", "pdf"),
            "doclingProcessed": extraction_result.get("docling_processed", False),
        },
    }

    payload = {
        "process": process,
        "data": json.dumps(file_data),
    }

    # Get SF token
    access_token = get_sf_token()

    # Call SF Apex REST API
    sf_instance_url = os.environ.get("SF_INSTANCE_URL", "")
    full_url = f"{sf_instance_url}{sf_endpoint}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    logger.info(f"Calling Salesforce API: {full_url}, payload: {payload}")

    response = requests.post(full_url, headers=headers, json=payload, timeout=30)

    if response.status_code not in [200, 201]:
        error_msg = f"Salesforce API returned {response.status_code}: {response.text}"
        logger.error(error_msg)
        return error_msg

    sf_response = response.json()
    logger.info(f"Salesforce push successful: {sf_response}")

    return sf_response


def transform_fields_to_list(fields_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform fields from dict format to list format.

    Args:
        fields_dict: Fields in dict format (old structure).

    Returns:
        Fields in list format (new structure).
    """
    fields_list = []

    for field_name, field_data in fields_dict.items():
        # Handle both old structure (with nested dict) and new structure (flat dict)
        if isinstance(field_data, dict):
            field_item = {
                "name": field_name,
                "value": field_data.get("value"),
                "confidence": field_data.get("confidence", "NOT_FOUND"),
                "confidenceScore": field_data.get("confidence_score", 0.0),
                "page": field_data.get("page"),
                "section": field_data.get("section"),
                "reasoning": field_data.get("reasoning", ""),
            }

            # Add optional validation fields if present
            if "docling_match" in field_data:
                field_item["doclingMatch"] = field_data["docling_match"]
            if "format_match" in field_data:
                field_item["formatMatch"] = field_data["format_match"]
            if "validation_passed" in field_data:
                field_item["validationPassed"] = field_data["validation_passed"]
            if "validation_notes" in field_data:
                field_item["validationNotes"] = field_data["validation_notes"]

            fields_list.append(field_item)

    return fields_list


def get_sf_token() -> str:
    """Get Salesforce OAuth token using token management.

    Returns:
        Access token.

    Raises:
        Exception: If token retrieval fails.
    """
    token_table_name = os.environ.get("SF_TOKEN_TABLE", "")
    if not token_table_name:
        raise ValueError("SF_TOKEN_TABLE not configured")

    token_table = dynamodb.Table(token_table_name)

    # Try to get cached token
    sf_username = os.environ.get("SF_USERNAME", "")
    response = token_table.get_item(Key={"username": sf_username})

    if "Item" in response:
        token_data = response["Item"]
        access_token = token_data.get("access_token")
        expires_at = token_data.get("expires_at", 0)

        # Check if token is still valid
        if access_token and datetime.utcnow().timestamp() < expires_at:
            logger.info("Using cached SF token")
            return access_token

    # Token expired or not found, fetch new token
    logger.info("Fetching new SF token")
    access_token = fetch_new_sf_token()

    # Cache the token
    token_table.put_item(
        Item={
            "username": sf_username,
            "access_token": access_token,
            "expires_at": int(datetime.utcnow().timestamp() + 3600),  # 1 hour
            "updated_at": datetime.utcnow().isoformat(),
        }
    )

    return access_token


def fetch_new_sf_token() -> str:
    """Fetch new Salesforce OAuth token.

    Returns:
        Access token.

    Raises:
        Exception: If token fetch fails.
    """
    # Get credentials from Secrets Manager
    secret_name = os.environ.get("SF_SECRET_NAME", "")
    if not secret_name:
        raise ValueError("SF_SECRET_NAME not configured")

    secret_response = secrets_manager.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_response["SecretString"])

    # Get SF credentials
    client_id = secret.get("client_id")
    client_secret = secret.get("client_secret")
    username = os.environ.get("SF_USERNAME", "")
    password = secret.get("password")

    # Prepare OAuth request
    sf_host = os.environ.get("SF_HOST", "")
    auth_path = os.environ.get("SF_AUTH_PATH", "/services/oauth2/token")
    auth_url = f"{sf_host}{auth_path}"

    data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
    }

    response = requests.post(auth_url, data=data, timeout=30)

    if response.status_code != 200:
        raise Exception(f"SF auth failed: {response.status_code} - {response.text}")

    token_response = response.json()
    return token_response["access_token"]


def update_file_record(
    table: Any,
    path: str,
    process: str,
    sf_response: Dict[str, Any],
) -> None:
    """Update file record with Salesforce response.

    Args:
        table: DynamoDB table resource.
        path: S3 object key (partition key).
        process: Process identifier (sort key).
        sf_response: Salesforce API response.
    """
    table.update_item(
        Key={"path": path, "process": process},
        UpdateExpression="SET sf_response = :sf_response, sf_push_timestamp = :timestamp",
        ExpressionAttributeValues={
            ":sf_response": json.dumps(sf_response),
            ":timestamp": datetime.utcnow().isoformat(),
        },
    )
