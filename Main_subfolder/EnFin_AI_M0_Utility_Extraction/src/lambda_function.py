"""
AWS Lambda handler for utility bill extraction.

This Lambda function is triggered by SQS messages containing paths to utility bills (PDF or DOCX).
It extracts meter IDs, account numbers, and other required fields using Docling text extraction
and a single LLM call via Bedrock Converse API, with rule-based confidence scoring.

Payload structure:
{
    "filePath": "s3://bucket/path/to/file.pdf",
    "app_no": "APP-123456",
    "process": "m0_utility_bill"
}
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from config.settings import settings
from models.extraction_models import (
    SQSMessage,
    ExtractionOutput,
    ExtractionStatus,
    RecommendationType,
    UtilityClassification,
    ProcessingMetadata,
    ExtractedField,
    FieldScore
)
from services.s3_service import S3Service
from services.dynamodb_service import DynamoDBService
from services.salesforce_service import SalesforceService
from extractors.extraction_pipeline import ExtractionPipeline

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for processing utility bill extraction requests.

    Args:
        event: Lambda event containing SQS records
        context: Lambda context

    Returns:
        Processing result summary
    """
    logger.info("Lambda invoked with event: %s", json.dumps(event)[:500])

    # Validate settings
    validation = settings.validate()
    if not validation["valid"]:
        logger.error("Invalid settings: %s", validation["issues"])
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Configuration error",
                "issues": validation["issues"]
            })
        }

    # Log warnings
    for warning in validation.get("warnings", []):
        logger.warning("Configuration warning: %s", warning)

    # Initialize services
    s3_service = S3Service()
    dynamodb_service = DynamoDBService()

    # Only initialize Salesforce service if enabled
    salesforce_service = None
    if settings.SF_ENABLED:
        salesforce_service = SalesforceService()
        logger.info("Salesforce integration enabled")
    else:
        logger.info("Salesforce integration disabled (SF_ENABLED=false)")

    # Process SQS records
    records = event.get("Records", [])
    results = []
    failures = []

    for record in records:
        try:
            result = process_record(
                record,
                s3_service,
                dynamodb_service,
                salesforce_service
            )
            results.append(result)

        except Exception as e:
            logger.exception("Failed to process record: %s", record.get("messageId"))
            failures.append({
                "messageId": record.get("messageId"),
                "error": str(e)
            })

    # Return batch item failures for partial batch response
    response = {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(results),
            "failed": len(failures),
            "results": [r.get("extraction_id") for r in results]
        })
    }

    if failures:
        response["batchItemFailures"] = [
            {"itemIdentifier": f["messageId"]}
            for f in failures
        ]

    return response


def process_record(
    record: Dict[str, Any],
    s3_service: S3Service,
    dynamodb_service: DynamoDBService,
    salesforce_service: Optional[SalesforceService]
) -> Dict[str, Any]:
    """
    Process a single SQS record.

    Args:
        record: SQS record
        s3_service: S3 service instance
        dynamodb_service: DynamoDB service instance
        salesforce_service: Salesforce service instance (None if disabled)

    Returns:
        Processing result
    """
    # Parse SQS message
    message = SQSMessage.from_sqs_record(record)

    if not message.file_path:
        raise ValueError("Message missing required 'filePath' field")

    logger.info(
        "Processing document: %s (app_no: %s, process: %s)",
        message.file_path,
        message.app_no,
        message.process
    )

    # Download document from S3
    file_bytes = s3_service.download_document(message.file_path)
    logger.info("Downloaded document: %d bytes", len(file_bytes))

    # Create extraction pipeline with process-specific configuration
    extraction_pipeline = ExtractionPipeline(process=message.process)

    # Run extraction pipeline
    extraction_result = extraction_pipeline.extract(
        file_bytes=file_bytes,
        source_file=message.file_path,
        app_no=message.app_no,
        process=message.process
    )

    # Convert to output model
    output = convert_to_output(extraction_result, message)

    # Store result in S3 for audit
    result_key = s3_service.upload_extraction_result(
        extraction_id=extraction_result.extraction_id,
        result=output.to_dict(),
        source_file=message.file_path
    )
    logger.info("Stored extraction result in S3: %s", result_key)

    # Store result in DynamoDB
    dynamodb_service.store_extraction_result(
        extraction_id=extraction_result.extraction_id,
        project_id=message.app_no,
        source_file=message.file_path,
        extraction_result=output.to_dict()
    )
    logger.info("Stored extraction result in DynamoDB: %s", extraction_result.extraction_id)

    # Push to Salesforce (single POST call)
    sf_result = None
    if salesforce_service and settings.SF_ENABLED and message.app_no:
        sf_result = _update_salesforce(
            salesforce_service=salesforce_service,
            dynamodb_service=dynamodb_service,
            extraction_result=extraction_result,
            app_no=message.app_no
        )
    else:
        logger.info("Skipping Salesforce update (SF_ENABLED=false or no app_no)")

    return {
        "extraction_id": extraction_result.extraction_id,
        "source_file": message.file_path,
        "app_no": message.app_no,
        "process": message.process,
        "overall_confidence": extraction_result.overall_confidence,
        "recommendation": extraction_result.recommendation,
        "fields_extracted": list(extraction_result.fields.keys()),
        "salesforce_success": sf_result.get("success") if sf_result else None,
        "no_relevant_pages": extraction_result.no_relevant_pages,
        "file_type": extraction_result.file_type
    }


def _update_salesforce(
    salesforce_service: SalesforceService,
    dynamodb_service: DynamoDBService,
    extraction_result,
    app_no: str
) -> Dict[str, Any]:
    """
    Post extraction result to Salesforce via Apex REST.

    Single POST call regardless of recommendation (auto_accept/flag/manual).

    Args:
        salesforce_service: Salesforce service instance
        dynamodb_service: DynamoDB service instance
        extraction_result: Raw extraction result
        app_no: Application number

    Returns:
        Salesforce response dict
    """
    try:
        sf_result = salesforce_service.post_extraction_result(
            app_no=app_no,
            extraction_result=extraction_result
        )

        # Update DynamoDB with Salesforce sync status
        dynamodb_service.update_salesforce_status(
            extraction_id=extraction_result.extraction_id,
            salesforce_response=sf_result,
            success=sf_result.get("success", False)
        )

        return sf_result

    except Exception as e:
        logger.error("Failed to update Salesforce: %s", str(e))
        dynamodb_service.update_salesforce_status(
            extraction_id=extraction_result.extraction_id,
            salesforce_response={"error": str(e)},
            success=False,
            error_message=str(e)
        )
        return {"success": False, "error": str(e)}


def convert_to_output(
    extraction_result,
    message: SQSMessage
) -> ExtractionOutput:
    """
    Convert pipeline result to structured output model.

    Args:
        extraction_result: Result from extraction pipeline
        message: Original SQS message

    Returns:
        ExtractionOutput model
    """
    # Build classification
    classification = UtilityClassification(
        provider=extraction_result.utility_provider,
        state=extraction_result.state,
        program=extraction_result.program,
        confidence=extraction_result.classification_confidence
    )

    # Build fields
    fields = {}
    for field_name, field_data in extraction_result.fields.items():
        score_breakdown = FieldScore(
            docling_match=field_data.get("docling_match", False),
            format_match=field_data.get("format_match", False),
            final_confidence=field_data.get("confidence_score", 0)
        )

        extracted_field = ExtractedField(
            field_name=field_name,
            value=field_data.get("value"),
            confidence_score=field_data.get("confidence_score", 0),
            validation_passed=field_data.get("validation_passed", False),
            validation_notes=field_data.get("validation_notes", ""),
            score_breakdown=score_breakdown,
            page_found=field_data.get("page"),
            section_found=field_data.get("section")
        )
        fields[field_name] = extracted_field

    # Build metadata
    metadata = ProcessingMetadata(
        extraction_id=extraction_result.extraction_id,
        timestamp=extraction_result.timestamp,
        source_file=extraction_result.source_file,
        total_pages=len(extraction_result.pages_processed),
        pages_processed=extraction_result.pages_processed,
        llm_calls=extraction_result.llm_calls,
        docling_processed=extraction_result.docling_processed,
        processing_time_ms=extraction_result.processing_time_ms,
        models_used={
            "extraction": settings.EXTRACTION_MODEL
        }
    )

    # Determine status
    if extraction_result.overall_confidence >= settings.AUTO_ACCEPT_THRESHOLD:
        status = ExtractionStatus.SUCCESS
        recommendation = RecommendationType.AUTO_ACCEPT
    elif extraction_result.overall_confidence >= settings.FLAG_THRESHOLD:
        status = ExtractionStatus.SUCCESS
        recommendation = RecommendationType.FLAG_FOR_REVIEW
    else:
        status = ExtractionStatus.PENDING_REVIEW
        recommendation = RecommendationType.MANUAL_REQUIRED

    return ExtractionOutput(
        classification=classification,
        fields=fields,
        overall_confidence=extraction_result.overall_confidence,
        overall_status=status,
        recommendation=recommendation,
        requires_review=extraction_result.requires_review,
        review_reasons=extraction_result.review_reasons,
        metadata=metadata
    )


# For local testing
if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "messageId": "test-message-1",
                "body": json.dumps({
                    "filePath": "utility-bills/test/sample.pdf",
                    "app_no": "APP-TEST-123",
                    "process": "m0_utility_bill"
                })
            }
        ]
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
