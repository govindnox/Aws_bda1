"""
Lambda Function Handler for Document Extraction.

This Lambda is triggered by an SQS FIFO queue containing document
extraction requests.  The handler is intentionally **thin** — it only
parses the event and delegates all business logic to the
``ExtractionController``.

Tags:
    Application: Document-Extraction
    Environment: Dev

Author: Reet Roy
Version: 1.0.0
"""

import json
import logging

# Only lightweight imports at module level — no boto3, no heavy packages
logger = logging.getLogger()


def lambda_handler(event, _context):
    """Entry point for the extraction Lambda.

    Delegates processing to ``ExtractionController.handle_event``.

    Args:
        event: Lambda event containing SQS Records.
        _context: Lambda context (unused).

    Returns:
        dict: ``{ "batchItemFailures": [...] }`` for SQS partial
              batch retry support.
    """
    logger.info("Received event: %s", json.dumps(event))

    try:
        # Lazy import — keeps cold-start time minimal
        from controller import ExtractionController

        controller = ExtractionController()
        return controller.handle_event(event)
    except Exception as e:
        logger.exception("Unhandled error_message in lambda_handler: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error_message": str(e)}),
        }


def sf_push_handler(event, _context):
    """Entry point for the Salesforce push Lambda.

    Delegates processing to ``SalesforcePushController.handle_event``.

    Args:
        event: Lambda event containing SQS Records.
        _context: Lambda context (unused).

    Returns:
        dict: ``{ "batchItemFailures": [...] }`` for SQS partial
              batch retry support.
    """
    logger.info("Received SF push event: %s", json.dumps(event))

    try:
        # Lazy import — keeps cold-start time minimal
        from salesforce_push_controller import SalesforcePushController

        controller = SalesforcePushController()
        return controller.handle_event(event)
    except Exception as e:
        logger.exception("Unhandled error_message in sf_push_handler: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error_message": str(e)}),
        }
