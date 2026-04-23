"""
Lambda Function Handler for Extraction Aggregation.

This Lambda is triggered by an SQS FIFO queue (with 600s delay) containing
aggregation requests.  The handler is intentionally **thin** — it only
parses the event and delegates all business logic to the
``AggregationController``.

Tags:
    Application: Document-Extraction-Aggregation
    Environment: Dev

Author: Reet Roy
Version: 1.0.0
"""

import json
import logging

logger = logging.getLogger()


def lambda_handler(event, _context):
    """Entry point for the aggregation Lambda.

    Delegates processing to ``AggregationController.handle_event``.

    Args:
        event: Lambda event containing SQS Records.
        _context: Lambda context (unused).

    Returns:
        dict: ``{ "batchItemFailures": [...] }`` for SQS partial
              batch retry support.
    """
    logger.info("Received aggregation event: %s", json.dumps(event))

    try:
        # Lazy import — keeps cold-start time minimal
        from aggregation_controller import AggregationController

        controller = AggregationController()
        return controller.handle_event(event)
    except Exception as e:
        logger.exception("Unhandled error in aggregation lambda_handler: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error_message": str(e)}),
        }
