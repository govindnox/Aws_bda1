"""
DynamoDB service for storing extraction results and audit logs.
"""

import logging
import boto3
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal
import json

from config.settings import settings

logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types for DynamoDB"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def convert_floats_to_decimal(obj: Any) -> Any:
    """Convert float values to Decimal for DynamoDB storage"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    return obj


class DynamoDBService:
    """Service for DynamoDB operations"""

    def __init__(self):
        self.client = boto3.resource(
            'dynamodb',
            region_name=settings.REGION
        )
        self.table_name = settings.DYNAMODB_TABLE_NAME
        self.table = self.client.Table(self.table_name)

    def store_extraction_result(
        self,
        extraction_id: str,
        project_id: str,
        source_file: str,
        extraction_result: Dict[str, Any],
        salesforce_response: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Store extraction result in DynamoDB.

        Args:
            extraction_id: Unique extraction identifier
            project_id: Salesforce project/opportunity ID
            source_file: S3 path to source document
            extraction_result: Complete extraction result
            salesforce_response: Response from Salesforce update (if any)

        Returns:
            DynamoDB put response
        """
        timestamp = datetime.utcnow().isoformat()

        # Prepare item with proper type conversion for DynamoDB
        item = {
            'extraction_id': extraction_id,
            'project_id': project_id,
            'source_file': source_file,
            'created_at': timestamp,
            'updated_at': timestamp,

            # Classification
            'utility_provider': extraction_result.get('classification', {}).get('provider', 'Unknown'),
            'state': extraction_result.get('classification', {}).get('state', 'Unknown'),
            'program': extraction_result.get('classification', {}).get('program', 'Unknown'),

            # Overall status
            'overall_confidence': convert_floats_to_decimal(
                extraction_result.get('overall_confidence', 0)
            ),
            'overall_status': extraction_result.get('overall_status', 'unknown'),
            'recommendation': extraction_result.get('recommendation', 'manual_required'),
            # GSI key is type S — store as string "true"/"false"
            'requires_review': str(extraction_result.get('requires_review', True)).lower(),
            'review_reasons': extraction_result.get('review_reasons', []),

            # Extracted fields (stored as map)
            'extracted_fields': convert_floats_to_decimal(
                extraction_result.get('fields', {})
            ),

            # Metadata
            'metadata': convert_floats_to_decimal(
                extraction_result.get('metadata', {})
            ),

            # TTL for automatic cleanup (90 days)
            'ttl': int(datetime.utcnow().timestamp()) + (90 * 24 * 60 * 60)
        }

        # Add Salesforce response if available
        if salesforce_response:
            item['salesforce_response'] = salesforce_response
            item['salesforce_updated'] = True
            item['salesforce_updated_at'] = timestamp
        else:
            item['salesforce_updated'] = False

        try:
            response = self.table.put_item(Item=item)
            logger.info(f"Stored extraction result: {extraction_id}")
            return response
        except Exception as e:
            logger.error(f"Failed to store extraction result: {str(e)}")
            raise

    def update_salesforce_status(
        self,
        extraction_id: str,
        salesforce_response: Dict[str, Any],
        success: bool = True,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update the Salesforce sync status for an extraction.

        Args:
            extraction_id: Extraction ID to update
            salesforce_response: Response from Salesforce
            success: Whether the update was successful
            error_message: Error message if failed

        Returns:
            DynamoDB update response
        """
        timestamp = datetime.utcnow().isoformat()

        update_expression = """
            SET salesforce_updated = :updated,
                salesforce_updated_at = :updated_at,
                salesforce_response = :response,
                salesforce_success = :success,
                updated_at = :updated_at
        """

        expression_values = {
            ':updated': True,
            ':updated_at': timestamp,
            ':response': salesforce_response,
            ':success': success
        }

        if error_message:
            update_expression += ", salesforce_error = :error"
            expression_values[':error'] = error_message

        try:
            response = self.table.update_item(
                Key={'extraction_id': extraction_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='UPDATED_NEW'
            )
            logger.info(f"Updated Salesforce status for: {extraction_id}")
            return response
        except Exception as e:
            logger.error(f"Failed to update Salesforce status: {str(e)}")
            raise

    def get_extraction_result(self, extraction_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an extraction result by ID.

        Args:
            extraction_id: Extraction ID to retrieve

        Returns:
            Extraction result or None if not found
        """
        try:
            response = self.table.get_item(Key={'extraction_id': extraction_id})
            return response.get('Item')
        except Exception as e:
            logger.error(f"Failed to get extraction result: {str(e)}")
            return None

    def get_extractions_by_project(
        self,
        project_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get all extractions for a project.

        Args:
            project_id: Project ID to query
            limit: Maximum results to return

        Returns:
            List of extraction results
        """
        try:
            response = self.table.query(
                IndexName='project_id-index',
                KeyConditionExpression='project_id = :pid',
                ExpressionAttributeValues={':pid': project_id},
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Failed to query extractions by project: {str(e)}")
            return []

    def get_pending_reviews(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get extractions pending manual review.

        Args:
            limit: Maximum results to return

        Returns:
            List of extraction results needing review
        """
        try:
            response = self.table.query(
                IndexName='requires_review-index',
                KeyConditionExpression='requires_review = :review',
                ExpressionAttributeValues={':review': 'true'},
                Limit=limit
            )
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Failed to query pending reviews: {str(e)}")
            return []

    def mark_review_complete(
        self,
        extraction_id: str,
        reviewer: str,
        corrections: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Mark an extraction as reviewed.

        Args:
            extraction_id: Extraction ID
            reviewer: Reviewer identifier
            corrections: Any corrections made

        Returns:
            DynamoDB update response
        """
        timestamp = datetime.utcnow().isoformat()

        update_expression = """
            SET requires_review = :review,
                reviewed_at = :reviewed_at,
                reviewed_by = :reviewer,
                updated_at = :updated_at
        """

        expression_values = {
            ':review': 'false',
            ':reviewed_at': timestamp,
            ':reviewer': reviewer,
            ':updated_at': timestamp
        }

        if corrections:
            update_expression += ", review_corrections = :corrections"
            expression_values[':corrections'] = convert_floats_to_decimal(corrections)

        try:
            response = self.table.update_item(
                Key={'extraction_id': extraction_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='UPDATED_NEW'
            )
            logger.info(f"Marked review complete for: {extraction_id}")
            return response
        except Exception as e:
            logger.error(f"Failed to mark review complete: {str(e)}")
            raise
