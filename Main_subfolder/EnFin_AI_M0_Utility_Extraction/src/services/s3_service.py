"""
AWS S3 service for downloading utility bill documents and storing results.
"""

import boto3
import json
from typing import Dict, Any, Optional
from datetime import datetime

from config.settings import settings


class S3Service:
    """Service for S3 operations"""

    def __init__(self):
        self.client = boto3.client('s3', region_name=settings.REGION)
        self.bucket = settings.S3_BUCKET

    def download_document(self, s3_path: str) -> bytes:
        """
        Download a document from S3.

        Args:
            s3_path: Path within the bucket (key)

        Returns:
            Document bytes
        """
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=s3_path
        )
        return response['Body'].read()

    def upload_extraction_result(
        self,
        extraction_id: str,
        result: Dict[str, Any],
        source_file: str
    ) -> str:
        """
        Upload extraction result to S3 for audit/debugging.

        Args:
            extraction_id: Unique extraction ID
            result: Extraction result dictionary
            source_file: Original source file path

        Returns:
            S3 key where result was stored
        """
        # Generate key based on date and extraction ID
        date_prefix = datetime.now().strftime("%Y/%m/%d")
        key = f"extraction-results/{date_prefix}/{extraction_id}.json"

        # Add metadata
        result_with_meta = {
            "extraction_id": extraction_id,
            "source_file": source_file,
            "stored_at": datetime.now().isoformat(),
            "result": result
        }

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(result_with_meta, indent=2),
            ContentType="application/json"
        )

        return key

    def check_document_exists(self, s3_path: str) -> bool:
        """
        Check if a document exists in S3.

        Args:
            s3_path: Path within the bucket (key)

        Returns:
            True if exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_path)
            return True
        except self.client.exceptions.ClientError:
            return False

    def get_document_metadata(self, s3_path: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a document in S3.

        Args:
            s3_path: Path within the bucket (key)

        Returns:
            Metadata dictionary or None if not found
        """
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=s3_path)
            return {
                "content_type": response.get("ContentType"),
                "content_length": response.get("ContentLength"),
                "last_modified": response.get("LastModified").isoformat()
                if response.get("LastModified") else None,
                "metadata": response.get("Metadata", {})
            }
        except self.client.exceptions.ClientError:
            return None
