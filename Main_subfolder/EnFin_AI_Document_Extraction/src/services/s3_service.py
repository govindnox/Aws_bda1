"""
S3 service for downloading source documents.

Author: Reet Roy
Version: 1.0.0
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# boto3 is lazy-loaded to avoid cold-start overhead
_s3_client = None


def _get_client():
    """Lazy-initialise the S3 client on first use."""
    global _s3_client
    if _s3_client is None:
        import boto3
        from config import config

        _s3_client = boto3.client("s3", region_name=config.aws.region)
    return _s3_client


def download_document(s3_key: str, bucket: Optional[str] = None) -> bytes:
    """Download a document from S3.

    Args:
        s3_key: Object key within the bucket.
        bucket: Override bucket name (defaults to ``config.aws.s3_bucket``).

    Returns:
        Raw document bytes.
    """
    from config import config

    target_bucket = bucket or config.aws.s3_bucket
    client = _get_client()

    logger.info("Downloading s3://%s/%s", target_bucket, s3_key)
    response = client.get_object(Bucket=target_bucket, Key=s3_key)
    file_bytes = response["Body"].read()
    logger.info("Downloaded %d bytes from S3", len(file_bytes))
    return file_bytes
