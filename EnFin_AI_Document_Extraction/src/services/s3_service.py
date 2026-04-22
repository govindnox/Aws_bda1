"""
S3 service for downloading source documents.

Author: Reet Roy
Version: 1.0.0
"""

import logging
from typing import Any, Dict, List, Optional
import json

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


#Added for bda
def list_keys(prefix: str, bucket: Optional[str] = None) -> List[str]:
    """List all object keys under a prefix.

    Args:
        prefix: S3 key prefix to search under.
        bucket: Override bucket name (defaults to ``config.aws.s3_bucket``).

    Returns:
        List of object keys. Empty list if nothing matches.
    """
    from config import config

    target_bucket = bucket or config.aws.s3_bucket
    client = _get_client()

    logger.info("Listing objects under s3://%s/%s", target_bucket, prefix)
    response = client.list_objects_v2(Bucket=target_bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    return [obj["Key"] for obj in objects]


def read_json(s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """Download an S3 object and parse it as JSON.

    Args:
        s3_key: Object key within the bucket.
        bucket: Override bucket name (defaults to ``config.aws.s3_bucket``).

    Returns:
        Parsed JSON as a dict.

    Raises:
        json.JSONDecodeError: If the object body isn't valid JSON.
        botocore ClientError: If the object doesn't exist or isn't readable.
    """
    from config import config

    target_bucket = bucket or config.aws.s3_bucket
    client = _get_client()

    logger.info("Reading JSON from s3://%s/%s", target_bucket, s3_key)
    response = client.get_object(Bucket=target_bucket, Key=s3_key)
    raw = response["Body"].read()
    return json.loads(raw)