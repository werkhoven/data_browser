"""
Dependency injection and external service initialization.
"""

import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .config import config

logger = logging.getLogger(__name__)

# Global S3 client instance
_s3_client = None


def get_s3_client():
    """
    Get or create the S3 client instance.

    Returns:
        Configured boto3 S3 client

    Raises:
        RuntimeError: If AWS credentials are not properly configured
    """
    global _s3_client

    if _s3_client is None:
        if not config.validate_aws_credentials():
            raise RuntimeError(
                "AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
            )

        try:
            client_kwargs = {
                "region_name": config.AWS_REGION,
                "aws_access_key_id": config.AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": config.AWS_SECRET_ACCESS_KEY,
            }

            # Add endpoint URL if provided (for LocalStack)
            if config.AWS_ENDPOINT_URL:
                client_kwargs["endpoint_url"] = config.AWS_ENDPOINT_URL

            _s3_client = boto3.client("s3", **client_kwargs)

            # Test the connection by listing the bucket
            _s3_client.head_bucket(Bucket=config.S3_BUCKET)
            logger.info(
                f"S3 client initialized successfully for bucket: {config.S3_BUCKET}"
            )

        except NoCredentialsError:
            raise RuntimeError("AWS credentials not found")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                raise RuntimeError(f"S3 bucket '{config.S3_BUCKET}' not found")
            elif error_code == "403":
                raise RuntimeError(f"Access denied to S3 bucket '{config.S3_BUCKET}'")
            else:
                raise RuntimeError(f"Failed to initialize S3 client: {str(e)}")

    return _s3_client


def reset_s3_client():
    """Reset the S3 client instance (useful for testing)."""
    global _s3_client
    _s3_client = None


async def health_check_s3() -> dict:
    """
    Perform a health check on the S3 service.

    Returns:
        Dictionary with health status information
    """
    try:
        client = get_s3_client()
        client.head_bucket(Bucket=config.S3_BUCKET)
        return {
            "status": "healthy",
            "service": "s3",
            "bucket": config.S3_BUCKET,
            "region": config.AWS_REGION,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "s3",
            "error": str(e),
        }
