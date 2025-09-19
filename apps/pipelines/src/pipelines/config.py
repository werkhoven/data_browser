"""
Configuration management for the data pipeline service.
"""

import os
from typing import Optional


class Config:
    """Configuration class for managing environment variables and settings."""

    # S3 Configuration
    S3_BUCKET: str = os.getenv("S3_BUCKET", "data-browser-uploads")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_ENDPOINT_URL: Optional[str] = os.getenv("AWS_ENDPOINT_URL")

    # API Configuration
    API_TITLE: str = os.getenv("API_TITLE", "Data Pipeline Service")
    API_VERSION: str = os.getenv("API_VERSION", "1.0.0")

    # Server Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))

    # File Processing Configuration
    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "100"))
    ALLOWED_EXTENSIONS: list = [".csv"]  # Could be expanded via env var if needed

    @classmethod
    def validate_aws_credentials(cls) -> bool:
        """Validate that AWS credentials are available."""
        return bool(cls.AWS_ACCESS_KEY_ID and cls.AWS_SECRET_ACCESS_KEY)

    @classmethod
    def get_s3_config(cls) -> dict:
        """Get S3 configuration as a dictionary."""
        return {
            "bucket": cls.S3_BUCKET,
            "region": cls.AWS_REGION,
            "access_key_id": cls.AWS_ACCESS_KEY_ID,
            "secret_access_key": cls.AWS_SECRET_ACCESS_KEY,
        }


# Global config instance
config = Config()
