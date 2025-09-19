"""
Configuration management for the browser application.
"""

import os


class Config:
    """Configuration class for managing environment variables and settings."""

    # Pipelines Service Configuration
    PIPELINES_URL: str = os.getenv("PIPELINES_URL", "http://localhost:8000")

    # File Configuration
    DEFAULT_FILE_PATH: str = os.getenv("DEFAULT_FILE_PATH", "data/transaction_db.csv")

    BROWSER_HOST: str = os.getenv("BROWSER_HOST", "0.0.0.0")
    BROWSER_PORT: int = int(os.getenv("BROWSER_PORT", "8001"))
    BROWSER_SHARE: bool = os.getenv("BROWSER_SHARE", "false").lower() == "true"
    GRADIO_TEMP_DIR: str = os.getenv("GRADIO_TEMP_DIR", "/app/data")

    @classmethod
    def get_pipelines_config(cls) -> dict:
        """Get pipelines configuration as a dictionary."""
        return {
            "base_url": cls.PIPELINES_URL,
            "timeout": 30.0,
        }


# Global config instance
config = Config()
