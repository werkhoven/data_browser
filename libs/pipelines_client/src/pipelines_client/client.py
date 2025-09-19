"""
HTTP client for the data pipeline service.

This module provides a client to interact with the pipelines API,
handling file uploads, data processing, and analysis operations.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import httpx
from pydantic import ValidationError

from pipelines_client.requests import ConcentrationAnalysisRequest
from pipelines_client.responses import (
    ConcentrationAnalysisResponse,
    LoadFileResponse,
    UploadFileResponse,
)

logger = logging.getLogger(__name__)


class PipelinesClientError(Exception):
    """Base exception for pipelines client errors."""

    pass


class PipelinesClient:
    """HTTP client for the data pipeline service."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: float = 30.0,
        api_key: Optional[str] = None,
    ):
        """
        Initialize the pipelines client.

        Args:
            base_url: Base URL of the pipelines API
            timeout: Request timeout in seconds
            api_key: Optional API key for authentication
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.api_key = api_key
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _ensure_client(self):
        """Ensure the HTTP client is initialized."""
        if self._client is None:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers=headers,
            )

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> httpx.Response:
        """
        Make an HTTP request with error handling.

        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional arguments for the request

        Returns:
            HTTP response

        Raises:
            PipelinesClientError: If the request fails
        """
        await self._ensure_client()

        if self._client is None:
            raise PipelinesClientError("Failed to initialize HTTP client")

        try:
            response = await self._client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
            logger.error(error_msg)
            raise PipelinesClientError(error_msg) from e
        except httpx.RequestError as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(error_msg)
            raise PipelinesClientError(error_msg) from e

    async def health_check(self) -> dict:
        """
        Check the health of the pipelines service.

        Returns:
            Health status dictionary

        Raises:
            PipelinesClientError: If the health check fails
        """
        response = await self._make_request("GET", "/health")
        return response.json()

    async def upload_file(
        self,
        file_path: Union[str, Path],
        filename: Optional[str] = None,
    ) -> UploadFileResponse:
        """
        Upload a CSV file to the pipelines service.

        Args:
            file_path: Path to the CSV file to upload
            filename: Optional custom filename (defaults to file basename)

        Returns:
            Upload response dictionary

        Raises:
            PipelinesClientError: If the upload fails
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise PipelinesClientError(f"File not found: {file_path}")

        if not file_path.suffix.lower() == ".csv":
            raise PipelinesClientError(
                f"Only CSV files are supported, got: {file_path.suffix}"
            )

        filename = filename or file_path.name

        with open(file_path, "rb") as f:
            files = {"file": (filename, f, "text/csv")}
            response = await self._make_request("POST", "/files/upload", files=files)

        return UploadFileResponse.model_validate(response.json())

    async def load_file(self, s3_key: str) -> LoadFileResponse:
        """
        Load and process a file from S3.

        Args:
            s3_key: S3 key of the file to load

        Returns:
            LoadFileResponse with processed table data

        Raises:
            PipelinesClientError: If the file loading fails
            ValidationError: If the response is invalid
        """
        response = await self._make_request(
            "GET",
            "/files",
            params={"s3_key": s3_key},
        )

        try:
            return LoadFileResponse.model_validate(response.json())
        except ValidationError as e:
            error_msg = f"Invalid response format: {str(e)}"
            logger.error(error_msg)
            raise PipelinesClientError(error_msg) from e

    async def run_concentration_analysis(
        self,
        cache_key: str,
        on: str,
        by: list[str],
    ) -> ConcentrationAnalysisResponse:
        """
        Run concentration analysis on a cached table.

        Args:
            cache_key: Cache key of the table to analyze
            on: Column to perform concentration analysis on (pivot by)
            by: Column to group by (concentration measure)

        Returns:
            ConcentrationAnalysisResponse with analysis results

        Raises:
            PipelinesClientError: If the analysis fails
            ValidationError: If the response is invalid
        """
        request_data = ConcentrationAnalysisRequest(
            cache_key=cache_key,
            on=on,
            by=by,
        )

        response = await self._make_request(
            "POST",
            "/analyses/concentration",
            json=request_data.model_dump(),
        )

        try:
            return ConcentrationAnalysisResponse.model_validate(response.json())
        except ValidationError as e:
            error_msg = f"Invalid response format: {str(e)}"
            logger.error(error_msg)
            raise PipelinesClientError(error_msg) from e

    async def upload_and_process_file(
        self,
        file_path: Union[str, Path],
        filename: Optional[str] = None,
    ) -> LoadFileResponse:
        """
        Upload a file and immediately process it in one operation.

        Args:
            file_path: Path to the CSV file to upload
            filename: Optional custom filename

        Returns:
            LoadFileResponse with processed table data

        Raises:
            PipelinesClientError: If the upload or processing fails
        """
        # Upload the file
        upload_result = await self.upload_file(file_path, filename)
        s3_key = upload_result.s3_key

        # Process the uploaded file
        return await self.load_file(s3_key)


# Convenience function for quick usage
async def create_client(
    base_url: str = "http://localhost:8000",
    timeout: float = 30.0,
    api_key: Optional[str] = None,
) -> PipelinesClient:
    """
    Create and return a PipelinesClient instance.

    Args:
        base_url: Base URL of the pipelines API
        timeout: Request timeout in seconds
        api_key: Optional API key for authentication

    Returns:
        PipelinesClient instance
    """
    client = PipelinesClient(base_url=base_url, timeout=timeout, api_key=api_key)
    await client._ensure_client()
    return client
