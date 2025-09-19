"""
Pipelines Client Library

A client library for interacting with the data pipeline service.
"""

from pipelines_client.client import PipelinesClient, PipelinesClientError, create_client
from pipelines_client.requests import ConcentrationAnalysisRequest
from pipelines_client.responses import (
    ConcentrationAnalysisResponse,
    LoadFileResponse,
    TableData,
)

__version__ = "0.1.0"
__all__ = [
    "PipelinesClient",
    "PipelinesClientError",
    "create_client",
    "ConcentrationAnalysisRequest",
    "ConcentrationAnalysisResponse",
    "LoadFileResponse",
    "TableData",
]
