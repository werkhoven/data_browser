"""
Request models for the data pipeline service.
"""

from pydantic import BaseModel, Field


class ConcentrationAnalysisRequest(BaseModel):
    """Request model for concentration analysis."""

    cache_key: str = Field(description="Cache key of the table to analyze")
    on: str = Field(
        description="Column to perform concentration analysis on (pivot by)"
    )
    by: list[str] = Field(description="Columns to group by (concentration measure)")
