"""
Response models for the data pipeline service.
"""

from typing import Any

from pydantic import BaseModel, Field


class TableData(BaseModel):
    """Structured table data response."""

    name: str = Field(description="Name of the table")
    source: str = Field(description="Source of the table data")
    cache_key: str = Field(description="Cache key for the table")
    columns: list[str] = Field(description="List of column names")
    shape: tuple[int, int] = Field(description="Table shape [rows, columns]")
    data: list[dict[str, Any]] = Field(description="Table data as list of dictionaries")
    dimension_columns: list[str] = Field(description="Dimension/categorical columns")
    numeric_columns: list[str] = Field(description="Numeric columns")
    datetime_columns: list[str] = Field(description="Datetime columns")
    categorical_columns: list[str] = Field(description="Categorical columns")


class UploadFileResponse(BaseModel):
    """Response model for file uploading operations."""

    success: bool
    message: str
    s3_key: str = Field(description="S3 key where the file was uploaded")
    s3_bucket: str = Field(description="S3 bucket where the file was uploaded")
    filename: str = Field(description="Filename of the file that was uploaded")
    size: int = Field(description="Size of the file that was uploaded")


class LoadFileResponse(BaseModel):
    """Response model for file loading operations."""

    success: bool
    table: TableData
    message: str
    s3_key: str = Field(description="S3 key where the file was uploaded")


class ConcentrationAnalysisResponse(BaseModel):
    """Response model for concentration analysis operations."""

    success: bool
    table: TableData
    message: str
    pivot_by: list[str] = Field(description="Columns used for pivoting")
    concentration_measure: str = Field(
        description="Column used for concentration measure"
    )
