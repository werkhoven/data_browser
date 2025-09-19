from enum import StrEnum

import polars as pl
from pydantic import BaseModel, ConfigDict, Field


class TableSource(StrEnum):
    CSV = "csv"
    DATABASE = "database"
    API = "api"
    FILE = "file"
    STREAM = "stream"
    OTHER = "other"


class Table(BaseModel):
    """
    A table in the database.
    """

    model_config: ConfigDict = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(description="The name of the table.")
    source: TableSource = Field(description="The source of the table.")
    data: pl.DataFrame = Field(description="The data in the table.")
    path: str | None = Field(default=None, description="The path to the table.")
    query: str | None = Field(default=None, description="The query to the table.")

    @property
    def categorical_columns(self) -> list[str]:
        """The categorical columns of the table."""
        return self.data.select([pl.col(pl.Categorical), pl.col(pl.String)]).columns

    @property
    def datetime_columns(self) -> list[str]:
        """The datetime columns of the table."""
        return self.data.select(pl.col(pl.Datetime)).columns

    @property
    def dimension_columns(self) -> list[str]:
        """The dimension columns of the table."""
        return [*self.categorical_columns, *self.datetime_columns]

    @property
    def numeric_columns(self) -> list[str]:
        """The numeric columns of the table."""
        return self.data.select([pl.col(pl.Float64), pl.col(pl.Int64)]).columns

    def validate_columns(self, columns: list[str]):
        """Validate columns exist in the table."""
        if set(columns) - set(self.data.columns):
            raise ValueError(
                f"Column {set(columns) - set(self.data.columns)} not found in table."
            )

    def validate_dimensions(self, dimensions: list[str]):
        """Validate dimensions exist in the table."""
        if set(dimensions) - set(self.dimension_columns):
            raise ValueError(
                f"Dimension {set(dimensions) - set(self.dimension_columns)} not table dimensions."
            )

    def validate_measures(self, measures: list[str]):
        """Validate measures exist in the table."""
        if set(measures) - set(self.numeric_columns):
            raise ValueError(
                f"Measure {set(measures) - set(self.numeric_columns)} not found in table."
            )
