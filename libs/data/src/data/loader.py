"""
Data ingestion and normalization module for CSV files.

This module provides functionality to:
- Load CSV files with automatic schema inference
- Normalize and clean data
- Detect anomalies and data quality issues
- Prepare data for analysis
"""

import logging
from pathlib import Path
from typing import Any

import polars as pl

from data.agents.agents import EngineDeps, datatype_parser
from data.models.schemas import ColumnSchema
from data.models.tables import Table, TableSource
from data.transforms.formatting import (
    ColumnSchemaTransform,
    DefaultColumnSortingTransform,
    FusePartialDatetimeColumnsTransform,
    StringToCategoricalTransform,
)

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles CSV data ingestion and normalization."""

    async def _process_dataframe(
        self,
        df: pl.DataFrame,
        name: str,
        column_schemas: list[ColumnSchema] | None = None,
    ) -> Table:
        """
        Shared logic for processing a DataFrame into a Table.

        Args:
            df: Polars DataFrame to process
            name: Name for the table
            column_schemas: Optional schema to use

        Returns:
            Processed Table
        """
        logger.info(f"Shape: {df.shape}")

        # Store schema information
        df = self.preprocess_data(df)

        if not column_schemas:
            result = await datatype_parser.run(deps=EngineDeps(frame=df.sample(100)))
            column_schemas = result.output

        table = Table(name=name, source=TableSource.FILE, data=df)
        table = self.format_table(table, column_schemas=column_schemas)
        return table

    async def load_csv(
        self,
        file_path: str,
        column_schemas: list[ColumnSchema] | None = None,
    ) -> Table:
        """
        Load a CSV file with automatic schema inference.

        Args:
            file_path: Path to the CSV file
            column_schemas: Schema to use for the data

        Returns:
            Table with the loaded data
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if not path.suffix.lower() == ".csv":
            raise ValueError(f"Expected CSV file, got: {path.suffix}")

        try:
            # Load with schema inference
            df = pl.read_csv(path, infer_schema=False)

            logger.info(f"Successfully loaded CSV: {path}")

            # Process using shared logic
            return await self._process_dataframe(df, path.name, column_schemas)

        except Exception as e:
            logger.error(f"Error loading CSV file {path}: {e}")
            raise

    async def load_csv_from_bytes(
        self,
        csv_bytes: bytes,
        filename: str,
        column_schemas: list[ColumnSchema] | None = None,
    ) -> Table:
        """
        Load a CSV from raw bytes with automatic schema inference.

        Args:
            csv_bytes: Raw CSV content as bytes
            filename: Original filename for metadata
            column_schemas: Schema to use for the data

        Returns:
            Table with the loaded data
        """
        try:
            # Load from bytes using StringIO
            import io

            csv_string = csv_bytes.decode("utf-8")
            csv_io = io.StringIO(csv_string)

            # Load with schema inference
            df = pl.read_csv(csv_io, infer_schema=False)

            logger.info(f"Successfully loaded CSV from bytes: {filename}")

            # Process using shared logic
            return await self._process_dataframe(df, filename, column_schemas)

        except Exception as e:
            logger.error(f"Error loading CSV from bytes {filename}: {e}")
            raise

    def preprocess_data(self, frame: pl.DataFrame) -> pl.DataFrame:
        """
        Preprocess the data.
        """
        # Remove leading and trailing whitespace
        return frame.select(pl.all().str.strip_chars())

    def format_table(self, table: Table, column_schemas: list[ColumnSchema]) -> Table:
        """
        Apply the column schemas to the data.
        """
        transforms = [
            ColumnSchemaTransform(column_schemas=column_schemas),
            FusePartialDatetimeColumnsTransform(column_schemas=column_schemas),
            StringToCategoricalTransform(
                min_unique=1, max_unique=100, uniqueness_threshold=0.1
            ),
            DefaultColumnSortingTransform(),
        ]
        for transform in transforms:
            table = transform(table=table)
        return table
