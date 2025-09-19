from __future__ import annotations

from collections import defaultdict
from typing import override

import polars as pl
from data.models.schemas import ColumnSchema, DataTypeEnum, DatetimePart
from data.models.tables import Table
from data.transforms.base import BaseTransform
from pydantic import Field, ValidationError


class ColumnSchemaTransform(BaseTransform):
    """
    Transform that applies a column schema to the data.
    """

    column_schemas: list[ColumnSchema] = Field(
        description="The column schemas to apply to the data."
    )

    @override
    def apply(self, table: Table) -> Table:
        """Parse the table columns according to the schema."""
        schema_cols = {schema.name for schema in self.column_schemas}
        if schema_cols - set(table.data.columns):
            raise ValueError(
                f"Column {schema_cols - set(table.data.columns)} not in data"
            )

        try:
            data = table.data.select(pl.all().str.strip_chars())
            table.data = data.select(
                self._parse_expr(schema.name, schema) for schema in self.column_schemas
            )
            return table
        except Exception as e:
            raise ValueError(f"Error parsing columns {schema_cols}: {e}") from e

    @classmethod
    def _parse_expr(cls, column: str, schema: ColumnSchema) -> pl.Expr:
        """Parse the column according to the schema."""

        dtype_map = {
            DataTypeEnum.INTEGER: pl.Int64,
            DataTypeEnum.FLOAT: pl.Float64,
            DataTypeEnum.BOOLEAN: pl.Boolean,
            DataTypeEnum.DATETIME: pl.Datetime,
        }

        if schema.data_type == DataTypeEnum.STRING:
            return pl.col(column).str.replace_all(schema.regex_cleaning_pattern, "")
        elif schema.data_type in (DataTypeEnum.FLOAT, DataTypeEnum.INTEGER):
            return (
                pl.col(column)
                .str.replace_all(schema.regex_cleaning_pattern, "")
                .replace(["", "-"], None)
                .cast(dtype_map[schema.data_type])
            )
        elif schema.data_type == DataTypeEnum.BOOLEAN:
            return (
                pl.col(column)
                .str.to_lowercase()
                .str.replace_all(schema.regex_cleaning_pattern, "")
                .replace(["", "-"], None)
                .replace(["yes", "no", "true", "false"], ["1", "0", "1", "0"])
                .cast(dtype_map[DataTypeEnum.INTEGER])
                .cast(dtype_map[DataTypeEnum.BOOLEAN])
            )
        elif schema.data_type == DataTypeEnum.DATETIME:
            return pl.col(column).str.to_datetime(schema.datetime_format)
        else:
            raise ValidationError(f"Invalid data type: {schema.data_type}")


class FusePartialDatetimeColumnsTransform(BaseTransform):
    """Fuse partial datetime columns into a single datetime column."""

    column_schemas: list[ColumnSchema] = Field(
        description="The column schemas to apply to the data."
    )

    @override
    def apply(self, table: Table) -> Table:
        """Fuse partial datetime columns into a single datetime column."""
        table.data = self.fuse_partial_datetime_columns(table.data, self.column_schemas)
        return table

    @classmethod
    def fuse_partial_datetime_columns(
        cls, frame: pl.DataFrame, column_schemas: list[ColumnSchema]
    ) -> pl.DataFrame:
        """Fuse partial datetime columns into a single datetime column."""
        # Group columns by their parent column name
        col_groups: dict[str, dict[DatetimePart, str]] = defaultdict(dict)
        for schema in column_schemas:
            if not schema.partial_datetime_schema:
                continue
            parent_name = schema.partial_datetime_schema.parent_column_name
            part = schema.partial_datetime_schema.part
            col_groups[parent_name][part] = schema.name

        # Create datetime columns from the parts
        for parent_name, parts_dict in col_groups.items():
            # Ensure we have at least the year
            if DatetimePart.YEAR not in parts_dict:
                raise ValidationError(f"Missing required year part for {parent_name}")

            # Prepare expressions for each part, handling both string and numeric columns
            # Required part
            year_expr = cls._datetime_part_expr(frame, parts_dict[DatetimePart.YEAR])

            # Date parts with defaults
            month_expr = cls._datetime_part_expr(
                frame, parts_dict.get(DatetimePart.MONTH), default=1
            )
            day_expr = cls._datetime_part_expr(
                frame, parts_dict.get(DatetimePart.DAY), default=1
            )

            # Time parts with defaults
            hour_expr = cls._datetime_part_expr(
                frame, parts_dict.get(DatetimePart.HOUR), default=0
            )
            minute_expr = cls._datetime_part_expr(
                frame, parts_dict.get(DatetimePart.MINUTE), default=0
            )
            second_expr = cls._datetime_part_expr(
                frame, parts_dict.get(DatetimePart.SECOND), default=0
            )

            # Create the datetime column
            datetime_expr = pl.datetime(
                year_expr, month_expr, day_expr, hour_expr, minute_expr, second_expr
            )

            # Add the new datetime column and drop the original part columns
            frame = frame.with_columns(datetime_expr.alias(parent_name))
            frame = frame.drop(list(parts_dict.values()))

        return frame

    @classmethod
    def _datetime_part_expr(
        cls,
        frame: pl.DataFrame,
        column_name: str | None,
        default: int | None = None,
    ) -> pl.Expr:
        """Prepare a datetime part column, handling both string and numeric types."""
        if column_name is None:
            return pl.lit(default)

        if column_name not in frame.columns:
            return pl.lit(default)

        # Check if the column is numeric or string
        column_dtype = frame.select(pl.col(column_name)).dtypes[0]

        if column_dtype in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ]:
            # Already numeric, use directly
            return pl.col(column_name)
        else:
            # String column, try to convert to int, with fallback to default
            return pl.col(column_name).str.to_integer(strict=False).fill_null(default)


class StringToCategoricalTransform(BaseTransform):
    """Convert string columns to categorical if they have low uniqueness."""

    uniqueness_threshold: float = Field(
        default=0.1,
        description="Threshold for uniqueness percentage (0.1 = 10% unique values)",
    )
    min_unique: int = Field(
        default=1,
        description="Minimum number of unique values to consider for categorical",
    )
    max_unique: int = Field(
        default=100,
        description="Maximum number of unique values to consider for categorical",
    )

    @override
    def apply(self, table: "Table") -> "Table":
        """Convert string columns to categorical based on uniqueness."""
        string_columns = table.data.select(pl.col(pl.String)).columns

        if not string_columns:
            return table

        # Calculate uniqueness for each string column
        uniqueness_stats = self._calculate_uniqueness(table.data, string_columns)

        # Determine which columns should be categorical
        categorical_columns = self._identify_categorical_columns(uniqueness_stats)

        if not categorical_columns:
            return table

        # Convert to categorical
        exprs = []
        for col in table.data.columns:
            if col in categorical_columns:
                exprs.append(pl.col(col).cast(pl.Categorical))
            else:
                exprs.append(pl.col(col))

        table.data = table.data.select(exprs)
        return table

    def _calculate_uniqueness(
        self, frame: pl.DataFrame, columns: list[str]
    ) -> dict[str, dict[str, float]]:
        """Calculate uniqueness statistics for string columns."""
        stats = {}

        for col in columns:
            # Count total non-null values
            total_count = frame.select(pl.col(col).is_not_null().sum()).item()

            if total_count == 0:
                continue

            # Count unique values
            unique_count = frame.select(pl.col(col).n_unique()).item()

            # Calculate uniqueness percentage
            uniqueness_pct = unique_count / total_count

            stats[col] = {
                "unique_count": unique_count,
                "total_count": total_count,
                "uniqueness_pct": uniqueness_pct,
            }

        return stats

    def _identify_categorical_columns(
        self, uniqueness_stats: dict[str, dict[str, float]]
    ) -> list[str]:
        """Identify which columns should be converted to categorical."""
        categorical_cols = []

        for col, stats in uniqueness_stats.items():
            unique_count = stats["unique_count"]
            uniqueness_pct = stats["uniqueness_pct"]

            # Check if column meets categorical criteria
            if (
                self.min_unique <= unique_count <= self.max_unique
                and uniqueness_pct <= self.uniqueness_threshold
            ):
                categorical_cols.append(col)

        return categorical_cols


class DefaultColumnSortingTransform(BaseTransform):
    """Sort the columns by the default column sorting."""

    @override
    def apply(self, table: Table) -> Table:
        """Sort the columns by the default column sorting."""
        order = [
            *sorted(table.datetime_columns),
            *sorted(table.categorical_columns),
            *sorted(table.numeric_columns),
        ]
        table.data = table.data.select(order)
        return table
