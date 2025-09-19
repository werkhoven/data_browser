"""
Unit tests for formatting transforms.
"""

from datetime import datetime

import polars as pl
import pytest
from data.models.schemas import (
    ColumnSchema,
    DataTypeEnum,
    DatetimePart,
    PartialDatetimeSchema,
)
from data.models.tables import Table, TableSource
from data.transforms.formatting import (
    ColumnSchemaTransform,
    DefaultColumnSortingTransform,
    FusePartialDatetimeColumnsTransform,
    StringToCategoricalTransform,
)


class TestColumnSchemaTransform:
    """Test cases for ColumnSchemaTransform."""

    @pytest.fixture
    def unformatted_table(self) -> Table:
        """Create a sample table for testing with messy data that needs cleaning."""
        data = pl.DataFrame(
            {
                "id": ["#1", "ID-2", "3rd"],
                "name": [" Alice ", "Bob", " (Charlie) "],
                "age": ["25 years", "30+", "35"],
                "salary": ["$50,000", "$60,000.00", "-70,000"],
                "is_active": ["YES", "no", "Yes"],
                "created_at": [
                    "2023-01-01T00:00:00",
                    "2023-01-02T00:00:00",
                    "2023-01-03T00:00:00",
                ],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    @pytest.fixture
    def formatted_table(self) -> Table:
        """Create a sample table for testing with cleaned data."""
        data = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "salary": [50000.0, 60000.0, -70000.0],
                "is_active": [True, False, True],
                "created_at": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_column_schema_transform(
        self, unformatted_table: Table, formatted_table: Table
    ):
        """Test the happy path for ColumnSchemaTransform."""
        column_schemas = [
            ColumnSchema(
                name="id",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern=r"[^\d]",  # Remove all non-digits
            ),
            ColumnSchema(
                name="name",
                data_type=DataTypeEnum.STRING,
                regex_cleaning_pattern=r"[^a-zA-Z0-9]",  # No cleaning needed for strings
            ),
            ColumnSchema(
                name="age",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern=r"[^\d]",  # Remove all non-digits
            ),
            ColumnSchema(
                name="salary",
                data_type=DataTypeEnum.FLOAT,
                regex_cleaning_pattern=r"[^\d.-]",  # Keep only digits, decimal points, and hyphens
            ),
            ColumnSchema(
                name="is_active",
                data_type=DataTypeEnum.BOOLEAN,
                regex_cleaning_pattern="",  # No regex cleaning, handled by string replacement
            ),
            ColumnSchema(
                name="created_at",
                data_type=DataTypeEnum.DATETIME,
                regex_cleaning_pattern="",
                datetime_format="%Y-%m-%dT%H:%M:%S",
            ),
        ]

        transform = ColumnSchemaTransform(column_schemas=column_schemas)
        result = transform(unformatted_table)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == unformatted_table.name
        assert result.data.equals(formatted_table.data)


class TestFusePartialDatetimeColumnsTransform:
    """Test cases for FusePartialDatetimeColumnsTransform."""

    @pytest.fixture
    def sample_table_with_datetime_parts(self) -> Table:
        """Create a sample table with separate datetime parts."""
        data = pl.DataFrame(
            {
                "year": [2023, 2023, 2023],
                "month": [1, 2, 3],
                "day": [1, 15, 30],
                "hour": [10, 14, 18],
                "minute": [30, 45, 0],
                "second": [0, 30, 0],
                "value": [100, 200, 300],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_fuse_partial_datetime_columns(
        self, sample_table_with_datetime_parts: Table
    ):
        """Test the happy path for FusePartialDatetimeColumnsTransform."""
        column_schemas = [
            ColumnSchema(
                name="year",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.YEAR, parent_column_name="datetime"
                ),
            ),
            ColumnSchema(
                name="month",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.MONTH, parent_column_name="datetime"
                ),
            ),
            ColumnSchema(
                name="day",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.DAY, parent_column_name="datetime"
                ),
            ),
            ColumnSchema(
                name="hour",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.HOUR, parent_column_name="datetime"
                ),
            ),
            ColumnSchema(
                name="minute",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.MINUTE, parent_column_name="datetime"
                ),
            ),
            ColumnSchema(
                name="second",
                data_type=DataTypeEnum.INTEGER,
                regex_cleaning_pattern="",
                partial_datetime_schema=PartialDatetimeSchema(
                    part=DatetimePart.SECOND, parent_column_name="datetime"
                ),
            ),
        ]

        transform = FusePartialDatetimeColumnsTransform(column_schemas=column_schemas)
        result = transform(sample_table_with_datetime_parts)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_with_datetime_parts.name

        # Verify the datetime column was created
        assert "datetime" in result.data.columns
        assert result.data["datetime"].dtype == pl.Datetime

        # Verify the original datetime part columns were removed
        assert "year" not in result.data.columns
        assert "month" not in result.data.columns
        assert "day" not in result.data.columns
        assert "hour" not in result.data.columns
        assert "minute" not in result.data.columns
        assert "second" not in result.data.columns

        # Verify the datetime values are correct
        expected_datetimes = [
            datetime(2023, 1, 1, 10, 30, 0),
            datetime(2023, 2, 15, 14, 45, 30),
            datetime(2023, 3, 30, 18, 0, 0),
        ]
        assert result.data["datetime"].to_list() == expected_datetimes


class TestStringToCategoricalTransform:
    """Test cases for StringToCategoricalTransform."""

    @pytest.fixture
    def sample_table_with_strings(self) -> Table:
        """Create a sample table with string columns."""
        data = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "category": ["A", "A", "B", "B", "C"],  # Low uniqueness
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],  # High uniqueness
                "status": [
                    "active",
                    "inactive",
                    "active",
                    "pending",
                    "active",
                ],  # Medium uniqueness
                "value": [100, 200, 300, 400, 500],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_string_to_categorical_transform(
        self, sample_table_with_strings: Table
    ):
        """Test the happy path for StringToCategoricalTransform."""
        transform = StringToCategoricalTransform(
            uniqueness_threshold=0.5,  # 50% uniqueness threshold - more lenient
            min_unique=2,
            max_unique=10,
        )
        result = transform(sample_table_with_strings)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_with_strings.name

        # Check which columns should be categorical based on uniqueness
        # category: 3 unique out of 5 = 60% uniqueness (should remain string)
        # status: 3 unique out of 5 = 60% uniqueness (should remain string)
        # name: 5 unique out of 5 = 100% uniqueness (should remain string)

        # With 50% threshold, none should be converted to categorical
        assert result.data["category"].dtype == pl.String
        assert result.data["status"].dtype == pl.String
        assert result.data["name"].dtype == pl.String

        # Verify that non-string columns are unchanged
        assert result.data["id"].dtype == pl.Int64
        assert result.data["value"].dtype == pl.Int64

        # Verify values are preserved
        assert set(result.data["category"].unique()) == {"A", "B", "C"}
        assert set(result.data["status"].unique()) == {"active", "inactive", "pending"}


class TestDefaultColumnSortingTransform:
    """Test cases for DefaultColumnSortingTransform."""

    @pytest.fixture
    def sample_table_mixed_columns(self) -> Table:
        """Create a sample table with mixed column types."""
        data = pl.DataFrame(
            {
                "numeric_col_b": [1, 2, 3],
                "numeric_col_a": [10, 20, 30],
                "string_col_b": ["a", "b", "c"],
                "datetime_col_b": pl.Series(
                    [
                        datetime(2023, 1, 1),
                        datetime(2023, 1, 2),
                        datetime(2023, 1, 3),
                    ],
                    dtype=pl.Datetime,
                ),
                "categorical_col_b": pl.Series(["a", "b", "c"], dtype=pl.Categorical),
                "categorical_col_a": pl.Series(["x", "y", "z"], dtype=pl.Categorical),
                "datetime_col_a": pl.Series(
                    [
                        datetime(2023, 1, 1),
                        datetime(2023, 1, 2),
                        datetime(2023, 1, 3),
                    ],
                    dtype=pl.Datetime,
                ),
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_default_column_sorting_transform(
        self, sample_table_mixed_columns: Table
    ):
        """Test the happy path for DefaultColumnSortingTransform."""
        transform = DefaultColumnSortingTransform()
        result = transform(sample_table_mixed_columns)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_mixed_columns.name

        # Verify columns are sorted in the expected order:
        # datetime_columns, dimension_columns (categorical/string), numeric_columns
        expected_order = [
            "datetime_col_a",  # datetime columns first
            "datetime_col_b",
            "categorical_col_a",  # then categorical columns
            "categorical_col_b",
            "string_col_b",
            "numeric_col_a",  # then numeric columns
            "numeric_col_b",
        ]
        assert result.data.columns == expected_order
