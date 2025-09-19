"""
Unit tests for base transform.
"""

import polars as pl
import pytest
from data.models.tables import Table, TableSource
from data.transforms.base import BaseTransform


class MockTransform(BaseTransform):
    """Mock transform for testing the base class."""

    def apply(self, table: Table, **kwargs) -> Table:
        """Mock apply method that adds a new column."""
        table.data = table.data.with_columns(
            pl.lit("transformed").alias("transform_result")
        )
        return table


class TestBaseTransform:
    """Test cases for BaseTransform."""

    @pytest.fixture
    def sample_table(self) -> Table:
        """Create a sample table for testing."""
        data = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100, 200, 300],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    @pytest.fixture
    def empty_table(self) -> Table:
        """Create an empty table for testing."""
        data = pl.DataFrame({"id": [], "name": [], "value": []})
        return Table(name="empty_table", source=TableSource.CSV, data=data)

    def test_happy_path_base_transform_call(self, sample_table: Table):
        """Test the happy path for BaseTransform.__call__."""
        transform = MockTransform()
        result = transform(sample_table)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table.name

        # Verify the original table was not modified (deep copy)
        assert "transform_result" not in sample_table.data.columns
        assert sample_table.data.shape == (3, 3)

        # Verify the transform was applied
        assert "transform_result" in result.data.columns
        assert result.data.shape == (3, 4)  # Original 3 columns + 1 new column

        # Verify the transform result column has expected values
        assert result.data["transform_result"].to_list() == [
            "transformed",
            "transformed",
            "transformed",
        ]

    def test_base_transform_with_empty_table_raises_error(self, empty_table: Table):
        """Test that BaseTransform raises error with empty table."""
        transform = MockTransform()

        with pytest.raises(ValueError, match="Cannot transform empty dataframe"):
            transform(empty_table)

    def test_base_transform_preserves_original_table(self, sample_table: Table):
        """Test that BaseTransform preserves the original table."""
        original_data = sample_table.data.clone()
        original_shape = sample_table.data.shape
        original_columns = sample_table.data.columns.copy()

        transform = MockTransform()
        result = transform(sample_table)

        # Verify original table is unchanged
        assert sample_table.data.equals(original_data)
        assert sample_table.data.shape == original_shape
        assert sample_table.data.columns == original_columns

        # Verify result is different from original
        assert not result.data.equals(original_data)
        assert result.data.shape != original_shape
        assert result.data.columns != original_columns
