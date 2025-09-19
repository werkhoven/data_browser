"""
Unit tests for standard transforms.
"""

from datetime import datetime

import polars as pl
import pytest
from data.models.tables import Table, TableSource
from data.transforms.standard import (
    ConcentrationAnalysisTransform,
    FilterTransform,
    PivotTransform,
    QuantileLabelTransform,
    SumTransform,
    VerticalConcatenateTransform,
)


class TestQuantileLabelTransform:
    """Test cases for QuantileLabelTransform."""

    @pytest.fixture
    def sample_table_with_measures(self) -> Table:
        """Create a sample table with numeric measures."""
        data = pl.DataFrame(
            {
                "category": pl.Series(
                    ["A", "A", "B", "B", "C", "C"], dtype=pl.Categorical
                ),
                "region": pl.Series(
                    ["North", "South", "North", "South", "North", "South"],
                    dtype=pl.Categorical,
                ),
                "value": [100, 200, 300, 400, 500, 600],
                "revenue": [1000, 2000, 3000, 4000, 5000, 6000],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_quantile_label_transform(
        self, sample_table_with_measures: Table
    ):
        """Test the happy path for QuantileLabelTransform."""
        transform = QuantileLabelTransform(
            partition_by=["category"],
            labels=["Low", "Medium", "High"],
            breaks=[0.33, 0.67],
            column="value",
            alias="value_tier",
        )
        result = transform(sample_table_with_measures)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_with_measures.name

        # Verify the new column was added
        assert "value_tier" in result.data.columns
        assert result.data["value_tier"].dtype == pl.Categorical

        # Verify the original data is preserved
        assert result.data.shape[0] == sample_table_with_measures.data.shape[0]
        assert "value" in result.data.columns
        assert "category" in result.data.columns

        # Verify quantile labels are applied within each partition
        # The exact values depend on the data distribution
        unique_labels = set(result.data["value_tier"].unique())
        assert len(unique_labels) <= 3  # Should have at most 3 labels


class TestSumTransform:
    """Test cases for SumTransform."""

    @pytest.fixture
    def sample_table_for_aggregation(self) -> Table:
        """Create a sample table for aggregation testing."""
        data = pl.DataFrame(
            {
                "category": pl.Series(
                    ["A", "A", "B", "B", "C", "C"], dtype=pl.Categorical
                ),
                "region": pl.Series(
                    ["North", "South", "North", "South", "North", "South"],
                    dtype=pl.Categorical,
                ),
                "sales": [100, 200, 300, 400, 500, 600],
                "profit": [10, 20, 30, 40, 50, 60],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_sum_transform_with_group_by(
        self, sample_table_for_aggregation: Table
    ):
        """Test the happy path for SumTransform with group by."""
        transform = SumTransform(columns=["sales", "profit"], group_by=["category"])
        result = transform(sample_table_for_aggregation)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_for_aggregation.name

        # Verify grouping worked correctly
        assert result.data.shape[0] == 3  # 3 unique categories
        assert "category" in result.data.columns
        assert "sales" in result.data.columns
        assert "profit" in result.data.columns

        # Verify sums are correct
        expected_sales = {"A": 300, "B": 700, "C": 1100}
        expected_profit = {"A": 30, "B": 70, "C": 110}

        for row in result.data.iter_rows(named=True):
            assert row["sales"] == expected_sales[row["category"]]
            assert row["profit"] == expected_profit[row["category"]]

    def test_happy_path_sum_transform_without_group_by(
        self, sample_table_for_aggregation: Table
    ):
        """Test the happy path for SumTransform without group by."""
        transform = SumTransform(columns=["sales", "profit"], group_by=[])
        result = transform(sample_table_for_aggregation)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_for_aggregation.name

        # Verify single row result
        assert result.data.shape[0] == 1
        assert "sales" in result.data.columns
        assert "profit" in result.data.columns

        # Verify totals are correct
        assert result.data["sales"].item() == 2100  # Sum of all sales
        assert result.data["profit"].item() == 210  # Sum of all profit


class TestPivotTransform:
    """Test cases for PivotTransform."""

    @pytest.fixture
    def sample_table_for_pivot(self) -> Table:
        """Create a sample table for pivot testing."""
        data = pl.DataFrame(
            {
                "date": pl.Series(
                    [
                        datetime(2023, 1, 1),
                        datetime(2023, 1, 2),
                        datetime(2023, 1, 1),
                        datetime(2023, 1, 2),
                    ],
                    dtype=pl.Datetime,
                ),
                "category": pl.Series(["A", "A", "B", "B"], dtype=pl.Categorical),
                "region": pl.Series(
                    ["North", "South", "North", "South"], dtype=pl.Categorical
                ),
                "sales": [100, 200, 300, 400],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_pivot_transform(self, sample_table_for_pivot: Table):
        """Test the happy path for PivotTransform."""
        transform = PivotTransform(on=["category"], index=["region"], values=["sales"])
        result = transform(sample_table_for_pivot)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_for_pivot.name

        # Verify pivot structure
        assert "region" in result.data.columns
        assert "A" in result.data.columns  # Pivoted category
        assert "B" in result.data.columns  # Pivoted category

        # Verify we have the expected number of rows (unique regions)
        assert result.data.shape[0] == 2  # North and South

        # Verify data values
        north_row = result.data.filter(pl.col("region") == "North")
        south_row = result.data.filter(pl.col("region") == "South")

        assert north_row["A"].item() == 100
        assert north_row["B"].item() == 300
        assert south_row["A"].item() == 200
        assert south_row["B"].item() == 400


class TestFilterTransform:
    """Test cases for FilterTransform."""

    @pytest.fixture
    def sample_table_for_filter(self) -> Table:
        """Create a sample table for filter testing."""
        data = pl.DataFrame(
            {
                "category": ["A", "A", "B", "B", "C", "C"],
                "value": [100, 200, 300, 400, 500, 600],
                "active": [True, False, True, True, False, True],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_filter_transform(self, sample_table_for_filter: Table):
        """Test the happy path for FilterTransform."""
        transform = FilterTransform(column="category", values=["A", "B"])
        result = transform(sample_table_for_filter)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_for_filter.name

        # Verify filtering worked correctly
        assert result.data.shape[0] == 4  # Only A and B categories
        assert set(result.data["category"].unique()) == {"A", "B"}

        # Verify all rows have the filtered values
        for category in result.data["category"]:
            assert category in ["A", "B"]


class TestVerticalConcatenateTransform:
    """Test cases for VerticalConcatenateTransform."""

    @pytest.fixture
    def sample_tables_for_concatenation(self) -> tuple[Table, Table]:
        """Create sample tables for concatenation testing."""
        table1 = Table(
            name="table1",
            source=TableSource.CSV,
            data=pl.DataFrame(
                {"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]}
            ),
        )

        table2 = Table(
            name="table2",
            source=TableSource.CSV,
            data=pl.DataFrame(
                {"id": [3, 4], "name": ["Charlie", "David"], "value": [300, 400]}
            ),
        )

        return table1, table2

    def test_happy_path_vertical_concatenate_transform(
        self, sample_tables_for_concatenation: tuple[Table, Table]
    ):
        """Test the happy path for VerticalConcatenateTransform."""
        table1, table2 = sample_tables_for_concatenation

        transform = VerticalConcatenateTransform(other=table2)
        result = transform(table1)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == table1.name

        # Verify concatenation worked correctly
        assert result.data.shape[0] == 4  # 2 rows from each table
        assert result.data.shape[1] == 3  # Same number of columns

        # Verify all data is present
        expected_ids = [1, 2, 3, 4]
        expected_names = ["Alice", "Bob", "Charlie", "David"]
        expected_values = [100, 200, 300, 400]

        assert result.data["id"].to_list() == expected_ids
        assert result.data["name"].to_list() == expected_names
        assert result.data["value"].to_list() == expected_values


class TestConcentrationAnalysisTransform:
    """Test cases for ConcentrationAnalysisTransform."""

    @pytest.fixture
    def sample_table_for_concentration(self) -> Table:
        """Create a sample table for concentration analysis testing."""
        data = pl.DataFrame(
            {
                "category": pl.Series(
                    ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A"],
                    dtype=pl.Categorical,
                ),
                "region": pl.Series(
                    [
                        "North",
                        "South",
                        "East",
                        "North",
                        "South",
                        "East",
                        "North",
                        "South",
                        "East",
                        "North",
                    ],
                    dtype=pl.Categorical,
                ),
                "revenue": [
                    1000,
                    2000,
                    3000,
                    4000,
                    5000,
                    6000,
                    7000,
                    8000,
                    9000,
                    10000,
                ],
            }
        )
        return Table(name="test_table", source=TableSource.CSV, data=data)

    def test_happy_path_concentration_analysis_transform(
        self, sample_table_for_concentration: Table
    ):
        """Test the happy path for ConcentrationAnalysisTransform."""
        transform = ConcentrationAnalysisTransform(on="revenue", by=["category"])
        result = transform(sample_table_for_concentration)

        # Verify the result is a Table
        assert isinstance(result, Table)
        assert result.name == sample_table_for_concentration.name

        # Verify the concentration analysis structure
        assert "Concentration" in result.data.columns

        # Verify we have concentration labels
        concentration_labels = set(result.data["Concentration"].unique())
        expected_labels = {"Top 50%", "Top 20%", "Top 10%", "Total"}
        assert concentration_labels.issubset(expected_labels)

        # Verify the analysis was performed by category
        # The exact structure depends on the concentration analysis logic
        assert result.data.shape[0] > 0  # Should have some results

        revenue = sample_table_for_concentration.data["revenue"]
        top_10_percent = result.data.filter(pl.col("Concentration") == "Top 10%")
        assert top_10_percent[0, 1] == revenue[-1:].sum()

        top_20_percent = result.data.filter(pl.col("Concentration") == "Top 20%")
        assert top_20_percent[0, 1] == revenue[-2:].sum()

        top_50_percent = result.data.filter(pl.col("Concentration") == "Top 50%")
        assert top_50_percent[0, 1] == revenue[-5:].sum()

        total = result.data.filter(pl.col("Concentration") == "Total")
        assert total[0, 1] == revenue.sum()
