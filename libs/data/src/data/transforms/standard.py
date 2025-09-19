from __future__ import annotations

from typing import Any, override

import polars as pl
from data.models.tables import Table
from data.transforms.base import BaseTransform
from pydantic import Field, ValidationError, model_validator
from pydantic.functional_validators import field_validator


class QuantileLabelTransform(BaseTransform):
    """
    Transform that applies a column schema to the data.
    """

    partition_by: list[str] = Field(description="The columns to partition the data by.")
    labels: list[str] = Field(
        description="The labels to apply to the resulting quantiles."
    )
    breaks: list[float] = Field(
        description="The quantile breaks to use to split the data."
    )
    column: str = Field(
        description="The name of the column to apply the quantile labels to."
    )
    alias: str = Field(description="The alias to apply to the resulting column.")

    @model_validator(mode="after")
    def validate_breaks(self):
        if len(self.breaks) != len(self.labels) - 1:
            raise ValidationError(
                "The number of breaks must be one less than the number of quantile labels."
            )
        for _break in self.breaks:
            if not 0 <= _break <= 1:
                raise ValidationError("Quantile breaks must be between 0 and 1.")
        return self

    @override
    def apply(self, table: Table) -> Table:
        """Apply the quantile labels to the data."""
        table.validate_measures([self.column])
        table.validate_dimensions(self.partition_by)

        expr = (
            pl.col(self.column)
            .qcut(self.breaks, labels=self.labels)
            .cast(pl.Categorical)
        )
        if self.partition_by:
            if set(self.partition_by) - set(table.dimension_columns):
                raise ValueError(
                    f"Partition by columns {set(self.partition_by) - set(table.dimension_columns)} not table dimensions."
                )
            expr = expr.over(self.partition_by)
        table.data = table.data.with_columns(expr.alias(self.alias))
        return table


class SumTransform(BaseTransform):
    """
    Transform that applies a column schema to the data.
    """

    columns: list[str] = Field(description="The columns to aggregate.")
    group_by: list[str] = Field(
        default_factory=list, description="The columns to group the data by."
    )

    @override
    def apply(self, table: Table) -> Table:
        """Apply the quantile labels to the data."""
        table.validate_measures(self.columns)
        table.validate_dimensions(self.group_by)
        exprs = [pl.sum(column) for column in self.columns]
        if self.group_by:
            table.data = table.data.group_by(self.group_by).agg(exprs)
        else:
            table.data = table.data.select(exprs)
        return table


class PivotTransform(BaseTransform):
    """
    Transform that pivots the data.
    """

    on: list[str] = Field(description="The columns to pivot the data by.")
    index: list[str] = Field(description="The columns to use as the index.")
    values: list[str] = Field(description="The column to use as the values.")

    @override
    def apply(self, table: Table) -> Table:
        """Pivot the data."""
        table.validate_dimensions([*self.index, *self.on])
        table.validate_measures(self.values)

        # Shorten datetime format if it's being pivoted on since it will end up in the column headers
        for col in self.on:
            if col in table.datetime_columns:
                table.data = table.data.with_columns(
                    pl.col(col).dt.strftime("%Y-%m-%d")
                )

        table.data = table.data.pivot(index=self.index, on=self.on, values=self.values)
        return table


class FilterTransform(BaseTransform):
    """
    Transform that filters the data.
    """

    column: str = Field(description="The column to filter the data by.")
    values: list[Any] = Field(description="The value to filter the data by.")

    @override
    def apply(self, table: Table) -> Table:
        """Filter the data."""
        table.data = table.data.filter(pl.col(self.column).is_in(self.values))
        return table


class VerticalConcatenateTransform(BaseTransform):
    """
    Transform that concatenates the data.
    """

    other: Table = Field(description="The other table to concatenate.")

    @override
    def apply(self, table: Table) -> Table:
        """Concatenate the data."""
        table.data = pl.concat([table.data, self.other.data], how="diagonal_relaxed")
        return table


class ConcentrationAnalysisTransform(BaseTransform):
    """
    Transform that performs a concentration analysis.
    """

    on: str = Field(description="The column to perform the concentration analysis on.")
    by: list[str] = Field(description="The columns to partition the data by.")

    @override
    def apply(
        self,
        table: Table,
        labels: list[str] = ["Top 50%", "Top 20%", "Top 10%"],
        breaks: list[float] = [0.5, 0.8, 0.9],
        output_column: str = "Concentration",
    ) -> Table:
        """Perform the concentration analysis."""
        table.validate_measures([self.on])
        table.validate_dimensions(self.by)

        tmp_tables: list[Table] = []
        for _break, label in zip(breaks, labels):
            tmp_table = table.model_copy(deep=True)
            transforms = [
                QuantileLabelTransform(
                    partition_by=self.by,
                    labels=["Below", label],
                    breaks=[_break],
                    column=self.on,
                    alias=output_column,
                ),
                FilterTransform(column=output_column, values=[label]),
                SumTransform(group_by=[*self.by, output_column], columns=[self.on]),
                PivotTransform(on=self.by, index=[output_column], values=[self.on]),
            ]
            for transform in transforms:
                tmp_table = transform(tmp_table)
            tmp_tables.append(tmp_table)

        if not tmp_tables:
            return table

        # Append sum total row
        transform = SumTransform(group_by=[*self.by], columns=[self.on])
        sum_total = transform(table)
        sum_total.data = sum_total.data.with_columns(
            pl.lit("Total").alias(output_column)
        )
        transform = PivotTransform(on=self.by, index=[output_column], values=[self.on])
        sum_total = transform(sum_total)
        tmp_tables.append(sum_total)

        # Concatenate all tables
        table = tmp_tables[0]
        for tmp_table in tmp_tables[1:]:
            transform = VerticalConcatenateTransform(
                other=tmp_table.model_copy(deep=True)
            )
            table = transform(table)

        table.data = table.data.sort(output_column, descending=False)
        return table
