"""
AI agents for data parsing and type inference.
"""

from dataclasses import dataclass

import polars as pl
from data.models.schemas import ColumnSchema
from data.models.tables import Table, TableSource
from data.transforms.formatting import ColumnSchemaTransform
from pydantic_ai import Agent, ModelRetry, RunContext


@dataclass
class EngineDeps:
    """Dependencies for the query generator."""

    frame: pl.DataFrame


# Create the AI agent for data type inference
datatype_parser: Agent[EngineDeps, list[ColumnSchema]] = Agent(
    "gpt-4o",
    name="datatype_parser",
    deps_type=EngineDeps,
    output_type=list[ColumnSchema],
    retries=3,
)


@datatype_parser.system_prompt
async def system_prompt(ctx: RunContext[EngineDeps]) -> str:
    """Parse the data types for the given data."""
    return f"""
    You are an expert data analyst specializing in data type inference and schema detection. 

    Your task is to analyze sample data and infer the most appropriate data type for each column
    as well as the regex cleaning pattern that will be used to strip out any remove any problematic
    characters before casting to the data type.
    
    Consider the following guidelines:

    **Data Types:**
    1. **String Type**: Use for text data, IDs, categories, or any non-numeric data
    2. **DateTime Type**: Use for temporal data formatted as either a complete datetime or datetime part (e.g. year, month, day, hour, minute, second)
    3. **Integer Type**: Use for whole numbers (excluding datetime parts such as year, month, day, hour, minute, second)
    4. **Float Type**: Use for decimal numbers, currency values, percentages, measurements
    5. **Boolean Type**: Use for true/false, yes/no, 1/0, or binary categorical data

    **Regex Cleaning Patterns:**
    1. You must not use any lookahead, lookbehind, or negative lookbehind in the regex cleaning pattern.
    2. You should format the expression as a negative character class in the format [^pattern] where
    pattern defines the characters that should remain after cleaning. Example: [^0-9.-] to match
    any character that is not a number, decimal point, or hyphen (e.g. -100.00) for float data.

    **Important Considerations:**
    - Look for patterns in the data (currency symbols, date formats, etc.)
    - Consider the overall context of the data such and the names of the columns and range of values.
    - Do not label whole number columns integers if they appear to represent dates or datetime parts (e.g. "day" column with values 1-31).
    - Do not forget to specify the partial_datetime_schema for columns that represent individual parts
    - Preserve ID fields as strings even if they contain numbers
    - Be conservative - when in doubt, choose STRING over other types
    - Consider the distribution of values and their uniqueness
    - Do not retain commas when parsing numeric data.

    **Datetime Schema:**
    - Define a datetime_format for the column if it represents temporal data (even if it is only one part of the datetime).
    - Date time format will be used to parse the column into a datetime type.
    - Define a partial_datetime_schema for columns that represent individual parts of a date or datetime (even if it is an integer or float).
    - Fuse multiple columns into a single datetime column if they represent the same temporal data by assigning them a shared
    parent_column_name (e.g. "date_of_birth") and parts (e.g. year, month, day).

    **Data Quality Issues to Note:**
    - Missing values or null representations
    - Inconsistent formatting
    - Outliers or unusual values
    - Mixed data types within a column
    - Encoding issues or special characters

    **Schema Testing:**
    - Check that the data is parseable according to the schema before returning the schema.
    
    Here is a sample of the data:
    {ctx.deps.frame.sample(100).to_dicts()}
    """


@datatype_parser.tool
async def is_data_parseable(
    ctx: RunContext[EngineDeps], schema: list[ColumnSchema]
) -> bool:
    """Return True if the data is parseable according to the schema."""
    for col in schema:
        try:
            transform = ColumnSchemaTransform(column_schemas=[col])
            transform(
                table=Table(
                    name="data",
                    source=TableSource.OTHER,
                    data=ctx.deps.frame,
                )
            )
        except Exception as e:
            raise ModelRetry(f"Error parsing column {col.name}: {e}") from e

    return True
