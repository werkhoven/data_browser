from enum import StrEnum

from pydantic import BaseModel, Field


class DataTypeEnum(StrEnum):
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    STRING = "string"


class DatetimePart(StrEnum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


class PartialDatetimeSchema(BaseModel):
    part: DatetimePart = Field(
        description="The part of the datetime column this column represents."
    )
    parent_column_name: str = Field(
        default="Date",
        description="The name of the composite datetime column this will be used to create.",
    )


class ColumnSchema(BaseModel):
    name: str = Field(description="The name of the column.")
    data_type: DataTypeEnum = Field(description="The data type of the column.")
    regex_cleaning_pattern: str = Field(
        description="The regex cleaning pattern for the column."
    )
    datetime_format: str | None = Field(
        default=None,
        description="The datetime format of the column (e.g. %Y-%m-%d %H:%M:%S).",
    )
    partial_datetime_schema: PartialDatetimeSchema | None = Field(
        default=None,
        description="""The partial datetime schema for the column if it represents
            an individual part of a datetime (e.g. year, month, day, hour, minute, second).
            Used to group multiple columns into a single datetime column.
            """,
    )
