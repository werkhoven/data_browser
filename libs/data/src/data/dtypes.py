from enum import StrEnum

from pydantic import BaseModel


# class DataTypeCleaningPattern(StrEnum):
#     INTEGER = r"[^0-9\-,]"
#     FLOAT = r"[^0-9.\-,]"
#     BOOLEAN = r"[^(true|false|yes|no|1|0)]"
#     DATETIME = r"[^0-9\-/:T\s]"
#     STRING = r"a^"  #


# class DataType(BaseModel):
#     name: str
#     matching_pattern: str
#     matching_threshold: float


# # Data types are ordered by parsing priority (will match the first one that matches)
# data_types = [
#     DataType(
#         name=DataTypeEnum.INTEGER,
#         matching_pattern=DataTypeCleaningPattern.INTEGER,
#         matching_threshold=0.85,
#     ),
#     DataType(
#         name=DataTypeEnum.FLOAT,
#         matching_pattern=DataTypeCleaningPattern.FLOAT,
#         matching_threshold=0.85,
#     ),
#     DataType(
#         name=DataTypeEnum.DATETIME,
#         matching_pattern=DataTypeCleaningPattern.DATETIME,
#         matching_threshold=0.85,
#     ),
#     DataType(
#         name=DataTypeEnum.STRING,
#         matching_pattern=DataTypeCleaningPattern.STRING,
#         matching_threshold=0.85,
#     ),
# ]
