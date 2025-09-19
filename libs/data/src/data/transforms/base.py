from __future__ import annotations

import logging
from abc import abstractmethod

import polars as pl
from data.models.tables import Table
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseTransform(BaseModel):
    """
    Base class for all transforms.
    """

    def __call__(self, table: Table, **kwargs) -> Table:
        """
        Make the transform callable with default logic.

        Insert other routine work here.
        """
        if table.data.is_empty():
            msg = "Cannot transform empty dataframe"
            logger.error(msg)
            raise ValueError(msg)

        # Apply the transform
        logger.info(
            f"Applying transform {self.__class__.__name__} to table {table.name}"
        )
        # Copy the table to avoid modifying the original in place
        result = self.apply(table=table.model_copy(deep=True), **kwargs)
        return result

    @abstractmethod
    def apply(self, table: Table, **kwargs) -> Table:
        """
        Apply the transform to the data.
        """
        raise NotImplementedError
