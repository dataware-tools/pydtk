"""Search engines."""

from abc import ABCMeta
from typing import TypeVar

from .. import BaseDBHandler, MetaDBHandler, TimeSeriesDBHandler

T = TypeVar("T", BaseDBHandler, MetaDBHandler, TimeSeriesDBHandler)


class BaseDBSearchEngine(metaclass=ABCMeta):
    """Base DB Search Engine class."""

    def __init__(self, db_handler: T):
        """Initialize BaseDBSearchEngine class.

        Args:
            db_handler (T): database handler

        """
        self._db_handler = db_handler
        self._conditions: [str] = []

    def search(self):
        """Execute SQL query in database handler.

        Returns:
            (pandas.DataFrame): search result

        """
        self._db_handler.read(query=self.query)
        return self._db_handler.df

    def clear(self):
        """Clean search conditions."""
        self._conditions = []

    @property
    def condition(self):
        """Return search condition.

        Returns:
            (str): search condition as string

        """
        return " and ".join(self._conditions)

    @property
    def select(self):
        """Return select query.

        Returns:
            (str): query

        """
        return "select * from {}".format(self._db_handler.df_name)

    @property
    def query(self):
        """Return query.

        Returns:
            (str): query

        """
        if self.condition == "":
            return self.select
        return "{0} where {1}".format(self.select, self.condition)
