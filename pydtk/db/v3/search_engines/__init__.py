"""Search engines."""

import importlib
import logging
import os
from abc import ABCMeta
from typing import TypeVar

from .. import (
    DBHandler,
    MetaDBHandler,
    StatisticsCassandraDBHandler,
    StatisticsDBHandler,
    TimeSeriesCassandraDBHandler,
    TimeSeriesDBHandler,
)

T = TypeVar(
    "T",
    DBHandler,
    MetaDBHandler,
    TimeSeriesDBHandler,
    TimeSeriesCassandraDBHandler,
    StatisticsDBHandler,
    StatisticsCassandraDBHandler,
)

DB_SEARCH_ENGINES = {}  # key: db_handler class, value: search engine


def register_engines():
    """Register engines."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__))):
        if not os.path.isfile(os.path.join(os.path.dirname(__file__), filename)):
            continue
        if filename == "__init__.py":
            continue

        try:
            importlib.import_module(
                os.path.join(
                    "pydtk.db.v3.search_engines", os.path.splitext(filename)[0]
                ).replace(os.sep, ".")
            )
        except ModuleNotFoundError:
            logging.debug("Failed to load handlers in {}".format(filename))


def register_engine(db_handlers: [T]):
    """Register a DB-search-engine.

    Args:
        db_handlers ([T]): database handler class

    """

    def decorator(cls):
        for db_handler in db_handlers:
            if db_handler not in DB_SEARCH_ENGINES.keys():
                DB_SEARCH_ENGINES.update({db_handler: cls})
        return cls

    return decorator


@register_engine(db_handlers=[MetaDBHandler, TimeSeriesDBHandler, StatisticsDBHandler])
class BaseDBSearchEngine(metaclass=ABCMeta):
    """Base DB Search Engine class."""

    def __new__(cls, db_handler, **kwargs) -> object:
        """Create object.

        Args:
            db_handler (T): database handler
            **kwargs: DB-handler specific arguments

        Returns:
            (object): the corresponding handler object

        """
        if cls is BaseDBSearchEngine:
            engine = cls._get_search_engine(db_handler)
            return super(BaseDBSearchEngine, cls).__new__(engine)
        else:
            return super(BaseDBSearchEngine, cls).__new__(cls)

    @classmethod
    def _get_search_engine(cls, db_handler):
        """Returns an appropriate search engine.

        Args:
            db_handler (T): database handler

        Returns:
            (handler): database handler object

        """
        for _db_handler, engine in DB_SEARCH_ENGINES.items():
            if type(db_handler) is _db_handler:
                return engine

        raise ValueError("Unsupported handler: {}".format(db_handler))

    def __init__(self, db_handler: T):
        """Initialize BaseDBSearchEngine class.

        Args:
            db_handler (T): database handler

        """
        self._db_handler = db_handler
        self._conditions: [str] = []

    def add_condition(self, condition: str):
        """Add a search condition.

        Args:
            condition (str): search condition
                             (e.g. '"/vehicle/acceleration/accel_linear_x/mean" > 0')

        """
        if '"' not in condition:
            raise ValueError(
                "column name must be quoted "
                '(e.g. "/vehicle/acceleration/accel_linear_x/mean" > 0)'
            )
        self._conditions.append(condition)

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
        return 'select * from "{}"'.format(self._db_handler.df_name)

    @property
    def query(self):
        """Return query.

        Returns:
            (str): query

        """
        if self.condition == "":
            return self.select
        return "{0} where {1}".format(self.select, self.condition)


register_engines()
