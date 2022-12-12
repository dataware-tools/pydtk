#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Toolkit Authors

import hashlib
import os

from . import register_handler
from .time_series import TimeSeriesCassandraDBHandler, TimeSeriesDBHandler


@register_handler(db_classes=["statistics"], db_engines=["sqlite", "mysql", "mariadb"])
class StatisticsDBHandler(TimeSeriesDBHandler):
    """DB Handler for statistics data."""

    _database_id: str = ""
    _span = None
    _df_class = "statistics_df"
    _df_name = "statistics_df"

    def __init__(self, database_id: str, span: float, **kwargs):
        """Initialize StatisticsDBHandler.

        Args:
            database_id (str): ID of the database
            span (float): interval of statistic values

        """
        self._database_id = database_id
        self._span = span
        super(StatisticsDBHandler, self).__init__(**kwargs)

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_STATISTICS_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_STATISTICS_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_STATISTICS_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_STATISTICS_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_STATISTICS_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)

    @property
    def _df_name(self):
        """Return _df_name."""
        template = self._config.statistics_df.df_name
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode("utf-8"), digest_size=self._config.hash.digest_size
        ).hexdigest()
        return template.format(
            **{"database_id": database_id_hashed, "span": self._span}
        )

    @_df_name.setter
    def _df_name(self, value):
        """Setter for self._df_name."""
        raise RuntimeError("Setting df_name is not supported in StatisticsDBHandler")


@register_handler(db_classes=["statistics"], db_engines=["cassandra"])
class StatisticsCassandraDBHandler(TimeSeriesCassandraDBHandler):
    """DB Handler for statistics data using Apache Cassandra."""

    _database_id: str = ""
    _span: float = None
    _df_class = "statistics_df"
    _df_name = "statistics_df"

    def __init__(self, database_id: str, span: float, **kwargs):
        """Initialize StatisticsDBHandler.

        Args:
            database_id (str): ID of the database
            span (float): interval of statistic values

        """
        self._database_id = database_id
        self._span = span
        super(StatisticsCassandraDBHandler, self).__init__(**kwargs)

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_STATISTICS_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_STATISTICS_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_STATISTICS_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_STATISTICS_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_STATISTICS_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)

    @property
    def _df_name(self):
        """Return _df_name."""
        template = self._config[self._df_class]["df_name"]
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode("utf-8"), digest_size=self._config.hash.digest_size
        ).hexdigest()
        return template.format(
            **{"database_id": database_id_hashed, "span": self._span}
        )

    @_df_name.setter
    def _df_name(self, value):
        """Setter for self._df_name."""
        raise RuntimeError("Setting df_name is not supported in StatisticsDBHandler")
