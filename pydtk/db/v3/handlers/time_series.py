#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Toolkit Auhtors


import os
import re

import pandas as pd
from sqlalchemy import sql
from sqlalchemy.types import VARCHAR

from pydtk.utils.utils import load_config

from . import BaseDBHandler as _BaseDBHandler
from . import register_handler

_extra_supports = {"cassandra": True}
try:
    import pandra as cql
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.policies import WhiteListRoundRobinPolicy
except ImportError:
    _extra_supports["cassandra"] = False


def _replace_into(table, conn, keys, data_iter):
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.sql.expression import Insert

    @compiles(Insert)
    def replace_string(insert, compiler, **kw):
        s = compiler.visit_insert(insert, **kw)
        s = s.replace("INSERT INTO", "INSERT OR REPLACE INTO")
        return s

    data = [dict(zip(keys, row)) for row in data_iter]

    conn.execute(table.table.insert(replace_string=""), data)


@register_handler(db_classes=["time_series"], db_engines=["sqlite", "mysql"])
class TimeSeriesDBHandler(_BaseDBHandler):
    """Time series database handler."""

    __version__ = "v3"
    db_defaults = load_config(__version__).sql.time_series
    _df_class = "time_series_df"
    _df_name = "time_series_df"
    _columns = None

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
            else os.environ.get("PYDTK_TIME_SERIES_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)

    def save(self, df=None, remove_duplicates=False, **kwargs):
        """Save data to SQL.

        Args:
            df (pandas.DataFrame): DataFrame to save (if None, self.df will be saved)
            remove_duplicates (bool): if True, duplicated rows will be removed
            **kwargs: kwargs for function `pandas.dataframe.to_sql`

        """
        dataframe = df.copy() if df is not None else self.df.copy()
        dataframe = dataframe[~dataframe.index.duplicated(keep="last")]

        if "index" not in kwargs.keys():
            kwargs.update({"index": True})
        self._prepare_columns()

        # TODO(hdl-members): alter column 'uuid_in_df' as primary key or wait for the PR below
        # TODO(hdl-members): Use `if_exists=upsert_overwrite` after the following PR is merged
        # https://github.com/pandas-dev/pandas/pull/29636
        dataframe.to_sql(
            self.df_name,
            self._engine,
            if_exists="append",
            dtype={"uuid_in_df": VARCHAR(32)},
            method=_replace_into,
            **kwargs
        )


@register_handler(db_classes=["time_series"], db_engines=["cassandra"])
class TimeSeriesCassandraDBHandler(TimeSeriesDBHandler):
    """Handler for Time series database using Apache Cassandra as backend."""

    def __init__(self, *args, **kwargs):
        """Initialize Handler.

        Args:
            *args: args for super().__init__()
            **kwargs: kwargs for super().__init__()

        """
        super().__init__(*args, **kwargs)

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        if not _extra_supports["cassandra"]:
            raise ImportError(
                "Cassandra-related modules could not be loaded.",
                "Make sure you installed extra modules",
            )

        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_TIME_SERIES_DB_DATABASE", None)
        )

        # Load default settings from config file
        engine = engine if engine is not None else self.db_defaults.engine
        username = username if username is not None else self.db_defaults.username
        password = password if password is not None else self.db_defaults.password
        host = host if host is not None else self.db_defaults.host
        database = database if database is not None else self.db_defaults.database

        assert engine == "cassandra"

        # Substitute
        self._config.current_db = {
            "engine": engine,
            "host": host,
            "username": username,
            "password": password,
            "database": database,
        }

        # Connect
        hostname = host
        hostport = 9042
        if ":" in host:
            hostname, hostport = host.split(":")

        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(
            contact_points=[hostname],
            port=int(hostport),
            connect_timeout=int(os.environ.get("PYDTK_CASSANDRA_CONNECT_TIMEOUT", 60)),
            auth_provider=auth_provider,
            load_balancing_policy=WhiteListRoundRobinPolicy([hostname]),
        )

        # Define pandas factory function
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        session = cluster.connect(database)
        session.row_factory = pandas_factory
        session.default_fetch_size = None

        self._engine = cluster
        self._session = session

    def read(self, df_name=None, query=None, where=None, order_by=None, **kwargs):
        """Read data from SQL.

        Args:
            df_name (str): Dataframe name to read
            query (str SQL query or SQLAlchemy Selectable): query to select items
            where (str): query string for filtering items
            order_by (srt): column name to sort by
            **kwargs: kwargs for function `pandas.read_sql_query`
                      or `influxdb.DataFrameClient.query`

        Returns:
            (pandas.df): table

        """
        if df_name is not None:
            self.df_name = df_name

        # Create a query
        q = query
        if q is None:
            # Check if the table exits on DB
            temp = self._session.execute(
                "select table_name from system_schema.tables "
                "where keyspace_name = '{0}' and table_name = '{1}'".format(
                    self._config.current_db["database"], self._df_name
                )
            )
            if len(temp._current_rows) == 0:
                self.df = self._initialize_df()
                self.logger.warning('Table "{0}" not found on DB'.format(str(df_name)))
                return

            # Create a sub-query for extracting unique records
            sub_q = sql.select("*", from_obj=sql.table(self._df_name))

            # Create a query
            q = sub_q
            if where is not None:
                q = q.where(sql.text(where))
            if order_by is not None:
                q = q.order_by(sql.text(order_by))

        # Read table from DB
        try:
            res = self._session.execute(str(q), timeout=None)
            df = res._current_rows
        except Exception as e:
            df = self._initialize_df()
            self.logger.warning(
                'Could not execute SQL statement: "{0}" (reason: {1})'.format(
                    str(q), str(e)
                )
            )

        self.df = df

    def save(self, df=None, **kwargs):
        """Save data to SQL.

        Args:
            df (pandas.DataFrame): dataframe to save. if None, self.df will be saved

        """
        dataframe: pd.DataFrame = df if df is not None else self.df

        # Convert to CassandraDataFrame
        cql_df = cql.CassandraDataFrame(dataframe)

        # Get column types
        primary_key = ["timestamp"]
        if "record_id" in [c["name"] for c in self.columns]:
            primary_key = ["record_id"] + primary_key
        column_types = cql_df._infer_data_type_from_dtype(primary_key)
        for value in column_types.values():
            value.column_type = (
                "double" if value.column_type == "float" else value.column_type
            )
            value.column_type = (
                "double" if value.column_type == "int" else value.column_type
            )
            value.name = '"{}"'.format(value.name)

        # Prepare columns
        self._prepare_columns(column_types)

        # Write to DB
        cql_df.to_cassandra(
            cassandra_session=self._session,
            table_name='"{}"'.format(self._df_name),
            data_types=column_types,
            create_table=True,
        )

    def _prepare_columns(self, column_types):
        """Add missing column to the table on DB.

        Args:
            column_types (list of dict): list of column name and type

        """
        # Check if the table exits on DB
        temp = self._session.execute(
            "select table_name from system_schema.tables "
            "where keyspace_name = '{0}' and table_name = '{1}'".format(
                self._config.current_db["database"], self._df_name
            )
        )
        if len(temp._current_rows) == 0:
            return

        # Get columns in the table on DB
        db_table_info = self._session.execute(
            "select column_name from system_schema.columns "
            "where keyspace_name = '{0}' "
            " and table_name = '{1}' allow filtering".format(
                self._config.current_db["database"], self._df_name
            )
        )._current_rows.to_dict("list")

        # Calculate difference
        difference = set(column_types.keys()).difference(db_table_info["column_name"])

        # Add columns
        for new_column in difference:
            self._session.execute(
                'ALTER TABLE {keyspace_name}.{table_name} ADD "{column_name}" {dtype};'.format(
                    keyspace_name=self._config.current_db["database"],
                    table_name=self._df_name,
                    column_name=new_column,
                    dtype=column_types[new_column].column_type,
                )
            )

    @property
    def df_name(self):
        """Return df_name."""
        value = self._df_name
        return value

    @df_name.setter
    def df_name(self, value):
        """Setter for self.df_name."""
        if not re.fullmatch("[a-zA-Z_0-9]+", value):
            raise ValueError(
                "Invalid df name: {}  "
                "df name can only contain alphabet, number and underscore"
            )
        self._df_name = value
