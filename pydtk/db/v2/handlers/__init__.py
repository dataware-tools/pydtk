#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""V2DBHandler."""

import hashlib
import logging
from copy import deepcopy
from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy
from addict import Dict as AttrDict
from sqlalchemy import sql
from sqlalchemy.orm import scoped_session, sessionmaker
from tqdm import tqdm

from pydtk.utils.utils import (
    deserialize_dict_1d,
    dicts_to_listed_dict_2d,
    dtype_string_to_dtype_object,
    load_config,
    serialize_dict_1d,
)

_extra_supports = {"influxdb": True}
try:
    from influxdb import DataFrameClient
except ImportError:
    _extra_supports["influxdb"] = False


class BaseDBHandler(object):
    """Handler for db."""

    __version__ = "v2"
    db_defaults = load_config(__version__).sql.base
    _config = AttrDict()
    _df_name = "base_df"
    _columns = None

    @classmethod
    def default_config(cls):
        """Return default configurations.

        Returns:
            (AttrDict): default configurations

        """
        return load_config(cls.__version__)

    def __init__(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
        df_name=None,
        read_on_init=True,
    ):
        """Initialize BaseDBHandler.

        Args:
            db_engine (str): database engine (if None, the one in the config file will be used)
            db_host (str): database HOST (if None, the one in the config file will be used)
            db_name (str): database name (if None, the one in the config file will be used)
            db_username (str): username (if None, the one in the config file will be used)
            db_password (str): password (if None, the one in the config file will be used)
            df_name (str): dataframe (table in DB) name (if None, class default value will be used)
            read_on_init (bool): if True, dataframe will be read from database on initialization

        """
        self._cursor = 0
        self.logger = logging.getLogger(__name__)
        if df_name is not None:
            self.df_name = df_name

        # Load config
        self._config = load_config(self.__version__)

        # Initialize database
        self._initialize_engine(db_engine, db_host, db_name, db_username, db_password)

        # Initialize dataframe
        self.df = self._initialize_df()

        # Fetch table
        if read_on_init:
            self.read()

    def __iter__(self):
        """Return iterator."""
        return self

    def __next__(self):
        """Return the next item."""
        if self._cursor >= len(self.df):
            self._cursor = 0
            raise StopIteration()

        # Grab data
        data = self.df.take([self._cursor]).to_dict(orient="records")[0]

        # Delete internal column
        if "uuid_in_df" in data.keys():
            del data["uuid_in_df"]

        # Post-processes
        data = deserialize_dict_1d(data)

        # Increment
        self._cursor += 1

        return data

    def __len__(self):
        """Return number of records."""
        return len(self.df)

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        # Parse
        engine = db_engine if db_engine is not None else self.db_defaults.engine
        username = db_username if db_username is not None else self.db_defaults.username
        password = db_password if db_password is not None else self.db_defaults.password
        host = db_host if db_host is not None else self.db_defaults.host
        database = db_name if db_name is not None else self.db_defaults.database

        # Substitute
        self._config.current_db = {
            "engine": engine,
            "host": host,
            "username": username,
            "password": password,
            "database": database,
        }

        # Connect
        if engine in ["influxdb"]:
            if not _extra_supports["influxdb"]:
                raise ImportError("Module `influxdb` cannot be not imported")
            hostname = host
            hostport = 8086
            if ":" in host:
                hostname, hostport = host.split(":")
            self._engine = DataFrameClient(
                hostname, hostport, username, password, database
            )
        else:
            engine = (
                "postgresql"
                if engine == "timescaledb"
                else "mysql"
                if engine == "mariadb"
                else engine
            )
            username_and_password = (
                ""
                if all([username == "", password == ""])
                else "{0}:{1}@".format(username, password)
            )
            self._engine = sqlalchemy.create_engine(
                "{0}://{1}{2}{3}".format(
                    engine,
                    username_and_password if engine != "sqlite" else "",
                    "/{0}".format(host) if engine == "sqlite" else host,
                    "" if engine == "sqlite" else "/" + database,
                ),
                echo=False,
            )
            self._session = scoped_session(sessionmaker(bind=self._engine))

    def _initialize_df(self):
        """Initialize DF."""
        df = pd.concat(
            [
                pd.Series(
                    name=c["name"], dtype=dtype_string_to_dtype_object(c["dtype"])
                )
                for c in self.columns
            ]
            + [pd.Series(name="uuid_in_df", dtype=str)],
            axis=1,
        )
        return df

    def _get_column_names(self):
        """Return column names."""
        return [c["name"] for c in self.columns]

    def _get_sql_columns(self):
        return [sql.column(c) for c in self._get_column_names()]

    def _get_uuid_from_item(self, data_in):
        """Return UUID of the given item.

        Args:
            data_in (dict or pandas.Series): dict or Series containing data

        Returns:
            (str): UUID

        """
        item = data_in
        if isinstance(item, pd.Series):
            item = data_in.to_dict()

        pre_hash = "".join(
            [
                "{:.09f}".format(item[c["name"]])
                if isinstance(item[c["name"]], float)
                else str(item[c["name"]])
                for c in self.columns
                if c["name"] in item.keys()
            ]
        )
        pre_hash = pre_hash.encode("utf-8")
        uuid = hashlib.md5(pre_hash).hexdigest()
        return uuid

    def _preprocess_list_of_dicts(self, data_in):
        """Preprocess list of dicts.

        Args:
            data_in (list): list of dicts containing data

        Returns:
            (dict): dict of lists = listed dict

        """
        data = deepcopy(data_in)

        # Serialize (convert list to str)
        self.logger.info("(Preprocess) Serializing...")
        for item in tqdm(data, desc="Serialization", leave=False):
            item = serialize_dict_1d(item)

            # Add df_uuid
            item["uuid_in_df"] = self._get_uuid_from_item(item)

            # Add missing columns
            for column in self.columns:
                column_name, column_dtype = column["name"], column["dtype"]
                if column_name not in item.keys():
                    item.update({column_name: None})
                else:
                    dtype_obj = dtype_string_to_dtype_object(column_dtype)
                    if dtype_obj is None:
                        continue
                    if item[column_name] is not None and not isinstance(
                        item[column_name], dtype_obj
                    ):
                        try:
                            item[column_name] = dtype_obj(item[column_name])
                        except ValueError:
                            item[column_name] = np.nan

        # Convert dict to listed dict
        self.logger.info("(Preprocess) Converting...")
        data = dicts_to_listed_dict_2d(data)

        return data

    def _append_listed_dict_to_df(self, data, check_unique=False):
        """Append pre-processed dict to self._df.

        Args:
            data (dict): data to add
            check_unique (bool): if True, it will be checked that the data is unique in the db

        """
        self.df = pd.concat([self.df, pd.DataFrame.from_dict(data)], sort=False)
        if check_unique:
            self.df.drop_duplicates("uuid_in_df", inplace=True)

    def add_data(self, data_in, **kwargs):
        """Add data to db.

        Args:
            data_in (dict): a dict containing data

        """
        self.add_list_of_data([data_in], **kwargs)

    def add_list_of_data(self, data_in, **kwargs):
        """Add list of data to db.

        Args:
            data_in (list): a list of dicts containing data

        """
        data = self._preprocess_list_of_dicts(data_in)
        self.logger.info("Adding data to DB...")
        self._append_listed_dict_to_df(data, **kwargs)
        self.logger.info("Successfully finished adding data to DB")

    def read(self, df_name=None, query=None, where=None, order_by=None, **kwargs):
        """Read data from SQL.

        Args:
            df_name (str): Dataframe name to read
            query (str SQL query or SQLAlchemy Selectable): query to select items
            where (str): query string for filtering items
            order_by (srt): column name to sort by
            **kwargs: kwargs for function `pandas.read_sql_query`
                      or `influxdb.DataFrameClient.query`

        """
        if df_name is not None:
            self.df_name = df_name

        # Create a query
        q = query
        if q is None:
            # Check if the table exits on DB
            if self._config.current_db["engine"] in ["influxdb"]:
                if self.df_name not in [
                    entry["name"] for entry in self._engine.get_list_measurements()
                ]:
                    self.df = self._initialize_df()
                    return
            else:
                if not self._engine.dialect.has_table(self._engine, self.df_name):
                    self.df = self._initialize_df()
                    return

            # Create a sub-query for extracting unique records
            sub_q = sql.select("*", from_obj=sql.table(self.df_name))
            if self._config.current_db["engine"] in ["mysql", "sqlite"]:
                sub_q = sub_q.group_by(sql.column("uuid_in_df"))
            elif self._config.current_db["engine"] in ["postgresql", "timescaledb"]:
                sub_q = sub_q.distinct(sql.column("uuid_in_df"))

            # Create a query
            if self._config.current_db["engine"] in ["influxdb"]:
                q = sub_q
            else:
                q = sql.select("*", from_obj=sub_q.alias("temp"))
            if where is not None:
                q = q.where(sql.text(where))
            if order_by is not None:
                q = q.order_by(sql.text(order_by))

        # Read table from DB
        try:
            if self._config.current_db["engine"] in ["influxdb"]:
                df = list(self._engine.query(str(q), **kwargs).values())[0]
            else:
                df = pd.read_sql_query(q, self._engine, **kwargs)
        except Exception as e:
            self.logger.warning(
                'Could not execute SQL statement: "{0}" (reason: {1})'.format(
                    str(q), str(e)
                )
            )
            df = self._initialize_df()

        self.df = df

    def save(self, df=None, remove_duplicates=False, **kwargs):
        """Save data to SQL.

        Args:
            df (pandas.DataFrame): DataFrame to save (if None, self.df will be saved)
            remove_duplicates (bool): if True, duplicated rows will be removed
            **kwargs: kwargs for function `pandas.dataframe.to_sql`
                      or `influxdb.DataFrameClient.write_points`

        """
        dataframe = df if df is not None else self.df
        dataframe.drop_duplicates("uuid_in_df", inplace=True)

        if self._config.current_db["engine"] in ["influxdb"]:
            self._engine.write_points(dataframe, self.df_name, **kwargs)
        else:
            if "index" not in kwargs.keys():
                kwargs.update({"index": False})
            dataframe.to_sql(self.df_name, self._engine, if_exists="append", **kwargs)

        if remove_duplicates:
            if self._config.current_db["engine"] in ["influxdb"]:
                logging.warning('Option "remove duplicates" is not supported yet.')
                return

            # Create temporal table
            temp_table_name = self.df_name + "_" + datetime.now().strftime("%s")
            self._initialize_df().to_sql(temp_table_name, self._engine, index=False)

            # Select unique rows and insert into the temporal table
            if self._config.current_db["engine"] in ["mysql", "sqlite"]:
                select = 'select * from "{0}" group by uuid_in_df'.format(self.df_name)
            elif self._config.current_db["engine"] in ["postgresql", "timescaledb"]:
                select = 'select distinct * from "{0}"'.format(self.df_name)
            else:
                raise ValueError("Unsupported engine: {}".format(self._engine.name))
            q = 'insert into "{0}" {1}'.format(temp_table_name, select)
            self._engine.execute(q)

            # Drop deprecated table if exists
            if self._engine.dialect.has_table(
                self._engine, self.df_name + "_deprecated"
            ):
                self._engine.execute(
                    'drop table "{0}"'.format(self.df_name + "_deprecated")
                )

            # Deprecate the original table
            self._engine.execute(
                'alter table "{0}" rename to "{1}"'.format(
                    self.df_name, self.df_name + "_deprecated"
                )
            )

            # Rename the temporal table
            self._engine.execute(
                'alter table "{0}" rename to "{1}"'.format(
                    temp_table_name, self.df_name
                )
            )

            # Drop the deprecated table
            self._engine.execute(
                'drop table "{0}"'.format(self.df_name + "_deprecated")
            )

    @property
    def df(self):
        """Return df."""
        return self._df

    @df.setter
    def df(self, value):
        """Setter for self.df."""
        if not isinstance(value, pd.DataFrame):
            raise ValueError("Only pandas dataframe is accepted.")

        # Set columns based on the given DF
        if len(self.columns) == 0 and len(value) > 0:
            self.columns = [
                {"name": c, "dtype": "none"} for c in value.columns.to_list()
            ]

        # Add column 'uuid_in_df'
        if "uuid_in_df" not in value.columns.to_list():
            value["uuid_in_df"] = value.apply(
                lambda x: self._get_uuid_from_item(x), axis=1
            )
            self.columns += [{"name": "uuid_in_df", "dtype": "str"}]

        self._df = value

    @property
    def df_name(self):
        """Return df_name."""
        return self._df_name

    @df_name.setter
    def df_name(self, value):
        """Setter for self.df_name."""
        self._df_name = value

    @property
    def columns(self):
        """Return columns of DF."""
        if self._columns is not None:
            return self._columns
        try:
            return self._config[self.df_name]["columns"]
        except KeyError:
            return []

    @columns.setter
    def columns(self, value):
        """Set self.columns."""
        if not isinstance(value, list):
            raise ValueError(
                'Columns must be a list of dicts with keys "name" and "dtype".'
            )
        if len(value) < 1:
            raise ValueError("At least one item must be in the list.")
        if (
            not isinstance(value[0], dict)
            or "name" not in value[0].keys()
            or "dtype" not in value[0].keys()
        ):
            raise ValueError(
                'Columns must be a list of dicts with keys "name" and "dtype".'
            )

        try:
            _ = pd.concat(
                [
                    pd.Series(
                        name=c["name"], dtype=dtype_string_to_dtype_object(c["dtype"])
                    )
                    for c in value
                ]
                + [pd.Series(name="uuid_in_df", dtype=str)],
                axis=1,
            )
        except KeyError as e:
            raise ValueError("Unrecognized value: {}".format(str(e)))

        self._columns = value


class TimeSeriesDBHandler(BaseDBHandler):
    """Time series database handler."""

    __version__ = "v2"
    db_defaults = load_config(__version__).sql.time_series_defaults
    _df_name = "time_series_df"
    _columns = None

    def read(self, *args, **kwargs):
        """Read data from SQL.

        Args:
            *args: args
            **kwargs: kwargs

        Returns:
            (pandas.df): table

        """
        # Read table from DB
        if self._config.current_db["engine"] in ["influxdb"]:
            super().read(*args, **kwargs)
        else:
            super().read(*args, **kwargs, index_col="timestamp")

        # Post-process
        try:
            df = self.df
            df.reset_index(inplace=True)
            df.rename(columns={"index": "timestamp"}, inplace=True)
            df["timestamp"] = df["timestamp"].astype(int) / 10**9
        except Exception as e:
            self.logger.warning("An error occurred in post-process: {}".format(e))
            df = self._initialize_df()

        self.df = df

    def save(self, df=None, **kwargs):
        """Save data to SQL.

        Args:
            df (pandas.DataFrame): dataframe to save. if None, self.df will be saved

        """
        dataframe: pd.DataFrame = df if df is not None else self.df

        # Convert timestamp (float) to datetime
        assert "timestamp" in dataframe.columns
        df_to_save = dataframe.copy()
        df_to_save["timestamp"] = pd.to_datetime(
            df_to_save["timestamp"].astype(float), unit="s"
        )
        df_to_save.set_index("timestamp", inplace=True)

        if self._config.current_db["engine"] in ["influxdb"]:
            super().save(df=df_to_save)
        else:
            super().save(df=df_to_save, index=True)
