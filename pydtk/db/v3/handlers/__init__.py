#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""V3DBHandler."""

import hashlib
import importlib
import logging
import os
from copy import deepcopy
from datetime import datetime

import numpy as np
import pandas as pd
from migrate import create_column
from sqlalchemy import Column, MetaData, Table, inspect, sql
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.types import BOOLEAN, DECIMAL, TEXT, VARCHAR
from tqdm import tqdm

from pydtk.db import V2BaseDBHandler as _V2BaseDBHandler
from pydtk.utils.utils import (
    deserialize_dict_1d,
    dicts_to_listed_dict_2d,
    dtype_string_to_dtype_object,
    load_config,
    serialize_dict_1d,
)

DB_HANDLERS = {}  # key: db_class, value: dict( key: db_engine, value: handler )


def map_dtype(dtype, db_engine="general"):
    """Mapper for dtype.

    Args:
        dtype (str): dtype in dataframe
        db_engine (str): DB engine

    Returns:
        (dict): { 'df': dtype for Pandas.DataFrame, 'sql': dtype for SQL table }

    """
    if dtype in ["int", "int32", "int64", "float", "float32", "float64", "double"]:
        if db_engine in ["mysql", "mariadb"]:
            return {"df": "double", "sql": DOUBLE}
        elif db_engine in ["postgresql", "timescaledb"]:
            return {"df": "double", "sql": DOUBLE_PRECISION}
        else:
            return {"df": "double", "sql": DECIMAL}

    if dtype in ["bool"]:
        return {"df": "boolean", "sql": BOOLEAN}

    if dtype in ["object", "string", "text"]:
        return {"df": "text", "sql": TEXT}

    raise ValueError("Unsupported dtype: {}".format(dtype))


def register_handlers():
    """Register handlers."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__))):
        if not os.path.isfile(os.path.join(os.path.dirname(__file__), filename)):
            continue
        if filename == "__init__.py":
            continue

        try:
            importlib.import_module(
                os.path.join(
                    "pydtk.db.v3.handlers", os.path.splitext(filename)[0]
                ).replace(os.sep, ".")
            )
        except ModuleNotFoundError:
            logging.debug("Failed to load handlers in {}".format(filename))


def register_handler(db_classes, db_engines):
    """Register a DB-handler.

    Args:
        db_classes (list): list of db_class names (e.g. ['meta'])
        db_engines (list): list of supported db_engines (e.g. ['sqlite', 'mysql'])

    """

    def decorator(cls):
        for db_class in db_classes:
            if db_class not in DB_HANDLERS.keys():
                DB_HANDLERS.update({db_class: {}})
            for db_engine in db_engines:
                if db_engine not in DB_HANDLERS[db_class].keys():
                    DB_HANDLERS[db_class].update({db_engine: cls})
        return cls

    return decorator


class BaseDBHandler(_V2BaseDBHandler):
    """Base handler for db."""

    __version__ = "v3"
    _df_class = "base_df"

    def __new__(cls, db_class: str = None, db_engine: str = None, **kwargs) -> object:
        """Create object.

        Args:
            db_class (str): database class (e.g. 'meta')
            db_engine (str): database engine (e.g. 'sqlite')
            **kwargs: DB-handler specific arguments

        Returns:
            (object): the corresponding handler object

        """
        if cls is BaseDBHandler:
            handler = cls._get_handler(db_class, db_engine)
            return super(BaseDBHandler, cls).__new__(handler)
        else:
            return super(BaseDBHandler, cls).__new__(cls)

    @classmethod
    def _get_handler(cls, db_class, db_engine=None):
        """Returns an appropriate handler.

        Args:
            db_class (str): database class (e.g. 'meta')
            db_engine (str): database engine (e.g. 'sqlite')

        Returns:
            (handler): database handler object

        """
        # Load default config
        config = load_config(cls.__version__)

        # Check if the db_class is available
        if db_class not in DB_HANDLERS.keys():
            raise ValueError("Unsupported db_class: {}".format(db_class))

        # Get db_engine from environment variable if not specified
        if db_engine is None:
            db_engine = os.environ.get(
                "PYDTK_{}_DB_ENGINE".format(db_class.upper()), None
            )

        # Get the default engine if not specified
        if db_engine is None:
            try:
                db_defaults = getattr(config.sql, db_class)
                db_engine = db_defaults.engine
            except (ValueError, AttributeError):
                raise ValueError("Could not find the default value")

        # Check if the corresponding handler is registered
        if db_engine not in DB_HANDLERS[db_class].keys():
            raise ValueError("Unsupported db_engine: {}".format(db_engine))

        # Get a DB-handler supporting the engine
        return DB_HANDLERS[db_class][db_engine]

    def __init__(self, **kwargs):
        """Initialize BaseDBHandler.

        Args:
            **kwargs: kwargs

        """
        self._count_total = 0
        if "db_class" in kwargs.keys():
            del kwargs["db_class"]
        super(BaseDBHandler, self).__init__(**kwargs)

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
        if "creation_time_in_df" in data.keys():
            del data["creation_time_in_df"]

        # Post-processes
        data = deserialize_dict_1d(data)

        # Increment
        self._cursor += 1

        return data

    def _initialize_df(self):
        """Initialize DF."""
        df = pd.concat(
            [
                pd.Series(
                    name=c["name"], dtype=dtype_string_to_dtype_object(c["dtype"])
                )
                for c in self.columns
                if c["name"] != "uuid_in_df" and c["name"] != "creation_time_in_df"
            ]  # noqa: E501
            + [
                pd.Series(name="uuid_in_df", dtype=str),
                pd.Series(name="creation_time_in_df", dtype=float),
            ],
            axis=1,
        )
        df.set_index("uuid_in_df", inplace=True)
        return df

    def _get_uuid_from_item(self, data_in):
        """Return UUID of the given item.

        Args:
            data_in (dict or pandas.Series): dict or Series containing data

        Returns:
            (str): UUID

        """
        hash_target_columns = self._config[self._df_class]["index_columns"]

        item = data_in
        if isinstance(item, pd.Series):
            item = data_in.to_dict()

        pre_hash = "".join(
            [
                "{:.09f}".format(item[column])
                if isinstance(item[column], float)
                else str(item[column])
                for column in hash_target_columns
                if column in item.keys()
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

            # Add df_uuid and creation_time_in_df
            item["uuid_in_df"] = self._get_uuid_from_item(item)
            item["creation_time_in_df"] = datetime.now().timestamp()

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
        self.df = pd.concat(
            [self.df, pd.DataFrame.from_dict(data).set_index("uuid_in_df")], sort=False
        )
        if check_unique:
            self.df = self.df[~self.df.index.duplicated(keep="last")]

    def _get_column_names_from_db(self):
        """Acquire one row from DB and get columns.

        Returns:
            (list): list for column names

        """
        q = "select * from {0} limit 1".format(self._quote(self.df_name))
        df = pd.read_sql_query(q, con=self._engine)
        columns = df.columns.tolist()
        return columns

    def read(
        self,
        df_name=None,
        query=None,
        where=None,
        group_by=None,
        order_by=None,
        limit=None,
        offset=None,
        disable_count_total=False,
        **kwargs
    ):
        """Read data from SQL.

        Args:
            df_name (str): Dataframe name to read
            query (str SQL query or SQLAlchemy Selectable): query to select items
            where (str): query string for filtering items
            group_by (str): column name to group
            order_by (srt): column name to sort by
            limit (int): number of items to return per a page
            offset (int): offset of cursor
            disable_count_total (bool): if True, `self.count_total` will not be calculated
            **kwargs: kwargs for function `pandas.read_sql_query`
                      or `influxdb.DataFrameClient.query`

        """
        if df_name is not None:
            self.df_name = df_name

        # Create a query
        q = query
        q_count = (
            "select count(*) from ({}) as sub".format(q) if q is not None else None
        )
        if q is None:
            # Check if the table exits on DB
            if not inspect(self._engine).has_table(self.df_name):
                self.df = self._initialize_df()
                return

            # Create a query
            if self._config.current_db["engine"] in ["postgresql", "timescaledb"]:
                q = sql.select("*", from_obj=sql.table(self.df_name))
                if group_by is not None:
                    q = sql.select(
                        [
                            sql.text(self._distinct(c))
                            for c in self._get_column_names_from_db()
                        ],
                        from_obj=sql.table(self.df_name),
                    )
                    q = q.group_by(sql.text(group_by))
                if where is not None:
                    q = q.where(sql.text(where))
                q_count = sql.select(
                    [sql.text("count(*)")], from_obj=sql.alias(q, "sub")
                )
            else:
                q = sql.select("*", from_obj=sql.table(self.df_name))
                q_count = sql.select(
                    [sql.text("count(*)")], from_obj=sql.table(self.df_name)
                )
                if group_by is not None:
                    q = sql.select(
                        [
                            sql.text(self._distinct(c))
                            for c in self._get_column_names_from_db()
                        ],
                        from_obj=sql.table(self.df_name),
                    )
                    q = q.group_by(sql.text(group_by))
                    q_count = sql.select(
                        [sql.text("count(distinct {})".format(group_by))],
                        from_obj=sql.table(self.df_name),
                    )
                if where is not None:
                    q = q.where(sql.text(where))
                    q_count = q_count.where(sql.text(where))

            # Add common options
            if order_by is not None:
                q = q.order_by(sql.text(order_by))
            if limit is not None:
                q = q.limit(limit)
            if offset is not None:
                q = q.offset(offset)

        # Read table from DB
        try:
            # Calculate total number of rows
            if not disable_count_total:
                count = self._engine.execute(q_count).scalar()
            else:
                count = 0

            # Fetch data
            if "index_col" not in kwargs.keys():
                kwargs.update({"index_col": "uuid_in_df"})
            df = pd.read_sql_query(q, self._engine, **kwargs)
            df = df[~df.index.duplicated(keep="last")]
        except Exception as e:
            self.logger.warning(
                'Could not execute SQL statement: "{0}" (reason: {1})'.format(
                    str(q), str(e)
                )
            )
            count = 0
            df = self._initialize_df()

        self._count_total = count
        self.df = df

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
        dataframe.to_sql(
            self.df_name,
            self._engine,
            if_exists="append",
            dtype={"uuid_in_df": VARCHAR(32)},
            **kwargs
        )

        if remove_duplicates:
            self._remove_duplicates()

    def _prepare_columns(self):
        try:
            existing_columns = self._get_column_names_from_db()
        except (OperationalError, ProgrammingError):
            return

        # Get table
        meta = MetaData(bind=self._engine)
        table = Table(self.df_name, meta, autoload=True)

        # Add columns
        for column_name in set([c["name"] for c in self.columns]).difference(
            existing_columns
        ):
            column = next((filter(lambda c: c["name"] == column_name, self.columns)))
            column_name = column["name"]
            if column_name in ["uuid_in_df"]:
                column_dtype = VARCHAR(32)
            elif column_name in ["creation_time_in_df"]:
                column_dtype = map_dtype("double", self._config.current_db["engine"])[
                    "sql"
                ]
            else:
                column_dtype = map_dtype(
                    column["dtype"], self._config.current_db["engine"]
                )["sql"]
            create_column(Column(column_name, column_dtype), table)

    def _quote(self, value):
        if self._config.current_db["engine"] in ["mariadb"]:
            return "`{}`".format(value)
        else:
            return '"{}"'.format(value)

    def _distinct(self, column_name):
        """Return selection key to get the distinct element.

        Args:
            column_name (str): name of the column

        Returns:
            (str): selection key

        """
        column_dtype = None
        if column_name in self._get_column_names():
            column_info = next(
                iter(filter(lambda c: c["name"] == column_name, self.columns))
            )
            column_dtype = column_info["dtype"]

        if self._config.current_db["engine"] in ["postgresql", "timescaledb"]:
            if column_dtype in ["string[]"]:
                return "string_agg({0}, ';') as {0}".format(column_name)
            return "min({0}) as {0}".format(column_name)
        else:
            if column_dtype in ["string[]"]:
                return "group_concat({0}, ';') as {0}".format(column_name)
            return column_name

    def _remove_duplicates(self):
        temp_table_name = self.df_name + "_" + datetime.now().strftime("%s")

        columns = self._get_column_names_from_db()

        q = (
            "create table {0} as "
            "select {2} from {1} "
            "where creation_time_in_df in ("
            "  select max(creation_time_in_df) from {1}"
            "  group by uuid_in_df"
            ") "
            "group by uuid_in_df".format(
                self._quote(temp_table_name),
                self._quote(self.df_name),
                ",".join([self._distinct(self._quote(c)) for c in columns]),
            )
        )
        self._engine.execute(q)

        # Create index
        if self._config.current_db["engine"] in ["sqlite"]:
            q = "create index index_uuid_in_df on {} (uuid_in_df)".format(
                self._quote(self.df_name)
            )
        elif self._config.current_db["engine"] in ["mysql", "mariadb"]:
            q = "alter table {} add index index_uuid_in_df(uuid_in_df)".format(
                self._quote(self.df_name)
            )
        elif self._config.current_db["engine"] in ["postgresql", "timescaledb"]:
            q = "create index on {} (uuid_in_df)".format(self._quote(self.df_name))
        else:
            raise ValueError(
                "Unsupported engine: {}".format(self._config.current_db["engine"])
            )
        self._engine.execute(q)

        # Drop deprecated table if exists
        if inspect(self._engine).has_table(self.df_name + "_deprecated"):
            self._engine.execute(
                "drop table {0}".format(self._quote(self.df_name + "_deprecated"))
            )

        # Deprecate the original table
        self._engine.execute(
            "alter table {0} rename to {1}".format(
                self._quote(self.df_name), self._quote(self.df_name + "_deprecated")
            )
        )

        # Rename the temporal table
        self._engine.execute(
            "alter table {0} rename to {1}".format(
                self._quote(temp_table_name), self._quote(self.df_name)
            )
        )

        # Drop the deprecated table and temporal table
        self._engine.execute(
            "drop table {0}".format(self._quote(self.df_name + "_deprecated"))
        )

    @property
    def columns(self):
        """Return columns of DF."""
        if self._columns is not None:
            return self._columns
        try:
            return self._config[self._df_class]["columns"]
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
                + [
                    pd.Series(name="uuid_in_df", dtype=str),
                    pd.Series(name="creation_time_in_df", dtype=float),
                ],
                axis=1,
            )
        except KeyError as e:
            raise ValueError("Unrecognized value: {}".format(str(e)))

        self._columns = value

    @property
    def df(self):
        """Return df."""
        return self._df

    @df.setter
    def df(self, value):
        """Setter for self.df."""
        if not isinstance(value, pd.DataFrame):
            raise ValueError("Only pandas dataframe is accepted.")

        if len(value) > 0:
            # Set columns based on the given DF
            self.columns = [
                {
                    "name": c,
                    "dtype": map_dtype(dtype.name, self._config.current_db["engine"])[
                        "df"
                    ],
                }
                for c, dtype in value.dtypes.to_dict().items()
            ]

            # Add column 'uuid_in_df' and 'creation_time_in_df'
            if "uuid_in_df" not in self._get_column_names():
                self.columns += [{"name": "uuid_in_df", "dtype": "str"}]
            if "uuid_in_df" not in value.columns.to_list():
                value["uuid_in_df"] = value.apply(
                    lambda x: self._get_uuid_from_item(x), axis=1
                )
            if "creation_time_in_df" not in self._get_column_names():
                self.columns += [{"name": "creation_time_in_df", "dtype": "double"}]
            if "creation_time_in_df" not in value.columns.to_list():
                value["creation_time_in_df"] = datetime.now().timestamp()
            value.set_index("uuid_in_df", inplace=True)

        self._df = value

    @property
    def count_total(self):
        """Return total number of rows."""
        return self._count_total


register_handlers()
