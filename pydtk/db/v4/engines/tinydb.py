#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

import logging
from datetime import datetime
from typing import Optional

from tinydb import Query, TinyDB
from tinydb import __version__ as tinydb_version

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION_NAME = "default"


def connect(db_host: str, collection_name: Optional[str] = None, **kwargs):
    """Connect to DB.

    Args:
        db_host (str): database host
        collection_name (str): collection name

    Returns:
        (any): connection

    """
    if collection_name is None:
        collection_name = DEFAULT_COLLECTION_NAME

    if tinydb_version.startswith("4"):
        db = TinyDB(db_host)
        db.default_table_name = collection_name
    elif tinydb_version.startswith("3"):
        db = TinyDB(db_host, default_table=collection_name)
    else:
        raise RuntimeError("TinyDB version < 3, >4 is not supported")
    return db


def read(db, query: Optional[dict or Query] = None, **kwargs):
    """Read data from DB.

    Args:
        db (TinyDB): DB connection
        query (dict or Query): Query to select items
        **kwargs: kwargs for function `pandas.read_sql_query`
                  or `influxdb.DataFrameClient.query`

    Returns:
        (list, int): list of data and total number of records

    """
    if query:
        data = db.search(query)
    else:
        data = db.all()

    return data, len(data)


def upsert(db, data, **kwargs):
    """Write data to DB.

    Args:
        db (TinyDB): DB connection
        data (list): data to save

    """
    for record in data:
        _record = _fix_datetime(record)
        uuid = _record["_uuid"]
        db.upsert(_record, Query()._uuid == uuid)


def remove(db, uuids, **kwargs):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuids (list): A list of unique IDs

    """
    for uuid in uuids:
        db.remove(Query()._uuid == uuid)


def drop_table(db, name, **kwargs):
    """Drop a table from DB.

    Args:
        db (TinyDB): DB connection
        name (str): Name of the target table

    """
    if tinydb_version.startswith("4"):
        db.drop_table(name)
    else:
        db.purge_table(name)


def exist_table(db, name, **kwargs):
    """Check if the specified table (collection) exist.

    Args:
        db (TinyDB): DB connection
        name (str): Name of the target table

    """
    return name in list(db.tables())


def _fix_datetime(data: dict):
    """Fix datetime object.

    Args:
        data (dict): Input data

    Returns:
        (dict): Fixed data

    """
    _data = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            _data[key] = value.timestamp()
        else:
            _data[key] = value

    return _data
