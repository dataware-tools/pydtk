#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

from typing import Optional

from tinydb import TinyDB, Query
from tinydb import __version__ as tinydb_version


def connect(
    db_host: str,
    collection_name: Optional[str] = None,
    **kwargs
):
    """Connect to DB.

    Args:
        db_host (str): database host
        collection_name (str): collection name

    Returns:
        (any): connection

    """
    if collection_name is None:
        collection_name = 'default'

    if tinydb_version.startswith('4'):
        db = TinyDB(db_host)
        db.default_table_name = collection_name
    elif tinydb_version.startswith('3'):
        db = TinyDB(db_host, default_table=collection_name)
    else:
        raise RuntimeError('TinyDB version < 3, >4 is not supported')
    return db


def read(db,
         query: Optional[dict or Query] = None,
         **kwargs):
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


def write(db, data):
    """Write data to DB.

    Args:
        db (TinyDB): DB connection
        data (list): data to save

    """
    for record in data:
        uuid = record['uuid_in_df']
        db.upsert(record, Query().uuid_in_df == uuid)


def remove(db, uuid):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuid (str): Unique id

    """
    db.remove(Query().uuid_in_df == uuid)
