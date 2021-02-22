#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

from typing import Optional

from tinydb import TinyDB, Query


def connect(db_host: str, db_name: Optional[str] = None):
    """Connect to DB.
    Args:
        db_host (str): database host
        db_name (str): database name

    Returns:
        (any): connection

    """
    db = TinyDB(db_host)
    if db_name is not None:
        db.default_table_name = db_name
    return db


def read(db,
         query: Optional[str] = None,
         **kwargs):
    """Read data from DB.

    Args:
        db (TinyDB): DB connection
        query (str SQL query or SQLAlchemy Selectable): query to select items
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
