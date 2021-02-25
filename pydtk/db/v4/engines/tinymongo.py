#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

import os
from typing import Optional

from tinymongo import TinyMongoClient
import pql as PQL


def connect(
    db_host: str,
    db_name: Optional[str] = None,
    collection_name: Optional[str] = None,
    **kwargs
):
    """Connect to DB.

    Args:
        db_host (str): database host
        db_name (str): database name
        collection_name (str): collection name

    Returns:
        (any): connection

    """
    if db_name is None:
        db_name = 'default'
    if collection_name is None:
        collection_name = 'default'

    if not os.path.isdir(db_host):
        os.makedirs(db_host, exist_ok=True)

    connection = TinyMongoClient(db_host)
    db = getattr(connection, db_name)
    collection = getattr(db, collection_name)
    return collection


def read(db,
         query: Optional[dict] = None,
         pql: Optional[any] = None,
         **kwargs):
    """Read data from DB.

    Args:
        db (TinyDB): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        **kwargs: kwargs for function `pandas.read_sql_query`
                  or `influxdb.DataFrameClient.query`

    Returns:
        (list, int): list of data and total number of records

    """
    if pql is not None and query is not None:
        raise ValueError('Either query or pql can be specified')

    if pql:
        query = PQL.find(pql)

    if query:
        data = db.find(query)
    else:
        data = db.find()

    data = data.cursordat

    return data, len(data)


def write(db, data):
    """Write data to DB.

    Args:
        db (TinyDB): DB connection
        data (list): data to save

    """
    for record in data:
        uuid = record['uuid_in_df']
        if db.find_one({'uuid_in_df': uuid}) is not None:
            db.update({'uuid_in_df': uuid}, record)
        else:
            db.insert(record)


def remove(db, uuid):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuid (str): Unique id

    """
    db.delete_many({'uuid_in_df': uuid})
