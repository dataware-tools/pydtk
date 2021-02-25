#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

from typing import Optional

from pymongo import MongoClient
import pql as PQL


def connect(
    db_host: str,
    db_name: Optional[str] = None,
    db_username: Optional[str] = None,
    db_password: Optional[str] = None,
    collection_name: Optional[str] = None
):
    """Connect to DB.

    Args:
        db_host (str): database host
        db_name (str): database name
        db_username (str): database username
        db_password (str): database password
        collection_name (str): collection name

    Returns:
        (any): connection

    """
    if db_name is None:
        db_name = 'default'
    if collection_name is None:
        collection_name = 'default'

    if ':' in db_host:
        address = db_host.split(':')[0]
        port = int(db_host.split(':')[1])
    else:
        address = db_host
        port = None

    kwargs = {}
    if db_name is not None:
        kwargs.update({'authSource': 'admin'})
    connection = MongoClient(
        host=address,
        port=port,
        username=db_username,
        password=db_password,
        **kwargs
    )
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

    data = list(data)

    return data, len(data)


def write(db, data):
    """Write data to DB.

    Args:
        db (TinyDB): DB connection
        data (list): data to save

    """
    for record in data:
        uuid = record['uuid_in_df']
        db.update({'uuid_in_df': uuid}, record, upsert=True)


def remove(db, uuid):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuid (str): Unique id

    """
    db.delete_many({'uuid_in_df': uuid})
