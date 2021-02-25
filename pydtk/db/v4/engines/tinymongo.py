#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

import os
from typing import Optional

from tinydb import TinyDB
from tinydb.database import Document as _Document
from tinydb.database import StorageProxy as _StorageProxy
from tinymongo import TinyMongoClient
import pql as PQL


class Document(_Document):
    """Custom Document."""

    def __getitem__(self, item):
        """Get item.

        Args:
            item (str): item-name

        Returns:
            (Document)

        """
        if '.' not in item:
            return super().__getitem__(item)
        else:
            current_item = self
            for sub_item in item.split('.'):
                current_item = current_item.__getitem__(sub_item)
            return current_item


class StorageProxy(_StorageProxy):
    """Custom StorageProxy."""

    def _new_document(self, key, val):
        doc_id = int(key)
        return Document(val, doc_id)


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

    # Customize storage-proxy
    TinyDB.storage_proxy_class = StorageProxy

    connection = TinyMongoClient(db_host)
    db = getattr(connection, db_name)
    collection = getattr(db, collection_name)
    return collection


def read(db,
         query: Optional[dict] = None,
         pql: Optional[any] = None,
         order_by: Optional[list] = None,
         **kwargs):
    """Read data from DB.

    Args:
        db (TinyDB): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        order_by (list): column name to sort by with format [ ( column1, 1 or -1 ), ... ]
        **kwargs: kwargs for function `pandas.read_sql_query`
                  or `influxdb.DataFrameClient.query`

    Returns:
        (list, int): list of data and total number of records

    """
    if order_by is None:
        order_by = [('_creation_time', 1)]

    if pql is not None and query is not None:
        raise ValueError('Either query or pql can be specified')

    if pql:
        query = PQL.find(pql)

    if query:
        data = db.find(query).sort(order_by)
    else:
        data = db.find().sort(order_by)

    data = list(data)

    return data, len(data)


def write(db, data, **kwargs):
    """Write data to DB.

    Args:
        db (TinyDB): DB connection
        data (list): data to save

    """
    for record in data:
        uuid = record['_uuid']
        if db.find_one({'_uuid': uuid}) is not None:
            db.update({'_uuid': uuid}, record)
        else:
            db.insert(record)


def remove(db, uuid, **kwargs):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuid (str): Unique id

    """
    db.delete_many({'_uuid': uuid})
