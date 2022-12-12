#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

import logging
import os
from datetime import datetime
from typing import Optional

from tinydb import TinyDB
from tinydb import __version__ as tinydb_version
from tinydb.database import Document as _Document
from tinydb.database import StorageProxy as _StorageProxy
from tinymongo import TinyMongoClient

from ..deps import pql as PQL

logger = logging.getLogger(__name__)


DEFAULT_DB_NAME = "default"
DEFAULT_COLLECTION_NAME = "default"


class Document(_Document):
    """Custom Document."""

    def __getitem__(self, item):
        """Get item.

        Args:
            item (str): item-name

        Returns:
            (Document)

        """
        if "." not in item:
            return super().__getitem__(item)
        else:
            current_item = self
            for sub_item in item.split("."):
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
        (TinyMongoCollection): connection

    """
    if db_name is None:
        db_name = DEFAULT_DB_NAME
    if collection_name is None:
        collection_name = DEFAULT_COLLECTION_NAME

    if not os.path.isdir(db_host):
        os.makedirs(db_host, exist_ok=True)

    # Customize storage-proxy
    TinyDB.storage_proxy_class = StorageProxy

    connection = TinyMongoClient(db_host)
    db = getattr(connection, db_name)
    collection = getattr(db, collection_name)

    # Add attributes
    collection._db_host = db_host
    collection._db_name = db_name
    collection._collection_name = collection_name

    return collection


def read(
    db,
    query: Optional[dict] = None,
    pql: any = None,
    order_by: Optional[list] = None,
    **kwargs
):
    """Read data from DB.

    Args:
        db (TinyMongoCollection): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        order_by (list): column name to sort by with format [ ( column1, 1 or -1 ), ... ]
        **kwargs: kwargs for function `pandas.read_sql_query`
                  or `influxdb.DataFrameClient.query`

    Returns:
        (list, int): list of data and total number of records

    """
    # Re-initialize DB to reload data (This operation is needed as tinymongo caches data in memory)
    db = connect(
        db_host=db._db_host,
        db_name=db._db_name,
        collection_name=db._collection_name,
    )

    if pql is not None and query is not None:
        raise ValueError("Either query or pql can be specified")

    if pql:
        query = PQL.find(pql)

    # Fix query
    if query:
        query = _fix_query_exists(query)

    if query:
        if order_by is None:
            data = db.find(query)
        else:
            data = db.find(query).sort(order_by)
    else:
        if order_by is None:
            data = db.find()
        else:
            data = db.find().sort(order_by)

    data = list(data)

    return data, len(data)


def upsert(db, data, **kwargs):
    """Write data to DB.

    Args:
        db (TinyMongoCollection): DB connection
        data (list): data to save

    """
    for record in data:
        _record = _fix_datetime(record)
        uuid = record["_uuid"]
        if db.find_one({"_uuid": uuid}) is not None:
            db.update({"_uuid": uuid}, _record)
        else:
            db.insert(_record)


def remove(db, uuids, **kwargs):
    """Remove data from DB.

    Args:
        db (TinyMongoCollection): DB connection
        uuids (list): A list of unique IDs

    """
    for uuid in uuids:
        db.delete_many({"_uuid": uuid})


def drop_table(db, name, **kwargs):
    """Drop a table from DB.

    Args:
        db (TinyMongoCollection): DB connection
        name (str): Name of the target table

    """
    if tinydb_version.startswith("4"):
        db.parent.tinydb.drop_table(name)
    else:
        db.parent.tinydb.purge_table(name)


def exist_table(db, name, **kwargs):
    """Check if the specified table (collection) exist.

    Args:
        db (TinyMongoCollection): DB connection
        name (str): Name of the target table

    """
    return name in list(db.parent.tinydb.tables())


def _fix_query_exists(query):
    if isinstance(query, list):
        fixed_query = []
        for item in query:
            fixed_query.append(_fix_query_exists(item))
        return fixed_query

    elif isinstance(query, dict):
        fixed_query = {}
        for key, value in query.items():
            if isinstance(value, dict) or isinstance(value, list):
                fixed_query[key] = _fix_query_exists(value)
            elif key == "$exists" and value:
                fixed_query["$ne"] = None
            else:
                fixed_query[key] = value

        return fixed_query

    else:
        raise ValueError


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
