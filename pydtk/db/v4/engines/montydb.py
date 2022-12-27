#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""
from datetime import datetime
from typing import Optional

from montydb import MontyClient, set_storage

from ..deps import pql as PQL

DEFAULT_DB_NAME = "default"
DEFAULT_COLLECTION_NAME = "default"


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
        (MontyCollection): connection

    """
    if db_name is None:
        db_name = DEFAULT_DB_NAME
    if collection_name is None:
        collection_name = DEFAULT_COLLECTION_NAME

    set_storage(
        # general settings
        repository=db_host,  # dir path for database to live on disk, default is {cwd}
        # NOTE(kan-bayashi): flatfile cannot pass test
        storage="sqlite",  # storage name, default "flatfile"
        use_bson=None,  # default None, and will import pymongo's bson if None or True
        # any other kwargs are storage engine settings.
        cache_modified=0,  # the only setting that flat-file have
    )

    collection = getattr(getattr(MontyClient(db_host), db_name), collection_name)
    return collection


def read(
    db,
    query: Optional[dict] = None,
    pql: any = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    disable_count_total: bool = False,
    **kwargs
):
    """Read data from DB.

    Args:
        db (MontyCollection): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        order_by (list): column name to sort by with format [ ( column1, 1 or -1 ), ... ]
        limit (int): number of items to return per a page
        offset (int): offset of cursor
        disable_count_total (bool): set True to avoid counting total number of records
        **kwargs: kwargs for function `pandas.read_sql_query`
                  or `influxdb.DataFrameClient.query`

    Returns:
        (list, int): list of data and total number of records

    """
    if limit is None:
        limit = 0
    if offset is None:
        offset = 0

    if pql is not None and query is not None:
        raise ValueError("Either query or pql can be specified")

    if pql:
        query = PQL.find(pql)

    if query:
        query = _fix_query_exists(query)
        if order_by is None:
            data = db.find(query).skip(offset).limit(limit)
            count_total = db.count(query) if not disable_count_total else None
        else:
            data = db.find(query).sort(order_by).skip(offset).limit(limit)
            count_total = db.count(query) if not disable_count_total else None
    else:
        if order_by is None:
            data = db.find().skip(offset).limit(limit)
            count_total = db.count({}) if not disable_count_total else None
        else:
            data = db.find().sort(order_by).skip(offset).limit(limit)
            count_total = db.count({}) if not disable_count_total else None

    data = list(data)
    count_total = count_total if count_total is not None else len(data)

    return data, count_total


def upsert(db, data, **kwargs):
    """Write data to DB.

    Args:
        db (MontyCollection): DB connection
        data (list): data to save

    """
    for record in data:
        _record = _fix_datetime(record)
        uuid = _record["_uuid"]
        existing_record = db.find_one({"_uuid": uuid})
        if existing_record is not None:
            existing_record.update(_record)
            db.replace_one({"_uuid": uuid}, existing_record)
        else:
            db.insert_one(_record)


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
        db (MontyCollection): DB connection
        name (str): Name of the target table

    """
    db.database.drop_collection(name)


def exist_table(db, name, **kwargs):
    """Check if the specified table (collection) exist.

    Args:
        db (MontyCollection): DB connection
        name (str): Name of the target table

    """
    return name in db.database.list_collection_names()


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
