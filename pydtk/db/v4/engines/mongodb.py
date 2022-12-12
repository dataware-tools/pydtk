#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""DB Engines for V4DBHandler."""

import logging
from copy import deepcopy
from typing import Optional

from pymongo import DeleteOne, MongoClient, UpdateOne

from ..deps import pql as PQL

logger = logging.getLogger(__name__)

DEFAULT_DB_NAME = "default"
DEFAULT_COLLECTION_NAME = "default"


def connect(
    db_host: str,
    db_name: Optional[str] = DEFAULT_DB_NAME,
    db_username: Optional[str] = None,
    db_password: Optional[str] = None,
    collection_name: Optional[str] = DEFAULT_COLLECTION_NAME,
    **kwargs
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
        db_name = DEFAULT_DB_NAME
    if collection_name is None:
        collection_name = DEFAULT_COLLECTION_NAME

    if ":" in db_host:
        address = db_host.split(":")[0]
        port = int(db_host.split(":")[1])
    else:
        address = db_host
        port = None

    kwargs = {}
    if db_name is not None:
        kwargs.update({"authSource": "admin"})
    connection = MongoClient(
        host=address, port=port, username=db_username, password=db_password, **kwargs
    )
    db = getattr(connection, db_name)
    collection = getattr(db, collection_name)
    return collection


def read(
    db,
    query: Optional[dict] = None,
    pql: any = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    handler: any = None,
    disable_count_total: bool = False,
    **kwargs
):
    """Read data from DB.

    Args:
        db (Collection): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        group_by (str): Aggregate by this key
        order_by (list): column name to sort by with format [ ( column1, 1 or -1 ), ... ]
        limit (int): number of items to return per a page
        offset (int): offset of cursor
        handler (BaseDBHandler): DBHandler
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

    if group_by is None:
        if query:
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
    else:
        aggregate = []
        if query:
            aggregate.append({"$match": query})

        columns = {}
        for column in set(handler.columns).union(["_uuid", "_creation_time"]):
            try:
                config = next(
                    filter(lambda c: c["name"] == column, handler.config["columns"])
                )
                agg = config["aggregation"]
                columns.update({column: {"${}".format(agg): "${}".format(column)}})
            except Exception:
                columns.update({column: {"$first": "${}".format(column)}})

        aggregate.append(
            {
                "$group": {
                    **columns,
                    "_id": "${}".format(group_by),
                }
            }
        )
        aggregate.append({"$project": {"_id": 0}})
        aggregate_count = deepcopy(aggregate)
        aggregate_count.append({"$count": "count"})

        if order_by is not None:
            aggregate.append({"$sort": {item[0]: item[1] for item in order_by}})

        if offset > 0:
            aggregate.append({"$skip": offset})
        if limit > 0:
            aggregate.append({"$limit": limit})

        data = db.aggregate(aggregate, allowDiskUse=True)
        try:
            count_total = (
                list(db.aggregate(aggregate_count))[0]["count"]
                if not disable_count_total
                else None
            )
        except Exception as e:
            logger.warning(e)
            count_total = None

    data = list(data)
    count_total = count_total if count_total is not None else len(data)

    return data, count_total


def upsert(db, data, **kwargs):
    """Write data to DB.

    Args:
        db (Collection): DB connection
        data (list): data to save

    """
    ops = [
        UpdateOne(
            {"_uuid": record["_uuid"]},
            {"$set": {**record, "_id": record["_uuid"]}},
            upsert=True,
        )
        for record in data
    ]
    if len(ops) > 0:
        db.bulk_write(ops)


def remove(db, uuids, **kwargs):
    """Remove data from DB.

    Args:
        db (Collection): DB connection
        uuids (list): A list of unique IDs

    """
    ops = [DeleteOne({"_uuid": uuid}) for uuid in uuids]
    if len(ops) > 0:
        db.bulk_write(ops)


def drop_table(db, name, **kwargs):
    """Drop a table from DB.

    Args:
        db (Collection): DB connection
        name (str): Name of the target table

    """
    db.database.__getitem__(name).drop()


def exist_table(db, name, **kwargs):
    """Check if the specified table (collection) exist.

    Args:
        db (Collection): DB connection
        name (str): Name of the target table (collection)

    """
    return name in db.database.list_collection_names()
