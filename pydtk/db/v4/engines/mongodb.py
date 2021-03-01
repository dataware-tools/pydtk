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
    collection_name: Optional[str] = None,
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
         group_by: Optional[str] = None,
         order_by: Optional[str] = None,
         limit: Optional[int] = None,
         offset: Optional[int] = None,
         handler: any = None,
         **kwargs):
    """Read data from DB.

    Args:
        db (TinyDB): DB connection
        query (dict or Query): Query to select items
        pql (PQL) Python-Query-Language to select items
        group_by (str): Aggregate by this key
        order_by (list): column name to sort by with format [ ( column1, 1 or -1 ), ... ]
        limit (int): number of items to return per a page
        offset (int): offset of cursor
        handler (BaseDBHandler): DBHandler
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
        raise ValueError('Either query or pql can be specified')

    if pql:
        query = PQL.find(pql)

    if group_by is None:
        if query:
            if order_by is None:
                data = db.find(query).skip(offset).limit(limit)
            else:
                data = db.find(query).sort(order_by).skip(offset).limit(limit)
        else:
            if order_by is None:
                data = db.find().skip(offset).limit(limit)
            else:
                data = db.find().sort(order_by).skip(offset).limit(limit)
    else:
        aggregate = []
        if query:
            aggregate.append({'$match': query})

        columns = {}
        for column in handler.columns:
            try:
                config = next(filter(lambda c: c['name'] == column, handler.config.columns))
                agg = config['aggregation']
                columns.update({column: {'${}'.format(agg): '${}'.format(column)}})
            except Exception:
                columns.update({column: {'$first': '${}'.format(column)}})

        aggregate.append({
            '$group': {
                '_id': '${}'.format(group_by),
                **columns
            }
        })
        aggregate.append({'$project': {'_id': 0}})

        if order_by is not None:
            aggregate.append({'$sort': {item[0]: item[1] for item in order_by}})

        if offset > 0:
            aggregate.append({'$skip': offset})
        if limit > 0:
            aggregate.append({'$limit': limit})

        data = db.aggregate(aggregate)

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
        db.update({'_uuid': uuid}, record, upsert=True)


def remove(db, uuid, **kwargs):
    """Remove data from DB.

    Args:
        db (TinyDB): DB connection
        uuid (str): Unique id

    """
    db.delete_many({'_uuid': uuid})