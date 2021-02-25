#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from typing import Optional

import pytest

db_parameters = [
    'db_engine,db_host,db_username,db_password',
    [
        ('tinydb', 'test/test_v4.json', None, None),
        ('tinymongo', 'test/test_v4', None, None),
        # ('mongodb', 'host', 'testuser', 'testpass')
    ]
]
default_db_parameter = db_parameters[1][0]


@pytest.mark.parametrize(*db_parameters)
def test_create_db(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Create DB of records directory.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    from pydtk.db import V4DBHandler, V4MetaDBHandler
    from pydtk.models import MetaDataModel

    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test'
    )
    assert isinstance(handler, V4MetaDBHandler)

    paths = [
        'test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json',
        'test/records/B05_17000000010000000829/data/records.bag.json',
        'test/records/sample/data/records.bag.json',
        'test/records/meti2019/ssd7.bag.json',
    ]

    # Load metadata and add to DB
    record_ids = set()
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        record_ids.add(metadata.data['record_id'])
        handler.add_data(metadata.data)

    # Get DF
    df = handler.df
    content_df = handler.content_df
    file_df = handler.file_df
    record_id_df = handler.record_id_df
    assert len(df) == len(handler) and len(df) > 0
    assert len(content_df) == len(handler) and len(content_df) > 0
    assert len(file_df) == len(paths)
    assert len(record_id_df) == len(record_ids)

    # Save
    handler.save()


@pytest.mark.parametrize(*db_parameters)
def test_load_db(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    from pydtk.db import V4DBHandler, V4MetaDBHandler

    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='contents'
    )
    assert isinstance(handler, V4MetaDBHandler)

    assert handler.count_total > 0
    assert len(handler) > 0
    assert len(handler) > handler.count_total

    try:
        for sample in handler:
            assert 'contents' in sample.keys()
            assert isinstance(sample['contents'], dict)
            assert len(sample['contents'].keys()) == 1
    except (EOFError, StopIteration):
        pass


@pytest.mark.parametrize(*db_parameters)
def test_create_db_with_env_var(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Create DB of records directory.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    import os
    from pydtk.db import V4DBHandler
    from pydtk.models import MetaDataModel

    # Set environment variables
    if db_engine is not None:
        os.environ['PYDTK_META_DB_ENGINE'] = db_engine
    if db_host is not None:
        os.environ['PYDTK_META_DB_HOST'] = db_host
    if db_username is not None:
        os.environ['PYDTK_META_DB_USERNAME'] = db_username
    if db_password is not None:
        os.environ['PYDTK_META_DB_PASSWORD'] = db_password

    handler = V4DBHandler(
        db_class='meta',
        base_dir_path='test'
    )

    paths = [
        'test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json',
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)

    # Save
    handler.save()

    assert os.path.exists('test/test_v4_env.json')


@pytest.mark.parametrize(*db_parameters)
def test_load_db_with_env_var(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    import os
    from pydtk.db import V4DBHandler

    # Set environment variables
    if db_engine is not None:
        os.environ['PYDTK_META_DB_ENGINE'] = db_engine
    if db_host is not None:
        os.environ['PYDTK_META_DB_HOST'] = db_host
    if db_username is not None:
        os.environ['PYDTK_META_DB_USERNAME'] = db_username
    if db_password is not None:
        os.environ['PYDTK_META_DB_PASSWORD'] = db_password

    handler = V4DBHandler(db_class='meta')

    try:
        for sample in handler:
            print(sample)
    except EOFError:
        pass


@pytest.mark.parametrize(*db_parameters)
def test_merge(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Test merging dicts.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    from pydtk.db import V4DBHandler

    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='test',
        orient='contents',
        read_on_init=False
    )

    data_1 = {
        'record_id': 'aaa',
        'string': 'test123',
        'dict': {
            'aaa': 'aaa'
        },
        'list': [
            'aaa'
        ]
    }
    data_2 = {
        'record_id': 'aaa',
        'string': 'test123',
        'dict': {
            'bbb': 'bbb'
        },
        'list': [
            'bbb'
        ]
    }

    handler.add_data(data_1)
    handler.add_data(data_2)

    assert len(handler) == 1


def test_search_tinydb():
    """Search on TinyDB."""
    from pydtk.db import V4DBHandler
    from tinydb import where

    handler = V4DBHandler(
        db_class='meta',
        db_engine='tinydb',
        db_host='test/test_v4.json',
        base_dir_path='/opt/pydtk/test',
        orient='contents',
        read_on_init=False
    )

    handler.read(query=where('record_id') == '20191001_094731_000_car3')
    assert len(handler) > 0

    handler.read(query=where('start_timestamp') < 1489728492.0)
    assert len(handler) > 0


@pytest.mark.parametrize(db_parameters[0], db_parameters[1][1:3])
def test_search_mongo(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Search on MongoDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    from pydtk.db import V4DBHandler

    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='contents',
        read_on_init=False
    )

    # MongoDB-like query
    handler.read(query={'record_id': '20191001_094731_000_car3'})
    assert len(handler) > 0
    handler.read(query={'record_id': {'$regex': '016'}})
    assert len(handler) > 0
    handler.read(query={'record_id': {'$regex': '^016.*'}})
    assert len(handler) > 0
    handler.read(query={'record_id': {'$regex': '.*240$'}})
    assert len(handler) > 0
    handler.read(query={
        '$and': [
            {'record_id': {'$regex': '.*'}},
            {'database_id': 'METI2019'}
        ]
    })
    assert len(handler) > 0

    # Python-Query-Language (PQL)
    # TODO: Wait for PR "https://github.com/alonho/pql/pull/30" to be merged.
    # handler.read(pql="record_id == '20191001_094731_000_car3'")


if __name__ == '__main__':
    test_create_db(*default_db_parameter)
    test_load_db(*default_db_parameter)
    test_create_db_with_env_var(*default_db_parameter)
    test_load_db_with_env_var(*default_db_parameter)
    test_merge(*default_db_parameter)
    test_search_tinydb()
    test_search_mongo(default_db_parameter[1][1])
