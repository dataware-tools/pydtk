#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from typing import Optional

from pydtk.db import V4DBHandler, V4MetaDBHandler, V4DatabaseIDDBHandler
import pytest

db_args = 'db_engine,db_host,db_username,db_password'
db_list = [
    ('tinydb', 'test/test_v4.json', None, None),
    ('tinymongo', 'test/test_v4', None, None),
    # ('mongodb', 'host', 'username', 'password')
]
default_db_parameter = db_list[0]


def _add_data_to_db(handler: V4DBHandler):
    from pydtk.models import MetaDataModel

    paths = [
        'test/records/sample/data/records.bag.json',
        'test/records/csv_model_test/data/test.csv.json',
        'test/records/json_model_test/json_test.json.json',
        'test/records/forecast_model_test/forecast_test.csv.json',
        'test/records/annotation_model_test/annotation_test.csv.json'
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
    assert len(df) == len(handler) and len(df) > 0

    # Save
    handler.save()


def _load_data_from_db(handler: V4DBHandler):
    assert handler.count_total > 0
    assert len(handler) > 0

    try:
        for sample in handler:
            assert 'contents' in sample.keys()
            assert isinstance(sample['contents'], dict)
            assert len(sample['contents'].keys()) == 1
    except (EOFError, StopIteration):
        pass


@pytest.mark.parametrize(db_args, db_list)
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
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test'
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _add_data_to_db(handler)


@pytest.mark.parametrize(db_args, db_list)
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
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='contents'
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _load_data_from_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_load_database_id(
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
    handler = V4DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
    )
    handler.read()

    assert isinstance(handler, V4DatabaseIDDBHandler)
    assert len(handler.df) == 1
    assert next(handler)['database_id'] == 'default'


@pytest.mark.parametrize(db_args, db_list)
def test_update_configs_db(
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
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='contents'
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    try:
        handler.config.update({'_df_name': 'aaa'})
        handler.config['_df_name'] = ''
        raise AssertionError
    except KeyError:
        pass
    handler.config['columns'].append({'name': 'test', 'dtype': 'str'})
    handler.save()

    del handler
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='contents'
    )
    handler.read()
    assert handler.config['columns'][-1]['name'] == 'test'
    del handler.config['columns'][-1]
    handler.save()


@pytest.mark.parametrize(db_args, db_list)
def test_delete_records(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Delete records from DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='record_id'
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)

    assert len(handler) == handler.count_total

    num_data = len(handler)

    # Remove one record without saving
    handler.remove_data(next(handler))
    assert len(handler) == num_data - 1
    handler.read()
    assert len(handler) == num_data

    # Remove all data and save
    try:
        for sample in handler:
            handler.remove_data(sample)
            num_data -= 1
            assert len(handler) == num_data
    except (EOFError, StopIteration):
        pass

    assert len(handler) == 0
    handler.save()

    # Rollback data
    _add_data_to_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_delete_collection(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Delete a collection from DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    handler = V4DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
    )
    handler.read()
    assert isinstance(handler, V4DatabaseIDDBHandler)

    num_databases_original = len(handler)
    database = next(handler)
    handler.remove_data(database)
    handler.save()
    assert len(handler) == num_databases_original - 1

    handler.read()
    assert len(handler) == num_databases_original - 1

    if db_engine not in ['tinydb', 'tinymongo']:
        # Check if the corresponding table is deleted
        meta_handler = V4DBHandler(
            db_class='meta',
            db_engine=db_engine,
            db_host=db_host,
            db_username=db_username,
            db_password=db_password,
        )
        assert isinstance(meta_handler, V4MetaDBHandler)
        assert len(meta_handler) == 0


@pytest.mark.parametrize(db_args, db_list)
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
        base_dir_path='/opt/pydtk/test'
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _add_data_to_db(handler)


@pytest.mark.parametrize(db_args, db_list)
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
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _load_data_from_db(handler)


@pytest.mark.parametrize(db_args, db_list)
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
    data_merged = {
        'record_id': 'aaa',
        'string': 'test123',
        'dict': {
            'aaa': 'aaa',
            'bbb': 'bbb'
        },
        'list': [
            'aaa',
            'bbb'
        ]
    }

    handler.add_data(data_1, strategy='merge')
    handler.add_data(data_2, strategy='merge')
    data = handler.data[0]

    assert len(handler) == 1
    assert all([set(data[key]) == set(data_merged[key]) for key in data_merged.keys()])


@pytest.mark.parametrize(db_args, list(filter(lambda d: d[0] in ['tinydb'], db_list)))
def test_search_tinydb(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Search on TinyDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    from tinydb import where

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

    handler.read(query=where('record_id') == 'test')
    assert len(handler) > 0

    handler.read(query=where('start_timestamp') < 1489728492.0)
    assert len(handler) > 0


@pytest.mark.parametrize(db_args, list(filter(lambda d: d[0] in ['tinymongo', 'mongodb'], db_list)))
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
    handler.read(query={'record_id': 'test'})
    assert len(handler) > 0
    handler.read(query={'record_id': {'$regex': '016'}})
    assert len(handler) > 0
    handler.read(query={'record_id': {'$regex': '^016.*'}})
    assert len(handler) > 0
    handler.read(query={
        '$and': [
            {'record_id': {'$regex': '.*'}},
            {'start_timestamp': {'$lt': 1489728492.0}}
        ]
    })
    assert len(handler) > 0

    # Python-Query-Language (PQL)
    handler.read(pql="record_id == 'test'")
    assert len(handler) > 0
    handler.read(pql="record_id == regex('test.*')")
    assert len(handler) > 0
    handler.read(query={'contents./points_concat_downsampled': {'$exists': True}})
    assert len(handler) > 0
    handler.read(pql='"contents./points_concat_downsampled" == exists(True)')
    assert len(handler) > 0
    handler.read(pql="start_timestamp > 1500000000.0")
    assert len(handler) > 0
    handler.read(
        pql='start_timestamp > 1400000000.0 '
            'and "contents./points_concat_downsampled" == exists(True)'
    )
    assert len(handler) > 0


@pytest.mark.parametrize(db_args, list(filter(lambda d: d[0] in ['mongodb'], db_list)))
def test_group_by_mongo(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Evaluate Group-by on MongoDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
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

    handler.read()
    group_keys = ['database_id', 'record_id', 'content_type', 'data_type']
    all = {k: [data[k] for data in handler.data] for k in group_keys}
    for key in group_keys:
        handler.read(group_by=key)
        grouped = [data[key] for data in handler.data]
        assert len(grouped) == len(set(all[key])), 'AssertionError: group_key: {}'.format(key)


@pytest.mark.parametrize(db_args, db_list)
def test_add_columns(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Add columns to DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
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
    data = {
        'key-int': int(0),
        'key-float': float(0.0),
        'key-str': 'str',
        'key-dict': {
            'abc': 'def'
        }
    }
    handler.add_data(data)
    for key in ['key-int', 'key-float', 'key-str', 'key-dict']:
        assert key in [c['name'] for c in handler.config['columns']]
        assert next(filter(lambda c: c['name'] == key, handler.config['columns']))['dtype'] \
               == type(data[key]).__name__  # noqa: E721
    handler.save()

    handler.read()
    for key in ['key-int', 'key-float', 'key-str', 'key-dict']:
        assert key in [c['name'] for c in handler.config['columns']]
        assert next(filter(lambda c: c['name'] == key, handler.config['columns']))['dtype'] \
               == type(data[key]).__name__  # noqa: E721
    handler.remove_data(data)
    handler.save()


@pytest.mark.parametrize(db_args, db_list)
def test_display_name(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str]
):
    """Test for display_name in configs.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        base_dir_path='/opt/pydtk/test',
        orient='path'
    )
    assert isinstance(handler, V4MetaDBHandler)

    reserved_names = ['_id', '_uuid', '_creation_time']
    names = [c for c in handler.columns if c not in reserved_names]
    display_names = [c for c in handler.df.columns.tolist() if c not in reserved_names]
    assert all([n in [c['name'] for c in handler.config['columns']] for n in names])
    assert all([n in [c['display_name'] for c in handler.config['columns']] for n in display_names])


if __name__ == '__main__':
    test_create_db(*default_db_parameter)
    test_load_db(*default_db_parameter)
    test_create_db_with_env_var(*default_db_parameter)
    test_load_db_with_env_var(*default_db_parameter)
    test_merge(*default_db_parameter)
    test_search_tinydb()
    test_search_mongo(*next(filter(lambda d: d[0] in ['tinymongo'], db_list)))
