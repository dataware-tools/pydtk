#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import os
from typing import Optional

from pydtk.db import V4DBHandler, V4MetaDBHandler, V4DatabaseIDDBHandler
import pytest

db_args = 'db_engine,db_host,db_username,db_password,db_name'
db_list = [
    ('tinydb', 'test/test_v4.json', None, None, None),
    ('tinymongo', 'test/test_v4', None, None, None),
    ('montydb', 'test/test_v4', None, None, None),
    # ('mongodb', '<host>', '<username>', '<password>', '<database>')
]
default_db_parameter = db_list[0]


@pytest.fixture(autouse=True)
def _clean_env():
    import os
    import shutil
    try:
        os.remove('test/test_v4.json')
    except FileNotFoundError:
        pass
    shutil.rmtree('test/test_v4', ignore_errors=True)
    yield


def _add_files_to_db(handler: V4DBHandler):
    from pydtk.models import MetaDataModel

    paths = [
        'test/records/sample/data/records.bag.json',
        'test/records/csv_model_test/data/test.csv.json',
        'test/records/json_model_test/json_test.json.json',
        'test/records/forecast_model_test/forecast_test.csv.json',
        'test/records/annotation_model_test/annotation_test.csv.json'
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_file(metadata.data)

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
    db_password: Optional[str],
    db_name: Optional[str],
):
    """Create DB of records directory.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test")
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _add_files_to_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_rename_db(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str],
):
    """Rename DB of records directory.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        database_id="old",
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _add_files_to_db(handler)
    handler.rename_database_id("new")

    # check old one is empty
    old_handler = V4MetaDBHandler(
        database_id="old",
    )
    old_handler.read()
    assert len(old_handler.data) == 0

    new_handler = V4MetaDBHandler(
        database_id="new",
    )
    new_handler.read()
    assert len(new_handler.data) > 0


def test_validate_schema_file():
    """Validate schema."""
    handler = V4DBHandler(
        db_class='meta',
    )
    handler.add_data({
        '_api_version': 'dataware-tools.com/V1Alpha1',
        '_kind': 'File',
        'record_id': 'record_id',
        'path': 'path',
    })

    from pydtk.db.exceptions import SchemaNotFoundError
    with pytest.raises(SchemaNotFoundError):
        handler.add_data({
            '_api_version': 'dummy',
            '_kind': 'File',
            'record_id': 'record_id',
            'path': 'path',
        })
    with pytest.raises(SchemaNotFoundError):
        handler.add_data({
            '_api_version': 'dataware-tools.com/V1Alpha1',
            '_kind': 'dummy',
            'record_id': 'record_id',
            'path': 'path',
        })

    from pydantic.error_wrappers import ValidationError
    # Missing path
    with pytest.raises(ValidationError):
        handler.add_data({
            '_api_version': 'dataware-tools.com/V1Alpha1',
            '_kind': 'File',
            'record_id': 'record_id',
        })
    # Ignore extra field
    handler.add_data({
        '_api_version': 'dataware-tools.com/V1Alpha1',
        '_kind': 'File',
        'record_id': 'record_id',
        'path': 'path',
        'additional_field': 'additional_field',
    })


def test_validate_schema_record():
    """Validate schema."""
    handler = V4DBHandler(
        db_class='meta',
    )
    handler.add_data({
        '_api_version': 'dataware-tools.com/V1Alpha1',
        '_kind': 'Record',
        'record_id': 'record_id',
    })

    from pydtk.db.exceptions import SchemaNotFoundError
    with pytest.raises(SchemaNotFoundError):
        handler.add_data({
            '_api_version': 'dummy',
            '_kind': 'Record',
            'record_id': 'record_id',
        })
    with pytest.raises(SchemaNotFoundError):
        handler.add_data({
            '_api_version': 'dataware-tools.com/V1Alpha1',
            '_kind': 'dummy',
            'record_id': 'record_id',
        })

    from pydantic.error_wrappers import ValidationError
    # Missing record_id
    with pytest.raises(ValidationError):
        handler.add_data({
            '_api_version': 'dataware-tools.com/V1Alpha1',
            '_kind': 'Record',
        })
    # Ignore extra field
    handler.add_data({
        '_api_version': 'dataware-tools.com/V1Alpha1',
        '_kind': 'Record',
        'record_id': 'record_id',
        'additional_field': 'additional_field',
    })


@pytest.mark.parametrize(db_args, db_list)
def test_load_db(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents'
    )
    _add_files_to_db(handler)
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _load_data_from_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_load_database_id(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    metadata_handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents'
    )
    _add_files_to_db(metadata_handler)

    handler = V4DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
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
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    _handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents'
    )
    assert 'pytest' not in _handler.config.keys()

    # Update config and save it
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
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
    handler.config['pytest'] = 'abc'
    handler.save()

    # Make sure that the config is saved
    del handler
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents'
    )
    handler.read()
    assert handler.config['columns'][-1]['name'] == 'test'
    del handler.config['columns'][-1]
    handler.save()

    # Make sure that config is loaded from DB on read()
    _handler.read()
    assert 'pytest' in _handler.config.keys()


@pytest.mark.parametrize(db_args, db_list)
def test_delete_files(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Delete records from DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='record_id'
    )
    _add_files_to_db(handler)
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)

    assert len(handler) == handler.count_total

    num_data = len(handler)

    # Remove one record without saving
    handler.remove_file(next(handler))
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


@pytest.mark.parametrize(db_args, db_list)
def test_delete_collection(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Delete a collection from DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    metadata_handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='record_id'
    )
    _add_files_to_db(metadata_handler)

    handler = V4DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
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
            db_name=db_name,
        )
        assert isinstance(meta_handler, V4MetaDBHandler)
        assert len(meta_handler) == 0


@pytest.mark.parametrize(db_args, db_list)
def test_create_db_with_env_var(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Create DB of records directory.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

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
    if db_name is not None:
        os.environ['PYDTK_META_DB_DATABASE'] = db_name

    handler = V4DBHandler(
        db_class='meta',
        base_dir_path=os.path.join(os.getcwd(), "test")
    )
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _add_files_to_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_load_db_with_env_var(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Load DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

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
    if db_name is not None:
        os.environ['PYDTK_META_DB_DATABASE'] = db_name

    handler = V4DBHandler(db_class='meta')
    _add_files_to_db(handler)

    del handler
    handler = V4DBHandler(db_class='meta')
    handler.read()
    assert isinstance(handler, V4MetaDBHandler)
    _load_data_from_db(handler)


@pytest.mark.parametrize(db_args, db_list)
def test_merge(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test merging dicts.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
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


@pytest.mark.parametrize(db_args, db_list)
def test_file_operations(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test merging dicts.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(db_class='meta')

    data_1 = {'path': 'path_01'}
    data_2 = {'path': 'path_02'}

    handler.add_file(data_1)
    handler.add_file(data_2)
    assert len(handler) == 2
    handler.remove_file(data_2)
    assert len(handler) == 1


@pytest.mark.parametrize(db_args, db_list)
def test_record_operations(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test merging dicts.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(db_class='meta')

    data_1 = {'record_id': 'record_01'}
    data_2 = {'record_id': 'record_02', 'path': ''}

    handler.add_record(data_1)
    handler.add_record(data_2)
    assert len(handler) == 2
    handler.remove_record(data_2)
    assert len(handler) == 1


@pytest.mark.parametrize(db_args, list(filter(lambda d: d[0] in ['tinydb'], db_list)))
def test_search_tinydb(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Search on TinyDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    from tinydb import where

    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents',
        read_on_init=False
    )
    _add_files_to_db(handler)

    handler.read(query=where('record_id') == 'test')
    assert len(handler) > 0

    handler.read(query=where('start_timestamp') < 1489728492.0)
    assert len(handler) > 0


@pytest.mark.parametrize(
    db_args,
    list(filter(lambda d: d[0] in ['tinymongo', 'mongodb', 'montydb'], db_list))
)
def test_search_mongo(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Search on MongoDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents',
        read_on_init=False
    )
    _add_files_to_db(handler)

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
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Evaluate Group-by on MongoDB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='contents',
        read_on_init=False
    )

    handler.read()
    group_keys = ['record_id']
    all = {k: [data[k] for data in handler.data] for k in group_keys}
    for key in group_keys:
        handler.read(group_by=key)
        grouped = [data[key] for data in handler.data]
        assert len(grouped) == len(set(all[key])), 'AssertionError: group_key: {}'.format(key)


@pytest.mark.parametrize(
    db_args,
    list(filter(lambda d: d[0] in ['mongodb', 'montydb'], db_list))
)
def test_limit_mongo(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test for limit.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='file',
        read_on_init=False
    )
    _add_files_to_db(handler)

    handler.read(limit=1)
    assert len(handler) == 1
    handler.read(limit=2)
    assert len(handler) == 2


@pytest.mark.parametrize(db_args, db_list)
def test_add_columns(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Add columns to DB.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
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
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test for display_name in configs.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='path'
    )
    assert isinstance(handler, V4MetaDBHandler)

    reserved_names = ['_id', '_uuid', '_creation_time']
    names = [c for c in handler.columns if c not in reserved_names]
    display_names = [c for c in handler.df.columns.tolist() if c not in reserved_names]
    assert all([n in [c['name'] for c in handler.config['columns']] for n in names])
    assert all([n in [c['display_name'] for c in handler.config['columns']] for n in display_names])


@pytest.mark.parametrize(db_args, db_list)
def test_read_with_offset(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test for reading database with offset.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    handler = V4DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        orient='path'
    )
    assert isinstance(handler, V4MetaDBHandler)
    _add_files_to_db(handler)

    handler.read(offset=0)
    assert handler.df.index[0] == 0
    handler.read(offset=1)
    assert handler.df.index[0] == 1
    handler.read(offset=1, limit=1)
    assert handler.df.index[0] == 1


@pytest.mark.parametrize(db_args, db_list)
def test_db_handler_dtype(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test for checking data-types handled by DBHandler.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name

    """
    from datetime import datetime
    from pydtk.db import DBHandler

    handler = DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
    )
    _add_files_to_db(handler)

    del handler
    handler = DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
    )
    handler.add_data({
        'record_id': 1,
        'path': 'abc',
        'contents': {},
        'new_column_str': '',
        'new_column_int': 1,
        'new_column_float': 1.234,
        'new_column_list': [],
        'new_column_dict': {},
        'new_column_datetime': datetime.fromtimestamp(0)
    })
    assert isinstance(handler.data[0]['record_id'], str)
    assert isinstance(handler.data[0]['path'], str)
    assert isinstance(handler.data[0]['contents'], dict)
    assert isinstance(handler.data[0]['new_column_str'], str)
    assert isinstance(handler.data[0]['new_column_int'], int)
    assert isinstance(handler.data[0]['new_column_float'], float)
    assert isinstance(handler.data[0]['new_column_list'], list)
    assert isinstance(handler.data[0]['new_column_dict'], dict)
    assert isinstance(handler.data[0]['new_column_datetime'], datetime)
    handler.save()

    handler.read(pql='"record_id" == regex(".*")')
    assert len(handler) > 0


@pytest.mark.parametrize(
    db_args, list(filter(lambda d: d[0] in ['mongodb', 'montydb', 'tinydb'], db_list))
)
def test_remove_database_id(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str]
):
    """Test `drop_table` function."""
    from pydtk.db import DBHandler

    # Create a database with database-id 'pytest'
    metadata_handler = DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        database_id='pytest'
    )
    _add_files_to_db(metadata_handler)
    metadata_handler.config['pytest'] = 'abc'
    metadata_handler.save()

    # Load database-id handler
    handler = DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
    )
    handler.read()
    assert len(list(filter(lambda x: x['database_id'] == 'pytest', handler.data))) > 0

    # Remove database-id 'pytest' (in-memory)
    database_info_to_remove = next(filter(lambda x: x['database_id'] == 'pytest', handler.data))
    handler.remove_data(database_info_to_remove)

    # Make sure that no resources are changed on the remote DB
    _handler = DBHandler(
        db_class='database_id',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
    )
    _handler.read()
    assert len(list(filter(lambda x: x['database_id'] == 'pytest', _handler.data))) > 0
    _metadata_handler = DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        database_id='pytest'
    )
    _metadata_handler.read()
    assert len(_handler) > 0

    # Reflect the removal of database-id 'pytest' to the remote DB
    handler.save()

    # Confirm that the resources are removed on the remote DB
    _handler.read()
    assert len(list(filter(lambda x: x['database_id'] == 'pytest', _handler.data))) == 0
    _metadata_handler.read()
    assert len(_metadata_handler) == 0

    # Confirm that the corresponding config is also deleted
    _metadata_handler = DBHandler(
        db_class='meta',
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
        database_id='pytest'
    )
    assert 'pytest' not in _metadata_handler.config.keys()


def test_get_schema():
    """Test `get_schema` function."""
    from pydtk.db.schemas import get_schema
    cases = [
        ("dataware-tools.com/V1Alpha1", "File"),
        ("dataware-tools.com/V1Alpha1", "Record"),
        ("dataware-tools.com/V1Alpha1", "Annotation"),
        ("dataware-tools.com/V1Alpha1", "AnnotationCommentedPoint"),
        ("dataware-tools.com/V1Alpha1", "AnnotationCommentedImagePixel"),
        ("dataware-tools.com/V1Alpha1", "AnnotationCommentedImageRectanglerArea"),
    ]
    for api_version, kind in cases:
        schema = get_schema(api_version, kind)
        assert schema._kind.lower() == kind.lower()
        assert schema._api_version.lower() == api_version.lower()

    from pydtk.db.exceptions import SchemaNotFoundError
    with pytest.raises(SchemaNotFoundError):
        get_schema('dummy', 'dummy')
