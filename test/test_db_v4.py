#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import pytest


def test_create_db():
    """Create DB of records directory."""
    from pydtk.db import V4DBHandler
    from pydtk.models import MetaDataModel

    handler = V4DBHandler(
        db_class='meta',
        db_engine='tinydb',
        db_host='test/test_v4.json',
        base_dir_path='/opt/pydtk/test'
    )

    paths = [
        'test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json',
        'test/records/B05_17000000010000000829/data/records.bag.json',
        'test/records/sample/data/records.bag.json',
        'test/records/meti2019/ssd7.bag.json',
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)

    # Save
    handler.save()


def test_load_db():
    """Load DB."""
    from pydtk.db import V4DBHandler

    handler = V4DBHandler(
        db_class='meta',
        db_engine='tinydb',
        db_host='test/test_v4.json',
        base_dir_path='/opt/pydtk/test',
        orient='contents'
    )

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


def test_create_db_with_env_var():
    """Create DB of records directory."""
    import os
    from pydtk.db import V4DBHandler
    from pydtk.models import MetaDataModel

    # Set environment variables
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinydb'
    os.environ['PYDTK_META_DB_HOST'] = 'test/test_v4_env.json'

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


def test_load_db_with_env_var():
    """Load DB."""
    import os
    from pydtk.db import V4DBHandler

    # Set environment variables
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinydb'
    os.environ['PYDTK_META_DB_HOST'] = 'test/test_v4_env.json'

    handler = V4DBHandler(db_class='meta')

    try:
        for sample in handler:
            print(sample)
    except EOFError:
        pass


def test_merge():
    """Test merging dicts."""
    from pydtk.db import V4DBHandler

    handler = V4DBHandler(
        db_class='meta',
        db_engine='tinydb',
        db_host='test/test_v4.json',
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


if __name__ == '__main__':
    test_create_db()
    test_load_db()
    # test_create_db_with_env_var()
    # test_load_db_with_env_var()
    # test_merge()
