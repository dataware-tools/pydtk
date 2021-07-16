#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from contextlib import redirect_stdout
import io
import json
import os
import random
import sys
import tempfile

import pytest

model_file_args = 'file_path,model'
model_file_list = [
    ('test/records/json_model_test/json_test.json', 'pydtk.models.json_model.GenericJsonModel'),
    ('test/records/csv_model_test/data/test.csv', 'pydtk.models.csv.GenericCsvModel'),
    ('test/records/movie_model_test/sample.mp4', 'pydtk.models.movie.GenericMovieModel'),
]


def _assert_content(metadata, content_key):
    if 'contents' in metadata.keys() and isinstance(metadata['contents'], dict):
        assert content_key in metadata['contents'].keys()


@pytest.mark.parametrize(model_file_args, model_file_list)
def test_model_generate_metadata(file_path, model):
    """Test `pydtk model generate metadata`."""
    from pydtk.bin.sub_commands.model import Model

    cli = Model()

    # Generate metadata without specifying a model
    f = io.StringIO()
    with redirect_stdout(f):
        cli.generate(
            target='metadata',
            from_file=file_path,
            content='abc'
        )
    metadata = json.loads(f.getvalue())
    _assert_content(metadata, 'abc')

    # Generate metadata by specifying a model
    f = io.StringIO()
    with redirect_stdout(f):
        cli.generate(
            target='metadata',
            model=model,
            from_file=file_path,
            content='abc'
        )
    metadata = json.loads(f.getvalue())
    _assert_content(metadata, 'abc')


def test_db_add_and_delete_file():
    """Test `pydtk db add file` and `pydtk db delete file."""
    from pydtk.bin.sub_commands.db import DB, _get_db_handler

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
        os.environ['PYDTK_META_DB_HOST'] = tmp_dir

        cli = DB()
        record_id = 'abc'
        path = '/abc'

        # Add 1 file
        sys.stdin = io.StringIO(json.dumps({
            'record_id': record_id,
            'path': path
        }))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='file',
                database_id='pytest'
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Check the file
        f = io.StringIO()
        with redirect_stdout(f):
            cli.get(
                target='file',
                database_id='pytest',
                record_id=record_id,
                path=path,
            )
        metadata = f.getvalue()
        assert 'Found: 1 items.' in metadata

        # Overwrite the file
        sys.stdin = io.StringIO(json.dumps({
            'record_id': record_id,
            'path': path
        }))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='file',
                database_id='pytest',
                overwrite=True,
            )
        metadata = f.getvalue()
        assert 'Updated: 1 items.' in metadata

        # Overwrite the file without checking existence
        sys.stdin = io.StringIO(json.dumps({
            'record_id': record_id,
            'path': path
        }))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='file',
                database_id='pytest',
                overwrite=True,
                skip_checking_existence=True,
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Add another file
        sys.stdin = io.StringIO(json.dumps({
            'record_id': record_id,
        }))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='file',
                database_id='pytest'
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Delete the file
        sys.stdin = io.StringIO(json.dumps([
            {
                'record_id': record_id,
                'path': path
            },
            {
                'record_id': record_id,
            },
        ]))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.delete(
                target='file',
                database_id='pytest',
                yes=True,
            )
        metadata = f.getvalue()
        assert 'Deleted: 2 items.' in metadata

        # Make sure that the data was deleted
        handler, _ = _get_db_handler(target='file', database_id='pytest')
        handler.read()
        assert len(handler) == 0


def test_db_add_database():
    """Test `pydtk db add file` and `pydtk db delete database."""
    from pydtk.bin.sub_commands.db import DB, _get_db_handler

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
        os.environ['PYDTK_META_DB_HOST'] = tmp_dir

        cli = DB()
        database_id = 'abc'

        # Add 1 database
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='database',
                content=database_id,
                overwrite=True,
                skip_checking_existence=True,
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Make sure that the data was deleted
        handler, _ = _get_db_handler(target='database')
        handler.read()
        assert next(handler)['database_id'] == database_id


def test_db_add_database_from_stdin():
    """Test `pydtk db add file` and `pydtk db delete database."""
    from pydtk.bin.sub_commands.db import DB, _get_db_handler

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
        os.environ['PYDTK_META_DB_HOST'] = tmp_dir

        cli = DB()
        database_id = 'abc'

        # Add 1 database
        sys.stdin = io.StringIO(json.dumps({
            'database_id': database_id,
            'name': database_id,
            'description': 'description'
        }))
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='database',
                overwrite=True,
                skip_checking_existence=True,
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Make sure that the data was deleted
        handler, _ = _get_db_handler(target='database')
        handler.read()
        assert next(handler)['database_id'] == database_id


def test_db_add_database_2():
    """Test `pydtk db add file` and `pydtk db delete database."""
    from pydtk.bin.sub_commands.db import DB, _get_db_handler

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
        os.environ['PYDTK_META_DB_HOST'] = tmp_dir

        cli = DB()
        database_id = 'abc'

        # Add 1 database
        f = io.StringIO()
        with redirect_stdout(f):
            cli.add(
                target='database',
                database_id=database_id,
                overwrite=True,
                skip_checking_existence=True,
            )
        metadata = f.getvalue()
        assert 'Added: 1 items.' in metadata

        # Make sure that the data was deleted
        handler, _ = _get_db_handler(target='database')
        handler.read()
        assert next(handler)['database_id'] == database_id
def test_pep515():
    """Test checks for PEP515."""
    import random
    from pydtk.bin.cli import _check_pep515

    def _test(arg, is_pep515):
        try:
            _check_pep515([arg])
            if is_pep515:
                raise Exception(f'Value {arg} must be rejected')
        except ValueError:
            if not is_pep515:
                raise Exception(f'Value {arg} must not be rejected')

    def _rand_num(digits=4):
        return ''.join([str(random.randint(0, 9)) for _ in range(digits)])

    _test('1234', False)
    _test('', False)
    _test('_2983', False)
    _test('03249_', False)
    _test('a_1', False)

    for _ in range(100):
        pep515 = '_'.join([_rand_num(random.randint(1, 10)) for _ in range(random.randint(2, 10))])
        _test(pep515, True)


@pytest.mark.parametrize('record_id', [str(random.randint(0, 999999)) for _ in range(10)])
def test_list_record_id_with_only_numbers(record_id):
    """Test `pydtk db list files --record_id=<number>`."""
    import json
    import sys
    from pydtk.bin.sub_commands.db import DB

    cli = DB()

    metadata = json.dumps({
        'record_id': record_id,
        'path': "/abc",
        "contents": {}
    })

    # Add metadata with numeric record_id
    sys.stdin = io.StringIO(metadata)
    cli.add(
        target='file',
        database_id='pytest'
    )

    # Get the metadata
    f = io.StringIO()
    with redirect_stdout(f):
        cli.get(
            target='file',
            database_id='pytest',
            record_id=record_id,
            parsable=True,
        )
    metadata = json.loads(f.getvalue())
    assert len(metadata) > 0
