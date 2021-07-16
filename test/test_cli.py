#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from contextlib import redirect_stdout
import io
import json
import os
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
