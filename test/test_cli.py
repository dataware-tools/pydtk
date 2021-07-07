#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from contextlib import redirect_stdout
import io
import json

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
