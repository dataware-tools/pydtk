#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import pytest

model_file_args = 'file_path,model'
model_file_list = [
    ('test/records/json_model_test/json_test.json', 'pydtk.models.json_model.GenericJsonModel'),
    ('test/records/csv_model_test/data/test.csv', 'pydtk.models.csv.GenericCsvModel'),
    ('test/records/movie_model_test/sample.mp4', 'pydtk.models.movie.GenericMovieModel'),
]


@pytest.mark.parametrize(model_file_args, model_file_list)
def test_model_generate_metadata(file_path, model):
    """Test `pydtk model generate metadata`."""
    from pydtk.bin.sub_commands.model import Model

    cli = Model()

    # Generate metadata without specifying a model
    cli.generate(
        target='metadata',
        from_file=file_path
    )

    # Generate metadata by specifying a model
    cli.generate(
        target='metadata',
        model=model,
        from_file=file_path
    )
