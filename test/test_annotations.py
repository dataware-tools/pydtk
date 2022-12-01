#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import os
import json
from typing import Optional

from pydtk.db import V4DBHandler, V4AnnotationDBHandler
import pytest
from test_db import db_args, db_list

schema_args = "schema_sample_json,drop_key"
schema_list = [
    (
        "./test/annotations/annotation_commented_image_pixel.json",
        "commented_image_pixel",
    ),
    (
        "./test/annotations/annotation_commented_image_rectangular_area.json",
        "commented_image_rectangular_area",
    ),
    ("./test/annotations/annotation_commented_point.json", "commented_point"),
]


@pytest.mark.parametrize(db_args, db_list)
@pytest.mark.parametrize(schema_args, schema_list)
def test_annotation_handler(
    db_engine: str,
    db_host: str,
    db_username: Optional[str],
    db_password: Optional[str],
    db_name: Optional[str],
    schema_sample_json: str,
    drop_key: str,
):
    """Test annotation handler.

    Args:
        db_engine (str): DB engine (e.g., 'tinydb')
        db_host (str): Host of path of DB
        db_username (str): Username
        db_password (str): Password
        db_name (str): Database name
        schema_sample_json (str): Path to schema sample json
        drop_key (str): Key to be be dropped for test

    """
    import pydantic

    handler = V4DBHandler(
        db_class="annotation",
        db_engine=db_engine,
        db_host=db_host,
        db_username=db_username,
        db_password=db_password,
        db_name=db_name,
        base_dir_path=os.path.join(os.getcwd(), "test"),
    )
    handler.read()
    assert isinstance(handler, V4AnnotationDBHandler)

    with open(schema_sample_json, "r") as f:
        annotation_dict = json.load(f)

    # test add annotation data
    handler.add_data(annotation_dict)
    handler.save()
    # test read annotation data
    handler.read()
    data = handler.data

    # data validation
    for k in annotation_dict.keys():
        assert annotation_dict[k] == data[0][k]

    # check schema
    annotation_dict.pop(drop_key)
    try:
        handler.add_data(annotation_dict)
        raise RuntimeError(f"{drop_key} is dropped but adding data was succeeded unexpectedly.")
    except pydantic.error_wrappers.ValidationError:
        pass
