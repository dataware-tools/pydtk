#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test make-meta with pytest."""

_template_with_contents = {
    'contents': {}
}


def test_csv():
    """Create metadata of a csv file."""
    from pydtk.bin.make_meta import make_meta

    path = 'test/records/annotation_model_test/annotation_test.csv'
    metadata = make_meta(path)

    assert isinstance(metadata, dict)
    assert 'path' in metadata.keys()
    assert metadata['path'] == path


def test_csv_with_contents():
    """Create metadata of a csv file."""
    from pydtk.bin.make_meta import make_meta

    path = 'test/records/annotation_model_test/annotation_test.csv'
    metadata = make_meta(path, _template_with_contents)

    assert isinstance(metadata, dict)
    assert 'contents' in metadata.keys()
    # assert isinstance(metadata['contents'], dict)   # FIXME: Remove comment-out
