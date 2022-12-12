#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import json
from abc import ABC

import numpy as np

from pydtk.models import BaseModel, register_model


@register_model(priority=1)
class GenericJsonModel(BaseModel, ABC):
    """A generic model for a json file."""

    _content_type = None  # allow any content-type
    _data_type = None  # allow any data-type
    _file_extensions = [".json"]
    _contents = None

    def __init__(self, **kwargs):
        super(GenericJsonModel, self).__init__(**kwargs)

    def _load(self, path, **kwargs):
        """Load a json file.

        Args:
            path (str): path to a json file
        """
        with open(path, "r") as fp:
            data = json.load(fp)
        self.data = data

    def _save(self, path, **kwargs):
        """Save dict data to a json file.

        Args:
            path (str): path to the output json file

        """
        with open(path, "w") as fp:
            json.dump(self.data, fp)

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        if "time_stamps" in self.data.keys():
            return np.ndarray(self.data["time_stamps"])

        return np.ndarray([])

    def to_ndarray(self):
        """Json model do not support to_ndarray."""
        raise AttributeError(
            "JsonModel do not support to_ndarray, since it may be semi-structured"
        )

    @classmethod
    def generate_contents_meta(cls, path, content_key="content"):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (dict): contents metadata

        """
        # Load file
        with open(path, "r") as p:
            data = json.load(p)

        if "contents" in data.keys():
            raise AttributeError(f"File '{path}' itself is a metadata")

        # Generate metadata
        contents = {content_key: {"keys": list(data.keys()), "tags": ["json"]}}
        return contents

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        raise NotImplementedError
