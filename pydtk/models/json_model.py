#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC

from pydtk.models import BaseModel, register_model
import json
import numpy as np


@register_model(priority=1)
class GenericJsonModel(BaseModel, ABC):
    """A generic model for a json file."""

    _content_type = "application/json"
    _data_type = None  # allow any data-type
    _file_extensions = [".json"]
    _contents = ".*"

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
