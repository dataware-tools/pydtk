#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import os
from abc import ABC

import cv2

from pydtk.models import BaseModel, register_model


@register_model(priority=1)
class GenericImageModel(BaseModel, ABC):
    """A generic model for a image file."""

    _content_type = None  # allow any content-type
    _data_type = None  # allow any data-type
    _file_extensions = [".png", ".jpg"]
    _contents = None

    def __init__(self, **kwargs):
        super(GenericImageModel, self).__init__(**kwargs)

    def _load(self, path, **kwargs):
        """Load a image file.

        Args:
            path (str): path to a image file

        """
        data = cv2.imread(path)
        self.data = data

    def _save(self, path, **kwargs):
        """Save ndarray data to a image file.

        Args:
            path (str): path to the output image file

        """
        cv2.imwrite(path, self.data)

    def to_ndarray(self):
        """Return data as ndarray."""
        return self.data

    @classmethod
    def generate_contents_meta(cls, path, content_key="image"):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (dict): contents metadata

        """
        # Load file
        data = cv2.imread(path)
        shape = data.shape
        _, ext = os.path.splitext(path)

        # Generate metadata
        contents = {
            content_key: {"size": shape, "tags": ["image", ext.replace(".", "")]}
        }

        return contents

    @property
    def timestamps(self, path):
        """Return timestamps as ndarray."""
        raise NotImplementedError

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        raise NotImplementedError
