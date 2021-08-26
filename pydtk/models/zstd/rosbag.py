#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC

import pyzstd

from pydtk.models import register_model
from pydtk.models.rosbag import GenericRosbagModel as _GenericRosbagModel


@register_model(priority=1)
class GenericZstdRosbagModel(_GenericRosbagModel, ABC):
    """A generic model for a zstandard rosbag file."""

    _file_extensions = ['.zst']

    def _load(self, path, **kwargs):
        """Load a zstandard rosbag file.

        Args:
            path (str): path to a rosbag file

        """
        with pyzstd.open(path, 'rb') as f:
            f.mode = 'rb'
            super()._load(path=f, **kwargs)

    def _load_as_generator(self, path, **kwargs):
        """Load a zstandard rosbag file for each sample.

        Args:
            path (str): path to a rosbag file

        """
        with pyzstd.open(path, 'rb') as f:
            f.mode = 'rb'
            yield super()._load_as_generator(path=f, **kwargs)

    def _save(self, path, contents=None, **kwargs):
        """Save ndarray data to a zstandard rosbag file.

        Args:
            path (str): path to the output zst file
            contents (str or dict): topic name

        """
        # TODO: implementation
        pass

    @classmethod
    def generate_contents_meta(cls, path, **kwargs):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (dict): contents metadata

        """
        with pyzstd.open(path, 'rb') as f:
            f.mode = 'rb'
            return super().generate_contents_meta(path=f, **kwargs)

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        with pyzstd.open(path, 'rb') as f:
            f.mode = 'rb'
            return super().generate_timestamp_meta(path=f)
