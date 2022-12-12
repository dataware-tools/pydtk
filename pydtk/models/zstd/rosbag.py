#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC

import cv2
import numpy as np
import pyzstd
import ros_numpy
import sensor_msgs.msg

from pydtk.models import register_model
from pydtk.models.rosbag import GenericRosbagModel as _GenericRosbagModel


@register_model(priority=1)
class GenericZstdRosbagModel(_GenericRosbagModel, ABC):
    """A generic model for a zstandard rosbag file."""

    _file_extensions = [".zst"]

    def _load(self, path, **kwargs):
        """Load a zstandard rosbag file.

        Args:
            path (str): path to a rosbag file

        """
        with pyzstd.open(path, "rb") as f:
            f.mode = "rb"
            super()._load(path=f, **kwargs)

    def _load_as_generator(self, path, **kwargs):
        """Load a zstandard rosbag file for each sample.

        Args:
            path (str): path to a rosbag file

        """
        with pyzstd.open(path, "rb") as f:
            f.mode = "rb"
            generator = super()._load_as_generator(path=f, **kwargs)
            for sample in generator:
                yield sample

    def _save(self, path, contents=None, **kwargs):
        """Save ndarray data to a zstandard rosbag file.

        Args:
            path (str): path to the output zst file
            contents (str or dict): topic name

        """
        # TODO(hdl-members): implementation
        pass

    @classmethod
    def generate_contents_meta(cls, path, **kwargs):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (dict): contents metadata

        """
        with pyzstd.open(path, "rb") as f:
            f.mode = "rb"
            return super().generate_contents_meta(path=f, **kwargs)

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        with pyzstd.open(path, "rb") as f:
            f.mode = "rb"
            return super().generate_timestamp_meta(path=f)


@register_model(priority=2)
class SensorMsgsCompressedImageZstdRosbagModel(GenericZstdRosbagModel, ABC):
    """A model for a rosbag file containing sensor_msgs/Range."""

    _contents = {".*": {"msg_type": "sensor_msgs/CompressedImage"}}
    _columns = ["red", "green", "blue"]

    @staticmethod
    def msg_to_data(msg, resize_rate=1.0, **kwargs):
        """Convert a message to data."""
        jpg = np.fromstring(msg.data, np.uint8)
        image = cv2.imdecode(jpg, cv2.IMREAD_COLOR)
        if resize_rate != 1.0:
            image = cv2.resize(
                image,
                dsize=None,
                fx=resize_rate,
                fy=resize_rate,
                interpolation=cv2.INTER_LINEAR,
            )
        image = image[:, :, ::-1]  # Convert BGR to RGB
        image = image.transpose((2, 0, 1))  # Reshape: [H, W, C] -> [C, H, W]
        return image

    def to_ndarray(self):
        """Return data as ndarray."""
        return np.array(self.data["data"])


@register_model(priority=2)
class SensorMsgsPointCloud2ZstdRosbagModel(GenericZstdRosbagModel, ABC):
    """A model for a rosbag file containing sensor_msgs/PointCloud2."""

    _contents = {".*": {"msg_type": "sensor_msgs/PointCloud2"}}
    _config = {"fields": ("x", "y", "z")}

    def __init__(self, fields=("x", "y", "z"), **kwargs):
        super(SensorMsgsPointCloud2ZstdRosbagModel, self).__init__(**kwargs)
        self._config["fields"] = fields

    def msg_to_data(self, msg, **kwargs):
        """Convert a message to data."""
        if msg.__class__.__name__ == "_sensor_msgs__PointCloud2":
            msg.__class__ = sensor_msgs.msg._PointCloud2.PointCloud2
        points = ros_numpy.numpify(msg)[list(self._config["fields"])]
        pointcloud = np.array(points.tolist())
        if "intensity" in self._config["fields"]:
            pointcloud[
                :, self._config["fields"].index("intensity")
            ] /= 255.0  # scale to [0, 1]
        return pointcloud

    def to_ndarray(self):
        """Return data as ndarray."""
        return np.array(self.data["data"], dtype="object")

    @property
    def columns(self):
        """Return columns."""
        return list(self._config["fields"])
