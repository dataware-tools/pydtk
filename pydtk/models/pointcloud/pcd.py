#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import sys
from abc import ABC

import numpy as np
from pandas import DataFrame
from pyntcloud import PyntCloud
from pypcd import pypcd

from pydtk.models import BaseModel, register_model


@register_model(priority=1)
class PCDModel(BaseModel, ABC):
    """A generic model for a .pcd file."""

    _content_type = None  # allow any content-type
    _data_type = None  # allow any data-type
    _file_extensions = [".pcd"]
    _contents = ".*"
    _columns = ["x", "y", "z"]
    data: DataFrame

    def __init__(self, **kwargs):
        super(PCDModel, self).__init__(**kwargs)

    def _load(self, path, start_timestamp=None, end_timestamp=None, **kwargs):
        """Load a csv file.

        Args:
            path (str): path to a csv file
            start_timestamp (float): timestamp to start loading (not supported)
            end_timestamp (float): timestamp to end loading (not supported)

        """
        if start_timestamp is not None and end_timestamp is not None:
            sys.stderr.write(
                "Warning: Specifying time-range to load is not supported in PCDModel\n"
            )
        try:
            cloud = PyntCloud.from_file(path)
            data = cloud.points
            columns = data.columns.tolist()
        except NotImplementedError:
            cloud = pypcd.PointCloud.from_path(path)
            data = DataFrame(cloud.pc_data)
            columns = cloud.fields
        self.data = data
        self._columns = columns

    def _save(self, path, compression="binary", **kwargs):
        """Save ndarray data to a csv file.

        Args:
            path (str): path to the output csv file

        """
        # TODO(hdl-members): Use pyntcloud instead of pypcd when saving .pcd files is supported

        assert type(self.data) == DataFrame
        dtypes = [dtype.name for dtype in self.data.dtypes.tolist()]
        points = np.core.records.fromarrays(
            self.to_ndarray().transpose(),
            names=", ".join(self._columns),
            formats=", ".join(dtypes),
        )
        cloud = pypcd.PointCloud.from_array(points)
        cloud.save_pcd(path, compression=compression)

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        return np.array([0])

    def to_ndarray(self):
        """Return data as ndarray."""
        return self.data.to_numpy()

    def from_ndarray(self, data, columns=None):
        """Set data from ndarray.

        Args:
            data (np.array): data

        """
        if columns is None:
            columns = self._columns
        assert len(data.shape) == 2
        assert len(columns) == data.shape[-1]

        self.data = DataFrame(data, columns=columns)
        self._columns = columns

    @classmethod
    def generate_contents_meta(cls, path, content_key="content"):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (dict): contents metadata

        """
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
