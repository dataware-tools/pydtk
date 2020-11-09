#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC

from pydtk.models import BaseModel, register_model
import numpy as np
import pandas as pd


@register_model(priority=1)
class GenericCsvModel(BaseModel, ABC):
    """A generic model for a csv file."""

    _content_type = 'text/csv'
    _data_type = None   # allow any data-type
    _file_extensions = ['.csv']
    _contents = '.*'

    def __init__(self, **kwargs):
        super(GenericCsvModel, self).__init__(**kwargs)

    def _load(self, path, start_timestamp=None, end_timestamp=None, **kwargs):
        """Load a csv file.

        Args:
            path (str): path to a csv file
            start_timestamp (float): timestamp to start loading (not supported)
            end_timestamp (float): timestamp to end loading (not supported)

        """
        if start_timestamp is not None and end_timestamp is not None:
            raise ValueError('Specifying time-range to load is not supported in GenericCsvModel')
        data = pd.read_csv(path, header=None).to_numpy()
        self.data = data

    def _save(self, path, **kwargs):
        """Save ndarray data to a csv file.

        Args:
            path (str): path to the output csv file

        """
        data = pd.DataFrame(self.data)
        data.to_csv(path, header=False, index=False)

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        # this is prototype
        return self.data

    def to_ndarray(self):
        """Return data as ndarray."""
        return self.data


@register_model(priority=2)
class CameraTimestampCsvModel(GenericCsvModel, ABC):
    """A model for a csv file containing camera timestamps."""

    _contents = {'camera/.*': {'tags': ['.*']}}
    _columns = ['timestamp']

    def __init__(self, **kwargs):
        super(GenericCsvModel, self).__init__(**kwargs)

    def _load(self, path, start_timestamp=None, end_timestamp=None, **kwargs):
        """Load a csv file.

        Args:
            path (str): path to a csv file
            start_timestamp (float): timestamp to start loading (not supported)
            end_timestamp (float): timestamp to end loading (not supported)

        """
        if start_timestamp is None:
            start_timestamp = self.metadata.data['start_timestamp']
        if end_timestamp is None:
            end_timestamp = self.metadata.data['end_timestamp']

        # load csv
        super()._load(path=path, **kwargs)

        # filter
        start_msec, end_msec = start_timestamp * 1000, end_timestamp * 1000  # sec. -> msec.
        data = self.data
        data = data[np.logical_and(data[:, 0] >= start_msec, data[:, 0] <= end_msec), 0]

        # Convert unit (msec. -> sec.)
        # Note: CSV file timestamps in "Driving behavior DB" is recorded in msec.
        data = data.astype(np.float) * pow(10, -3)

        self.data = data

    def to_ndarray(self):
        """Return data as ndarray."""
        return self.data

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        return self.data


@register_model(priority=3)
class AnnotationCsvModel(GenericCsvModel, ABC):
    """A model for a csv file containing annotations."""

    _contents = {'.*annotation': {'tags': ['.*']}}
    _data_type = "annotation"
    _columns = ['*']

    def __init__(self, **kwargs):
        super(GenericCsvModel, self).__init__(**kwargs)

    def _load(self, path, start_timestamp=None, end_timestamp=None, **kwargs):
        """Load a csv file.

        Args:
            path (str): path to a csv file

        """
        if start_timestamp is not None and end_timestamp is not None:
            raise ValueError('Specifying time-range to load is not supported in GenericCsvModel')
        data = pd.read_csv(path)
        self.data = data

    def to_ndarray(self):
        """Return data as ndarray."""
        return self.data.to_numpy()

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        return None

    @property
    def columns(self):
        """Return columns."""
        return self.data.columns.tolist()
