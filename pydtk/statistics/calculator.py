#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Base Statistic Calculation module."""

from abc import ABCMeta

import numpy as np


class UnsupportedOperationError(BaseException):
    """Error for unsupported file."""

    pass


class BaseCalculator(metaclass=ABCMeta):
    """Base Calculator."""

    def __init__(self, target_span=60.0, sync_timestamps=False):
        """Initialize Base Statistics Calculator.

        Args:
            target_span (float): interval of statistics calculation
            sync_timestamps (bool): if True, the output timestamps will
                                          start from 'timestamp // span * span'

        """
        self.target_span = target_span
        self.sync_timestamps = sync_timestamps
        self.operations = None

    def divide(self, timestamps, data):
        """Divide data with target span.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): signal to downsample

        Returns:
            divided_timestamps (ndarray): timestamps [sec]
            divided_data (list): divided data list

        """
        # Initialize the output array and buffers
        divided_timestamps, divided_data = [], []
        buffer_timestamps, buffer_data = [], []

        # Calculate the first timestamp-index
        fps_previous_index = (
            timestamps[0] // float(self.target_span) if len(timestamps) > 0 else 0
        )

        # Read over the data to store timestamps and data
        for timestamp, value in zip(timestamps, data):
            fps_current_index = timestamp // float(self.target_span)

            if fps_current_index == fps_previous_index:
                # Store data into the buffer in case of the same timestamp-index
                buffer_timestamps.append(timestamp)
                buffer_data.append(value)
            else:
                # Move data in the buffer to the output array
                if self.sync_timestamps:
                    divided_timestamps.append(fps_previous_index * self.target_span)
                else:
                    divided_timestamps.append(buffer_timestamps[0])
                divided_data.append(np.array(buffer_data))

                # Store the new data having a different timestamp-index into the buffer
                buffer_timestamps, buffer_data = [timestamp], [value]
                fps_previous_index = fps_current_index

        # Handle the data remaining in the buffer
        if len(buffer_data) > 0:
            if self.sync_timestamps:
                divided_timestamps.append(
                    buffer_timestamps[0] // self.target_span * self.target_span
                )
            else:
                divided_timestamps.append(buffer_timestamps[0])
            divided_data.append(np.array(buffer_data))

        divided_timestamps = np.array(divided_timestamps)
        return divided_timestamps, divided_data

    def mean(self, timestamps, data):
        """Divide and return means of divided data."""
        raise UnsupportedOperationError(
            "Model '{0}' does not support operation: {1}".format(
                type(self).__name__, "mean"
            )
        )

    def max(self, timestamps, data):
        """Divide and return maximum of divided data."""
        raise UnsupportedOperationError(
            "Model '{0}' does not support operation: {1}".format(
                type(self).__name__, "max"
            )
        )

    def min(self, timestamps, data):
        """Divide and return minimum of divided data."""
        raise UnsupportedOperationError(
            "Model '{0}' does not support operation: {1}".format(
                type(self).__name__, "min"
            )
        )

    def count(self, timestamps, data):
        """Divide and return count of True in divided data."""
        raise UnsupportedOperationError(
            "Model '{0}' does not support operation: {1}".format(
                type(self).__name__, "count"
            )
        )


class FloatCalculator(BaseCalculator):
    """Calculator for data of float."""

    def __init__(self, target_span=60.0, **kwargs):
        super().__init__(target_span, **kwargs)
        self.operations = ["mean", "max", "min"]

    def mean(self, timestamps, data):
        """Calculate mean.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): mean of input data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.mean(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data

    def max(self, timestamps, data):
        """Calculate max.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): max of input data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.max(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data

    def min(self, timestamps, data):
        """Calculate minimum.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): min of input data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.min(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data


class BoolCalculator(BaseCalculator):
    """Calculator for data of float."""

    def __init__(self, target_span=60.0, **kwargs):
        super().__init__(target_span, **kwargs)
        self.operations = ["mean", "max", "min"]

    def count(self, timestamps, data):
        """Count True.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): count of true data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.sum(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data

    def mean(self, timestamps, data):
        """Average of true counts during the span.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): count of true data

        """
        return self.count(timestamps, data)

    def max(self, timestamps, data):
        """Calculate max.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): max of input data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.max(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data

    def min(self, timestamps, data):
        """Calculate minimum.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): min of input data

        """
        index_timestamps, divided_data = self.divide(timestamps, data)
        data_dim = data.ndim
        stat_data = [
            values.min(axis=0) if values.ndim == data_dim else values
            for values in divided_data
        ]
        stat_data = np.array(stat_data)
        return index_timestamps, stat_data
