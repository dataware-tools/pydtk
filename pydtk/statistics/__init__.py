#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Base Statistic Calculation module."""

from abc import ABCMeta

import pandas as pd

from pydtk.statistics import calculator


class BaseStatisticCalculation(metaclass=ABCMeta):
    """Base Statistic Calculation."""

    def __init__(self, target_span=60.0, sync_timestamps=False):
        """Initialize Base Statistics Calculation class.

        Args:
            target_span (float): interval of statistics calculation
            sync_timestamps (bool): if True, the output timestamps will
                                          start from 'timestamp // span * span'

        """
        self.target_span = target_span
        self.sync_timestamps = sync_timestamps

    def _get_calculator(self, dtype):
        """Get calculator by data type.

        Args:
            dtype (string): dtype of ndarray

        Returns:
            dtype_calculator (object): calculator class

        """
        kwargs = {
            "target_span": self.target_span,
            "sync_timestamps": self.sync_timestamps,
        }
        if "bool" in dtype:
            dtype_calculator = getattr(calculator, "BoolCalculator")(**kwargs)
        else:
            dtype_calculator = getattr(calculator, "FloatCalculator")(**kwargs)
        return dtype_calculator

    def calculate(self, timestamps, data, operation):
        """Divide and calculate statistics of divided data.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data
            operation (str): operation

        Returns:
            index_timestamps (ndarray): timestamps [sec]
            stat_data (ndarray): mean of input data

        """
        self.calculator = self._get_calculator(str(data.dtype))
        index_timestamps, stat_data = getattr(self.calculator, operation)(
            timestamps, data
        )
        return index_timestamps, stat_data

    def mean(self, timestamps, data):
        """Divide and return means of divided data."""
        return self.calculate(timestamps, data, "mean")

    def max(self, timestamps, data):
        """Divide and return maximum of divided data."""
        return self.calculate(timestamps, data, "max")

    def min(self, timestamps, data):
        """Divide and return minimum of divided data."""
        return self.calculate(timestamps, data, "min")

    def count(self, timestamps, data):
        """Divide and return count of True in divided data."""
        return self.calculate(timestamps, data, "count")

    def statistic_tables(self, timestamps, data, columns):
        """Make statistic tables.

        Args:
            timestamps (ndarray): timestamps [sec]
            data (ndarray): input data
            columns (str): columns of table

        Returns:
            df_dict (dict): dict includes statistic DataFrames

        """
        self.calculator = self._get_calculator(str(data.dtype))
        for i, operation in enumerate(self.calculator.operations):
            index_timestamps, stat_data = self.calculate(timestamps, data, operation)
            time_df = pd.DataFrame(data=index_timestamps, columns=["timestamp"])
            if i == 0:
                stat_df = time_df
            if stat_data.ndim == 1:
                stat_data = stat_data.reshape(-1, 1)
            ope_columns = [column + "/" + operation for column in columns]
            _stat_df = pd.DataFrame(data=stat_data, columns=ope_columns)
            _stat_df = pd.concat([time_df, _stat_df], axis=1)
            stat_df = pd.merge(stat_df, _stat_df, on="timestamp")
        return stat_df
