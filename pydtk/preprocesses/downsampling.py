#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Downsample preprocessing function."""

import numpy as np

from .preprocess import BasePreprocess


class Downsample(BasePreprocess):
    """Downsample processing."""

    def __init__(self, target_frame_rate, mode="skipping"):
        """Downsampling initialization.

        Args:
            target_frame_rate (float): target frame rate in Hz
            mode (str): 'skipping' or 'averaging'

        """
        super(Downsample, self)
        assert mode in ["skipping", "averaging"]
        self.target_frame_rate = target_frame_rate
        self.mode = mode

    def processing(self, timestamps, values):
        """Downsample data into the target sampling rate.

        Args:
            timestamps (ndarray): timestamps [sec]
            values (ndarray): signal to downsample

        Returns:
            downsampled_timestamps (ndarray): timestamps [sec]
            downsampled_values (ndarray): signal to downsample

        """
        downsampled_timestamps = []
        downsampled_values = []
        buffer_timestamps = []
        buffer_values = []
        fps_previous_index = 0
        for timestamp, value in zip(timestamps, values):
            fps_current_index = timestamp // (1.0 / float(self.target_frame_rate))
            if fps_current_index == fps_previous_index:
                if self.mode == "averaging":
                    buffer_timestamps.append(timestamp)
                    buffer_values.append(value)
                continue
            else:
                fps_previous_index = fps_current_index

            if self.mode == "skipping":
                downsampled_timestamps.append(timestamp)
                downsampled_values.append(value)
            elif self.mode == "averaging":
                if len(buffer_values) > 1:
                    downsampled_timestamps.append(
                        buffer_timestamps[int(len(buffer_timestamps) // 2)]
                    )
                    downsampled_values.append(np.array(buffer_values).mean(axis=0))
                else:
                    downsampled_timestamps.append(timestamp)
                    downsampled_values.append(value)
                buffer_timestamps = []
                buffer_values = []
            else:
                continue

        downsampled_timestamps = np.array(downsampled_timestamps)
        downsampled_values = np.array(downsampled_values)

        return downsampled_timestamps, downsampled_values
