#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import logging
from abc import ABC

import numpy as np
import rosbag2_py
from pandas import DataFrame
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from pydtk.models import BaseModel, register_model


def get_rosbag_options(path, serialization_format="cdr"):
    """Get rosbag options for reader."""
    storage_options = rosbag2_py.StorageOptions(uri=path, storage_id="sqlite3")

    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format=serialization_format,
        output_serialization_format=serialization_format,
    )

    return storage_options, converter_options


@register_model(priority=1)
class GenericRosbag2Model(BaseModel, ABC):
    """A generic model for a rosbag2 file."""

    _content_type = None  # allow any content-type
    _data_type = None  # allow any data-type
    _file_extensions = None
    _contents = None

    def __init__(self, **kwargs):
        super(GenericRosbag2Model, self).__init__(**kwargs)

    @classmethod
    def _is_loadable(cls, path="", **kwargs):
        """Check a given rosbag is openable."""
        try:
            reader = rosbag2_py.SequentialReader()
            reader.open(*get_rosbag_options(path))
            del reader
            return True
        except Exception as e:
            logging.exception(e)
            return False

    def _load(
        self,
        path,
        contents=None,
        start_timestamp=None,
        end_timestamp=None,
        target_frame_rate=None,
        **kwargs,
    ):
        """Load a rosbag2 file.

        Args:
            path (str): path to a rosbag2 file
            contents (str or dict): topic name to load
            start_timestamp (float): timestamp to start loading in sec
            end_timestamp (float): timestamp to end loading in sec

        """
        topic = None
        if isinstance(contents, str):
            topic = contents
        if isinstance(contents, dict):
            topic = next(iter(contents))
        if topic is None:
            raise ValueError('Topic name must be specified by the argument "contents"')

        # NOTE(kan-bayashi): seek() is not available in current version (2022/12/08) with apt,
        #   and therefore, we do not use start_timestamp temporary.
        if start_timestamp is not None:
            self.logger.warning("start_timestamp is not supported. ignored.")
            start_timestamp = None

        # Load rosbag2
        reader = rosbag2_py.SequentialReader()
        reader.open(*get_rosbag_options(path))

        # Seek timestamp if needed
        if start_timestamp is not None:
            # NOTE(kan-bayashi): seek() is not available in current version (2022/12/08) with apt
            raise NotImplementedError()
            # TODO(kan-bayashi): What happened when start timestamp if exceed end timestamp?
            reader.seek(int(start_timestamp * 10 ** 9))

        # Create mapping dict of topic name and type
        topic_types = reader.get_all_topics_and_types()
        type_map = {
            topic_types[i].name: topic_types[i].type for i in range(len(topic_types))
        }
        assert topic in type_map, f"topic {topic} is not included in rosbag."

        # Set filter
        storage_filter = rosbag2_py.StorageFilter(topics=[topic])
        reader.set_filter(storage_filter)

        if target_frame_rate is not None:
            data, timestamps = [], []
            while True:
                if not reader.has_next():
                    break
                timestamp_in_nsec = reader.read_next()[2]
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                timestamps.append(timestamp)
            timestamps = self.downsample_timestamps(timestamps, target_frame_rate)
            if start_timestamp is not None:
                # TODO(kan-bayashi): seek() is not available in current version (2022/12/08) with apt
                raise NotImplementedError()
                reader.seek(int(start_timestamp * 10 ** 9))
            else:
                # TODO(kan-bayashi): Replace with seek()
                del reader
                reader = rosbag2_py.SequentialReader()
                reader.open(*get_rosbag_options(path))
            timestamp_idx = 0
            while True:
                if not reader.has_next():
                    break
                (topic_, data_, timestamp_in_nsec) = reader.read_next()
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                if timestamp == timestamps[timestamp_idx]:
                    msg_type = get_message(type_map[topic_])
                    msg = deserialize_message(data_, msg_type)
                    data.append(msg.data)
                    timestamp_idx += 1
                if timestamp_idx == len(timestamps):
                    break
        else:
            timestamps, data = [], []
            while True:
                if not reader.has_next():
                    break
                (topic_, data_, timestamp_in_nsec) = reader.read_next()
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                msg_type = get_message(type_map[topic_])
                msg = deserialize_message(data_, msg_type)
                timestamps.append(float(timestamp))
                # NOTE(kan-bayashi): Need to convert?
                data.append(msg.data)

        self.data = {"timestamps": timestamps, "data": data}

    def _load_as_generator(
        self,
        path,
        contents=None,
        start_timestamp=None,
        end_timestamp=None,
        target_frame_rate=None,
        **kwargs,
    ):
        """Load a rosbag2 file for each sample.

        Args:
            path (str): path to a rosbag2 file
            contents (str or dict): topic name to load
            start_timestamp (float): timestamp to start loading (not supported)
            end_timestamp (float): timestamp to end loading (not supported)

        """
        topic = None
        if isinstance(contents, str):
            topic = contents
        if isinstance(contents, dict):
            topic = next(iter(contents))
        if topic is None:
            raise ValueError('Topic name must be specified by the argument "contents"')

        # NOTE(kan-bayashi): seek() is not available in current version (2022/12/08) with apt,
        #   and therefore, we do not use start_timestamp temporary.
        if start_timestamp is not None:
            self.logger.warning("start_timestamp is not supported. ignored.")
            start_timestamp = None

        # Load rosbag2
        reader = rosbag2_py.SequentialReader()
        reader.open(*get_rosbag_options(path))

        # Seek timestamp if needed
        if start_timestamp is not None:
            # NOTE(kan-bayashi): seek() is not available in current version (2022/12/08) with apt
            raise NotImplementedError()
            # TODO(kan-bayashi): What happened when start timestamp if exceed end timestamp?
            reader.seek(start_timestamp)

        # Create mapping dict of topic name and type
        topic_types = reader.get_all_topics_and_types()
        type_map = {
            topic_types[i].name: topic_types[i].type for i in range(len(topic_types))
        }
        assert topic in type_map, f"topic {topic} is not included in rosbag."

        # Set filter
        storage_filter = rosbag2_py.StorageFilter(topics=[topic])
        reader.set_filter(storage_filter)

        if target_frame_rate is not None:
            timestamps = []
            while True:
                if not reader.has_next():
                    break
                timestamp_in_nsec = reader.read_next()[2]
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                timestamps.append(timestamp)
            timestamps = self.downsample_timestamps(timestamps, target_frame_rate)
            if start_timestamp is not None:
                # TODO(kan-bayashi): seek() is not available in current version (2022/12/08) with apt
                raise NotImplementedError()
                reader.seek(start_timestamp)
            else:
                # TODO(kan-bayashi): Replace with seek()
                del reader
                reader = rosbag2_py.SequentialReader()
                reader.open(*get_rosbag_options(path))
            timestamp_idx = 0
            while True:
                if not reader.has_next():
                    break
                (topic_, data_, timestamp_in_nsec) = reader.read_next()
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                if timestamp == timestamps[timestamp_idx]:
                    msg_type = get_message(type_map[topic_])
                    msg = deserialize_message(data_, msg_type)
                    yield {
                        "timestamps": [timestamp],
                        # NOTE(kan-bayashi): Need to convert?
                        "data": [msg.data],
                    }
                    timestamp_idx += 1
                if timestamp_idx == len(timestamps):
                    break
        else:
            while True:
                if not reader.has_next():
                    break
                (topic_, data_, timestamp_in_nsec) = reader.read_next()
                timestamp = float(timestamp_in_nsec) / (10**9)
                if end_timestamp is not None and timestamp > end_timestamp:
                    break
                msg_type = get_message(type_map[topic_])
                msg = deserialize_message(data_, msg_type)
                yield {
                    "timestamps": [timestamp],
                    # NOTE(kan-bayashi): Need to convert?
                    "data": [msg.data],
                }

    def _save(self, path, contents=None, **kwargs):
        """Save ndarray data to a csv file.

        Args:
            path (str): path to the output csv file
            contents (str or dict): topic name

        """
        pass

    def to_dataframe(self):
        """Return data as a Pandas DataFrame.

        Returns:
            (DataFrame): data

        """
        df = DataFrame.from_dict(self.data["data"])
        return df

    def to_ndarray(self):
        """Return data as ndarray."""
        df = self.to_dataframe()
        return df.to_numpy()

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        return np.array(self.data["timestamps"])

    @property
    def columns(self):
        """Return columns."""
        if self._columns is not None:
            return self._columns

        if self.data is not None:
            if len(self.data["data"]) > 0:
                return list(self.data["data"][0].keys())

        return []

    @classmethod
    def generate_contents_meta(cls, path, content_key="content"):
        """Generate contents metadata.

        Args:
            path (str): Path of rosbag2 file
            content_key (str): Not used

        Returns:
            dict: Contents metadata

        """
        # Load file
        reader = rosbag2_py.SequentialReader()
        reader.open(*get_rosbag_options(path))
        topics = reader.get_all_topics_and_types()

        # Generate metadata
        contents = {}
        for topic in topics:
            contents[topic.name] = {}
            contents[topic.name]["type"] = topic.type
            contents[topic.name]["serialization_format"] = topic.serialization_format
            contents[topic.name]["offered_qos_profiles"] = topic.offered_qos_profiles

        del reader

        return contents

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate timestamps metadata.

        Args:
            path (str): Path of rosbag2 file

        Returns:
            list: [start_timestamp, end_timestamp]

        """
        # Load file
        info_reader = rosbag2_py.Info()
        info = info_reader.read_metadata(path, "sqlite3")

        # Each time is datetime format
        start_time = info.starting_time
        end_time = start_time + info.duration

        return [start_time.timestamp(), end_time.timestamp()]