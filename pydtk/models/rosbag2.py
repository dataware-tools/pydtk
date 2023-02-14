#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import glob
import logging
import os.path as osp
import platform
from abc import ABC

import numpy as np
import rosbag2_py
from packaging import version
from pandas import DataFrame
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from pydtk.models import BaseModel, register_model

# check python version
python_version = ".".join(platform.python_version_tuple()[:2])
if version.parse(python_version) != version.parse("3.10"):
    raise ImportError(
        f"ROS2 (Humble) supports only Python 3.10, but your Python is {python_version}"
    )


def get_rosbag_options(
    path,
    storage_id,
    serialization_format="cdr",
):
    """Get rosbag options for reader."""
    storage_options = rosbag2_py.StorageOptions(
        uri=path,
        storage_id=storage_id,
    )
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
    _file_extensions = [None, ".db3", ".mcap"]
    _contents = None

    def __init__(self, **kwargs):
        super(GenericRosbag2Model, self).__init__(**kwargs)

    @classmethod
    def _is_loadable(cls, path="", **kwargs):
        """Check a given rosbag is openable."""
        try:
            reader = rosbag2_py.SequentialReader()
            reader.open(*get_rosbag_options(path, cls._get_storage_id(path)))
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

        # NOTE(kan-bayashi): Seek with mcap does not work well in 2022/12/27
        # TODO(kan-bayashi): May next ROS2 support seek with mcap format
        #   https://github.com/ros2/rosbag2/pull/1205
        if start_timestamp is not None and self._get_storage_id(path) == "mcap":
            self.logger.warning("start_timestamp is not supported. ignored.")
            start_timestamp = None

        # Load rosbag2
        reader = rosbag2_py.SequentialReader()
        reader.open(*get_rosbag_options(path, self._get_storage_id(path)))

        # Seek timestamp if needed
        if start_timestamp is not None:
            # TODO(kan-bayashi): What happened when start timestamp if exceed end timestamp?
            reader.seek(int(start_timestamp * 10**9))

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
                reader.seek(int(start_timestamp * 10**9))
            else:
                # TODO(kan-bayashi): Replace with seek()
                del reader
                reader = rosbag2_py.SequentialReader()
                reader.open(*get_rosbag_options(path, self._get_storage_id(path)))
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
                    data.append(self.msg_to_data(msg))
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
                data.append(self.msg_to_data(msg))

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

        # NOTE(kan-bayashi): Seek with mcap does not work well in 2022/12/27
        # TODO(kan-bayashi): May next ROS2 support seek with mcap format
        #   https://github.com/ros2/rosbag2/pull/1205
        if start_timestamp is not None and self._get_storage_id(path) == "mcap":
            self.logger.warning("start_timestamp is not supported. ignored.")
            start_timestamp = None

        # Load rosbag2
        reader = rosbag2_py.SequentialReader()
        reader.open(*get_rosbag_options(path, self._get_storage_id(path)))

        # Seek timestamp if needed
        if start_timestamp is not None:
            # TODO(kan-bayashi): What happened when start timestamp if exceed end timestamp?
            reader.seek(int(start_timestamp * 10**9))

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
                reader.seek(int(start_timestamp * 10**9))
            else:
                # TODO(kan-bayashi): Replace with seek()
                del reader
                reader = rosbag2_py.SequentialReader()
                reader.open(*get_rosbag_options(path, self._get_storage_id(path)))
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
                    # NOTE(kan-bayashi): If msg includes header, should we get timestamp from it?
                    yield {
                        "timestamps": [timestamp],
                        "data": [self.msg_to_data(msg)],
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
                # NOTE(kan-bayashi): If msg includes header, should we get timestamp from it?
                yield {
                    "timestamps": [timestamp],
                    "data": [self.msg_to_data(msg)],
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
        info_reader = rosbag2_py.Info()
        info = info_reader.read_metadata(path, cls._get_storage_id(path))
        contents = {}
        for topic in info.topics_with_message_count:
            meta = topic.topic_metadata
            contents[meta.name] = {}
            contents[meta.name]["message_count"] = topic.message_count
            contents[meta.name]["type"] = meta.type
            contents[meta.name]["serialization_format"] = meta.serialization_format
            contents[meta.name]["offered_qos_profiles"] = meta.offered_qos_profiles

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
        info = info_reader.read_metadata(path, cls._get_storage_id(path))

        # Each time is datetime format
        start_time = info.starting_time
        end_time = start_time + info.duration

        return [start_time.timestamp(), end_time.timestamp()]

    @classmethod
    def msg_to_data(cls, msg):
        """Convert msg to data."""
        ret = {}
        cls._convert_msg_to_flatdict(msg, ret, msg.__class__.__name__)
        return ret

    @classmethod
    def _maybe_get_fields_and_field_types(cls, msg):
        try:
            ret = msg.get_fields_and_field_types()
            return ret
        except AttributeError:
            return msg

    @classmethod
    def _get_storage_id(cls, path):
        if path.endswith(".db3"):
            storage_id = "sqlite3"
        elif path.endswith(".mcap"):
            storage_id = "mcap"
        elif osp.isdir(path) and len(glob.glob(osp.join(path, "*.db3"))) > 0:
            storage_id = "sqlite3"
        elif osp.isdir(path) and len(glob.glob(osp.join(path, "*.mcap"))) > 0:
            storage_id = "mcap"
        else:
            raise ValueError(f"Not supported rosbag2 format (path={path}).")
        return storage_id

    @classmethod
    def _convert_msg_to_flatdict(cls, msg, msg_dict={}, prefix=""):
        """Convert msg into flat dict recursively."""
        dict_or_value = cls._maybe_get_fields_and_field_types(msg)
        if isinstance(dict_or_value, dict):
            for key in dict_or_value.keys():
                msg_ = getattr(msg, key)
                next_prefix = f"{prefix}.{key}"
                cls._convert_msg_to_flatdict(msg_, msg_dict, next_prefix)
        elif isinstance(dict_or_value, list):
            for idx, _ in enumerate(dict_or_value):
                next_prefix = f"{prefix}.{idx}"
                cls._convert_msg_to_flatdict(msg[idx], msg_dict, next_prefix)
        else:
            msg_dict[prefix] = dict_or_value
