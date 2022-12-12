#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import re
from abc import ABC

import cv2
import numpy as np
import ros_numpy
import rosbag
import rospy
import sensor_msgs.msg
import yaml
from flatdict import FlatDict
from pandas import DataFrame

from pydtk.models import BaseModel, register_model


@register_model(priority=1)
class GenericRosbagModel(BaseModel, ABC):
    """A generic model for a rosbag file."""

    _content_type = None  # allow any content-type
    _data_type = None  # allow any data-type
    _file_extensions = [".bag"]
    _contents = None
    _config = {
        "keys_to_exclude": [
            "header.seq",
            "header.stamp.secs",
            "header.stamp.nsecs",
            "header.frame_id",
        ]
    }

    def __init__(self, **kwargs):
        super(GenericRosbagModel, self).__init__(**kwargs)

    def _load(
        self,
        path,
        contents=None,
        start_timestamp=None,
        end_timestamp=None,
        target_frame_rate=None,
        **kwargs
    ):
        """Load a rosbag file.

        Args:
            path (str): path to a rosbag file
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
        start_time = rospy.Time(start_timestamp) if start_timestamp else start_timestamp
        end_time = rospy.Time(end_timestamp) if end_timestamp else end_timestamp

        timestamps, data = [], []
        with rosbag.Bag(path, "r") as bag:
            if target_frame_rate:
                timestamps = self.load_timestamps(bag, topic, start_time, end_time)
                timestamps = self.downsample_timestamps(
                    timestamps, target_frame_rate=target_frame_rate
                )
                idx = 0
            for topic, msg, t in bag.read_messages(
                topics=[topic], start_time=start_time, end_time=end_time
            ):
                timestamp = self.msg_to_timestamp(msg, t).to_sec()
                if target_frame_rate:
                    if timestamp == timestamps[idx]:
                        data.append(
                            self.msg_to_data(msg, config=self._config, **kwargs)
                        )
                        idx += 1
                        if idx == len(timestamps):
                            break
                    continue
                timestamps.append(timestamp)
                data.append(self.msg_to_data(msg, config=self._config, **kwargs))

        self.data = {"timestamps": timestamps, "data": data}

    def _load_as_generator(
        self,
        path,
        contents=None,
        start_timestamp=None,
        end_timestamp=None,
        target_frame_rate=None,
        **kwargs
    ):
        """Load a rosbag file for each sample.

        Args:
            path (str): path to a rosbag file
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
        start_time = rospy.Time(start_timestamp) if start_timestamp else start_timestamp
        end_time = rospy.Time(end_timestamp) if end_timestamp else end_timestamp

        timestamps = []
        with rosbag.Bag(path, "r") as bag:
            if target_frame_rate:
                timestamps = self.load_timestamps(bag, topic, start_time, end_time)
                timestamps = self.downsample_timestamps(
                    timestamps, target_frame_rate=target_frame_rate
                )
                idx = 0
            for topic, msg, t in bag.read_messages(
                topics=[topic], start_time=start_time, end_time=end_time
            ):
                timestamp = self.msg_to_timestamp(msg, t).to_sec()
                if target_frame_rate:
                    if timestamp == timestamps[idx]:
                        yield {
                            "timestamps": [timestamp],
                            "data": [
                                self.msg_to_data(msg, config=self._config, **kwargs)
                            ],
                        }
                        idx += 1
                        if idx == len(timestamps):
                            break
                    continue
                yield {
                    "timestamps": [timestamp],
                    "data": [self.msg_to_data(msg, config=self._config, **kwargs)],
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

    def load_timestamps(self, bag, topic, start_time, end_time):
        """Load only timestamps."""
        timestamps = []
        for topic, msg, t in bag.read_messages(
            topics=[topic], start_time=start_time, end_time=end_time
        ):
            timestamps.append(self.msg_to_timestamp(msg, t).to_sec())
        return timestamps

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

    @staticmethod
    def msg_to_data(msg, **kwargs):
        """Convert a message to data.

        Args:
            msg (a ROS message): message

        Returns:
            (object): data

        """
        keys_to_exclude = []
        if "config" in kwargs.keys() and "keys_to_exclude" in kwargs["config"].keys():
            keys_to_exclude = kwargs["config"]["keys_to_exclude"]
        return {
            k: v
            for k, v in dict(FlatDict(yaml.safe_load(str(msg)), delimiter=".")).items()
            if k not in keys_to_exclude
        }

    @staticmethod
    def msg_to_timestamp(msg, t):
        """Extract timestamp from a message.

        Args:
            msg (a ROS message): message
            t (rospy.Time): timestamp read from rosbag

        Returns:
            (rospy.Time): timestamp

        """
        if hasattr(msg, "header"):
            if msg.header.stamp.is_zero():
                return t
            return msg.header.stamp
        else:
            return t

    @classmethod
    def generate_contents_meta(cls, path, content_key="content"):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (dict): contents metadata

        """
        # Load file
        contents = {}
        with rosbag.Bag(path, "r") as bag:
            # get topic names
            topic_info = bag.get_type_and_topic_info()
        topics = [topic for topic in topic_info[1].keys()]
        topics.sort()

        # Generate metadata
        for topic in topics:
            contents[topic] = {}
            contents[topic]["msg_type"] = topic_info[1][topic].msg_type
            contents[topic]["msg_md5sum"] = topic_info[0][topic_info[1][topic].msg_type]
            contents[topic]["count"] = topic_info[1][topic].message_count
            contents[topic]["frequency"] = topic_info[1][topic].frequency
            contents[topic]["tags"] = re.split("[_/-]", topic[1:])

        return contents

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        with rosbag.Bag(path, "r") as bag:
            start_time = bag.get_start_time()
            end_time = bag.get_end_time()
        return [start_time, end_time]


@register_model(priority=2)
class SensorMsgsCompressedImageRosbagModel(GenericRosbagModel, ABC):
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
class SensorMsgsPointCloud2RosbagModel(GenericRosbagModel, ABC):
    """A model for a rosbag file containing sensor_msgs/PointCloud2."""

    _contents = {".*": {"msg_type": "sensor_msgs/PointCloud2"}}
    _config = {"fields": ("x", "y", "z")}

    def __init__(self, fields=("x", "y", "z"), **kwargs):
        super(SensorMsgsPointCloud2RosbagModel, self).__init__(**kwargs)
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
