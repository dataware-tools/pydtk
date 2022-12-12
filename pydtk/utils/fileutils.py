#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)

import abc
import time  # for debug

import numpy as np
import rosbag


class BaseReader(object, metaclass=abc.ABCMeta):
    """Base class for data reader."""

    def __init__(self, file):
        """Set file."""
        self.file = file

    @abc.abstractmethod
    def contents(self):
        """Return ndarray data."""
        pass


class RosbagReader(BaseReader):
    """Rosbag data reader class."""

    def __init__(self, file):
        """Set file."""
        super(RosbagReader, self).__init__(file)

    def _get_msg_type(self, msg):
        try:
            _ = msg.data
            return "std_msgs/"
        except AttributeError:
            try:
                _ = msg.latitude
                return "sensor_msgs/NavSatFix"
            except AttributeError:
                try:
                    _ = msg.accel
                    return "geometry_msgs/AccelStamped"
                except AttributeError:
                    try:
                        _ = msg.range
                        return "sensor_msgs/Range"
                    except AttributeError:
                        raise Exception("Unsupported message type: %s" % type(msg))

    def _msg_to_list(self, msg, msg_type="std_msgs/"):
        if msg_type == "std_msgs/":
            return [msg.data]
        elif msg_type == "sensor_msgs/NavSatFix":
            return [msg.latitude, msg.longitude]
        elif msg_type == "geometry_msgs/AccelStamped":
            return [msg.accel.linear.x, msg.accel.linear.y, msg.accel.linear.z]
        elif msg_type == "sensor_msgs/Range":
            return [msg.range]

    def contents(self, content):
        """Return timestamps and data(ndarray)."""
        bag = rosbag.Bag(self.file)
        time_list, data_list = None, None
        for topic, msg, t in bag.read_messages():
            if topic == content:
                timestamp = t.secs + float(t.nsecs) / (10**9)
                if data_list is None:
                    time_list = [[timestamp]]
                    msg_type = self._get_msg_type(msg)
                    data_list = [self._msg_to_list(msg, msg_type)]
                else:
                    time_list.append([timestamp])
                    data_list.append(self._msg_to_list(msg, msg_type))
        return np.array(time_list), np.array(data_list)


if __name__ == "__main__":
    reader = RosbagReader(
        "/data_pool_1/small_DrivingBehaviorDatabase/records/ \
                          016_00000000030000000240/data/records.bag"
    )

    t1 = time.time()

    test = reader.contents("/vehicle/analog/speed_pulse")

    print(test.shape)
    print(test[0])
    print(time.time() - t1)
