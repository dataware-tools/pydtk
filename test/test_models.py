#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test metadata loader script with Pytest."""


def test_metadata_model():
    """Run the metadata loader test."""
    path = 'test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json'

    from pydtk.models import MetaDataModel
    assert MetaDataModel.is_loadable(path)

    # load
    metadata = MetaDataModel()
    metadata.load(path)
    metadata.save('/tmp/test.json')


def test_csv_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json'
    path = 'test/records/016_00000000030000000240/data/camera_01_timestamps.csv'

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import CameraTimestampCsvModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    csv = CameraTimestampCsvModel(metadata=metadata)
    csv.load(path)
    csv.save('/tmp/test.csv')


def test_std_msgs_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/B05_17000000010000000829/data/records.bag.json'
    path = 'test/records/B05_17000000010000000829/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import StdMsgsRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    data = StdMsgsRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/analog/speed_pulse')


def test_sensor_msgs_nav_sat_fix_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/B05_17000000010000000829/data/records.bag.json'
    path = 'test/records/B05_17000000010000000829/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import SensorMsgsNavSatFixRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    data = SensorMsgsNavSatFixRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/gnss')


def test_geometry_msgs_accel_stamped_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/B05_17000000010000000829/data/records.bag.json'
    path = 'test/records/B05_17000000010000000829/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GeometryMsgsAccelStampedRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    data = GeometryMsgsAccelStampedRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/acceleration')


def test_sensor_msgs_pointcloud2_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/sample/data/records.bag.json'
    path = 'test/records/sample/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import SensorMsgsPointCloud2RosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    model = SensorMsgsPointCloud2RosbagModel(metadata=metadata)
    model.configure(fields=('x', 'y', 'z', 'intensity'))
    model.load(path, contents='/points_concat_downsampled')


def test_autoware_can_msgs_can_packet_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/meti2019/ssd7.bag.json'
    path = 'test/records/meti2019/ssd7.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.autoware import AutowareCanMsgsCANPacketRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # load
    model = AutowareCanMsgsCANPacketRosbagModel(
        metadata=metadata,
        path_to_assign_list='test/assets/can_assign_list.csv'
    )
    model.load(path, contents='/vehicle/can_raw')

    timestamps = model.timestamps
    data = model.to_ndarray()
    columns = model.columns

    assert len(timestamps) == len(data)
    assert len(columns) == data.shape[-1]

    # load with configuration
    model = AutowareCanMsgsCANPacketRosbagModel(metadata=metadata)
    model.configure(path_to_assign_list='test/assets/can_assign_list.csv')
    model.load(path, contents='/vehicle/can_raw')

    # retrieve
    timestamps = model.timestamps
    data = model.to_ndarray()
    columns = model.columns

    assert len(timestamps) == len(data)
    assert len(columns) == data.shape[-1]


if __name__ == '__main__':
    # test_metadata_model()
    # test_csv_model()
    # test_std_msgs_rosbag_model()
    # test_sensor_msgs_nav_sat_fix_rosbag_model()
    # test_geometry_msgs_accel_stamped_rosbag_model()
    # test_sensor_msgs_pointcloud2_rosbag_model()
    test_autoware_can_msgs_can_packet_rosbag_model()
