#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test metadata loader script with Pytest."""

import os
import pytest


def test_metadata_model():
    """Run the metadata loader test."""
    path = 'test/records/json_model_test/json_test.json.json'

    from pydtk.models import MetaDataModel
    assert MetaDataModel.is_loadable(path)

    # load
    metadata = MetaDataModel()
    metadata.load(path)
    metadata.save('/tmp/test.json')


def test_csv_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/csv_model_test/data/test.csv.json'
    path = 'test/records/csv_model_test/data/test.csv'

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import CameraTimestampCsvModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    csv = CameraTimestampCsvModel(metadata=metadata)
    csv.load(path)
    csv.save('/tmp/test.csv')


def test_image_model():
    """Run the GenericImageModel test."""
    meta_path = 'test/records/image_model_test/sample.png.json'

    from pydtk.models import MetaDataModel
    from pydtk.models.image import GenericImageModel
    import numpy as np

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = GenericImageModel(metadata=metadata)
    model.load()

    assert isinstance(model.to_ndarray(), np.ndarray)

    model.save('/tmp/test_image.png')


def test_annotation_model():
    """Run the AnnotationCsvModel test."""
    meta_path = 'test/records/annotation_model_test/annotation_test.csv.json'

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import AnnotationCsvModel
    import numpy as np

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    annotation_model = AnnotationCsvModel(metadata=metadata)
    annotation_model.load()

    assert isinstance(annotation_model.to_ndarray(), np.ndarray)

    annotation_model.save('/tmp/test_annotation.csv')


def test_forecast_model():
    """Run the ForecastCsvModel test."""
    meta_path = 'test/records/forecast_model_test/forecast_test.csv.json'

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import ForecastCsvModel
    import numpy as np

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    forecast_model = ForecastCsvModel(metadata=metadata)
    forecast_model.load()

    assert isinstance(forecast_model.to_ndarray(), np.ndarray)

    forecast_model.save('/tmp/test_forecast.csv')

    from datetime import datetime

    strp_foramt = "%Y/%m/%d %H:%M:%S"

    forecast_model.load(
        start_timestamp=datetime.strptime("2020/11/03 00:30:00", strp_foramt).timestamp(),
        end_timestamp=datetime.strptime("2020/11/03 01:20:00", strp_foramt).timestamp(),
    )

    assert isinstance(forecast_model.to_ndarray(), np.ndarray)

    forecast_model.save("/tmp/test_forecast_query.csv")


def test_json_model():
    """Run the GenericJsonModel test."""
    meta_path = 'test/records/json_model_test/json_test.json.json'

    from pydtk.models import MetaDataModel
    from pydtk.models.json_model import GenericJsonModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    json_model = GenericJsonModel(metadata=metadata)
    json_model.load()

    assert isinstance(json_model.data, dict)

    json_model.save('/tmp/test_json.json')


def test_movie_model():
    """Run the GenericMovieModel test."""
    meta_path = 'test/records/movie_model_test/sample.mp4.json'

    from pydtk.models import MetaDataModel
    from pydtk.models.movie import GenericMovieModel
    import numpy as np

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = GenericMovieModel(metadata=metadata)
    model.load()

    assert isinstance(model.to_ndarray(), np.ndarray)

    model.save('/tmp/test_movie.mp4')


@pytest.mark.extra
@pytest.mark.pointcloud
def test_pointcloud_pcd_model():
    """Test pointcloud/PCDModel."""
    path = 'test/assets/test_pointcloud.pcd'

    import numpy as np
    from pydtk.models.pointcloud.pcd import PCDModel

    # Generate point-cloud
    pointcloud = np.random.random_sample((100, 4)) * np.array([100, 100, 100, 1])

    # Set
    pcd = PCDModel()
    pcd.from_ndarray(pointcloud, columns=['x', 'y', 'z', 'intensity'])

    # Save
    pcd.save(path)

    # Load
    new_pcd = PCDModel()
    new_pcd.load(path)

    # Assertion
    new_pointcloud = new_pcd.to_ndarray()
    diff = np.sum((pointcloud - new_pointcloud) ** 2)
    assert diff == 0.0
    assert all([c in new_pcd._columns for c in pcd._columns])
    assert all([c in pcd._columns for c in new_pcd._columns])


@pytest.mark.extra
@pytest.mark.ros
def test_std_msgs_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/sample/data/records.bag.json'
    path = 'test/records/sample/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/analog/speed_pulse')


@pytest.mark.extra
@pytest.mark.ros
def test_sensor_msgs_nav_sat_fix_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/sample/data/records.bag.json'
    path = 'test/records/sample/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/gnss')


@pytest.mark.extra
@pytest.mark.ros
def test_geometry_msgs_accel_stamped_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/sample/data/records.bag.json'
    path = 'test/records/sample/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/acceleration')


@pytest.mark.extra
@pytest.mark.ros
def test_sensor_msgs_pointcloud2_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/sample/data/records.bag.json'
    path = 'test/records/sample/data/records.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import SensorMsgsPointCloud2RosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = SensorMsgsPointCloud2RosbagModel(metadata=metadata)
    model.configure(fields=('x', 'y', 'z', 'intensity'))
    model.load(path, contents='/points_concat_downsampled')


@pytest.mark.extra
@pytest.mark.ros
def test_autoware_can_msgs_can_packet_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/can_model_test/test.bag.json'
    path = 'test/records/can_model_test/test.bag'

    from pydtk.models import MetaDataModel
    from pydtk.models.autoware import AutowareCanMsgsCANPacketRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

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


@pytest.mark.extra
@pytest.mark.ros
@pytest.mark.zstd
def test_std_msgs_zstd_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = 'test/records/zstd_rosbag_model_test/data/records.bag.zst.json'
    path = 'test/records/zstd_rosbag_model_test/data/records.bag.zst'

    from pydtk.models import MetaDataModel
    from pydtk.models.zstd.rosbag import GenericZstdRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericZstdRosbagModel(metadata=metadata)
    data.load(path, contents='/vehicle/analog/speed_pulse')


def generate_dummy_rosbag2(bag_path, topic_name="/chatter", sample_rate=10.0):
    from rclpy.serialization import serialize_message
    import rosbag2_py
    from std_msgs.msg import String

    from pydtk.models.rosbag2 import get_rosbag_options

    storage_options, converter_options = get_rosbag_options(bag_path)

    writer = rosbag2_py.SequentialWriter()
    writer.open(storage_options, converter_options)

    # create topic
    topic = rosbag2_py.TopicMetadata(
        name=topic_name, type="std_msgs/msg/String", serialization_format="cdr"
    )
    writer.create_topic(topic)

    for i in range(100):
        msg = String()
        msg.data = f"Hello, world! {str(i)}"
        timestamp_in_sec = i / sample_rate

        # timestamp must be nano seconds
        writer.write(topic_name, serialize_message(msg), int(timestamp_in_sec * 10 ** 9))

    # close bag and create new storage instance
    del writer


@pytest.mark.extra
@pytest.mark.ros2
def test_std_msgs_rosbag2_model():
    """Run the metadata and data loader test."""
    import shutil

    from pydtk.bin.sub_commands.model import Model
    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag2 import GenericRosbag2Model

    # generate dummy rosbag2 for testing
    bag_path = "test/records/rosbag2_model_test/data"
    topic_name = "/chatter"
    sample_rate = 10.0
    if os.path.exists(bag_path):
        shutil.rmtree(bag_path)
    generate_dummy_rosbag2(bag_path=bag_path, topic_name=topic_name, sample_rate=sample_rate)

    meta_path = "test/records/rosbag2_model_test/data.json"
    with open(meta_path, "w") as f:
        f.write(Model.generate(target="metadata", from_file=bag_path))

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # check data is loadable
    data = GenericRosbag2Model(metadata=metadata)
    data.load(contents=topic_name)
    assert len(data.data["data"]) == 100
    data.load(contents=topic_name, target_frame_rate=1)
    # NOTE(kan-bayashi): timestamp = 0 is not included, is it OK?
    assert len(data.data["data"]) == 9

    # check data is loadable as generator
    # NOTE(kan-bayashi): target_frame_rate is stored at running before so we need to overwrite here
    items = [item for item in data.load(contents=topic_name, as_generator=True, target_frame_rate=None)]
    assert len(items) == 100
    items = [item for item in data.load(contents=topic_name, as_generator=True, target_frame_rate=1)]
    # NOTE(kan-bayashi): timestamp = 0 is not included, is it OK?
    assert len(items) == 9


if __name__ == '__main__':
    # test_metadata_model()
    # test_csv_model()
    # test_std_msgs_rosbag_model()
    # test_sensor_msgs_nav_sat_fix_rosbag_model()
    # test_geometry_msgs_accel_stamped_rosbag_model()
    # test_sensor_msgs_pointcloud2_rosbag_model()
    # test_autoware_can_msgs_can_packet_rosbag_model()
    test_pointcloud_pcd_model()
