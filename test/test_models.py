#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test metadata loader script with Pytest."""

import io
import os
from contextlib import redirect_stdout

import pytest


def test_metadata_model():
    """Run the metadata loader test."""
    path = "test/records/json_model_test/json_test.json.json"

    from pydtk.models import MetaDataModel

    assert MetaDataModel.is_loadable(path)

    # load
    metadata = MetaDataModel()
    metadata.load(path)
    metadata.save("/tmp/test.json")


def test_csv_model():
    """Run the metadata and data loader test."""
    meta_path = "test/records/csv_model_test/data/test.csv.json"
    path = "test/records/csv_model_test/data/test.csv"

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import CameraTimestampCsvModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    csv = CameraTimestampCsvModel(metadata=metadata)
    csv.load(path)
    csv.save("/tmp/test.csv")


def test_image_model():
    """Run the GenericImageModel test."""
    meta_path = "test/records/image_model_test/sample.png.json"

    import numpy as np

    from pydtk.models import MetaDataModel
    from pydtk.models.image import GenericImageModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = GenericImageModel(metadata=metadata)
    model.load()

    assert isinstance(model.to_ndarray(), np.ndarray)

    model.save("/tmp/test_image.png")


def test_annotation_model():
    """Run the AnnotationCsvModel test."""
    meta_path = "test/records/annotation_model_test/annotation_test.csv.json"

    import numpy as np

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import AnnotationCsvModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    annotation_model = AnnotationCsvModel(metadata=metadata)
    annotation_model.load()

    assert isinstance(annotation_model.to_ndarray(), np.ndarray)

    annotation_model.save("/tmp/test_annotation.csv")


def test_forecast_model():
    """Run the ForecastCsvModel test."""
    meta_path = "test/records/forecast_model_test/forecast_test.csv.json"

    import numpy as np

    from pydtk.models import MetaDataModel
    from pydtk.models.csv import ForecastCsvModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    forecast_model = ForecastCsvModel(metadata=metadata)
    forecast_model.load()

    assert isinstance(forecast_model.to_ndarray(), np.ndarray)

    forecast_model.save("/tmp/test_forecast.csv")

    from datetime import datetime

    strp_foramt = "%Y/%m/%d %H:%M:%S"

    forecast_model.load(
        start_timestamp=datetime.strptime(
            "2020/11/03 00:30:00", strp_foramt
        ).timestamp(),
        end_timestamp=datetime.strptime("2020/11/03 01:20:00", strp_foramt).timestamp(),
    )

    assert isinstance(forecast_model.to_ndarray(), np.ndarray)

    forecast_model.save("/tmp/test_forecast_query.csv")


def test_json_model():
    """Run the GenericJsonModel test."""
    meta_path = "test/records/json_model_test/json_test.json.json"

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

    json_model.save("/tmp/test_json.json")


def test_movie_model():
    """Run the GenericMovieModel test."""
    meta_path = "test/records/movie_model_test/sample.mp4.json"

    import numpy as np

    from pydtk.models import MetaDataModel
    from pydtk.models.movie import GenericMovieModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = GenericMovieModel(metadata=metadata)
    model.load()

    assert isinstance(model.to_ndarray(), np.ndarray)

    model.save("/tmp/test_movie.mp4")


@pytest.mark.extra
@pytest.mark.pointcloud
def test_pointcloud_pcd_model():
    """Test pointcloud/PCDModel."""
    path = "test/assets/test_pointcloud.pcd"

    import numpy as np

    from pydtk.models.pointcloud.pcd import PCDModel

    # Generate point-cloud
    pointcloud = np.random.random_sample((100, 4)) * np.array([100, 100, 100, 1])

    # Set
    pcd = PCDModel()
    pcd.from_ndarray(pointcloud, columns=["x", "y", "z", "intensity"])

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
    meta_path = "test/records/sample/data/records.bag.json"
    path = "test/records/sample/data/records.bag"

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents="/vehicle/analog/speed_pulse")


@pytest.mark.extra
@pytest.mark.ros
def test_sensor_msgs_nav_sat_fix_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = "test/records/sample/data/records.bag.json"
    path = "test/records/sample/data/records.bag"

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents="/vehicle/gnss")


@pytest.mark.extra
@pytest.mark.ros
def test_geometry_msgs_accel_stamped_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = "test/records/sample/data/records.bag.json"
    path = "test/records/sample/data/records.bag"

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericRosbagModel(metadata=metadata)
    data.load(path, contents="/vehicle/acceleration")


@pytest.mark.extra
@pytest.mark.ros
def test_sensor_msgs_pointcloud2_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = "test/records/sample/data/records.bag.json"
    path = "test/records/sample/data/records.bag"

    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import SensorMsgsPointCloud2RosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = SensorMsgsPointCloud2RosbagModel(metadata=metadata)
    model.configure(fields=("x", "y", "z", "intensity"))
    model.load(path, contents="/points_concat_downsampled")


@pytest.mark.extra
@pytest.mark.ros
def test_autoware_can_msgs_can_packet_rosbag_model():
    """Run the metadata and data loader test."""
    meta_path = "test/records/can_model_test/test.bag.json"
    path = "test/records/can_model_test/test.bag"

    from pydtk.models import MetaDataModel
    from pydtk.models.autoware import AutowareCanMsgsCANPacketRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    model = AutowareCanMsgsCANPacketRosbagModel(
        metadata=metadata, path_to_assign_list="test/assets/can_assign_list.csv"
    )
    model.load(path, contents="/vehicle/can_raw")

    timestamps = model.timestamps
    data = model.to_ndarray()
    columns = model.columns

    assert len(timestamps) == len(data)
    assert len(columns) == data.shape[-1]

    # load with configuration
    model = AutowareCanMsgsCANPacketRosbagModel(metadata=metadata)
    model.configure(path_to_assign_list="test/assets/can_assign_list.csv")
    model.load(path, contents="/vehicle/can_raw")

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
    meta_path = "test/records/zstd_rosbag_model_test/data/records.bag.zst.json"
    path = "test/records/zstd_rosbag_model_test/data/records.bag.zst"

    from pydtk.models import MetaDataModel
    from pydtk.models.zstd.rosbag import GenericZstdRosbagModel

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)
    metadata.data["path"] = os.path.join(os.getcwd(), metadata.data["path"])  # Fix path

    # load
    data = GenericZstdRosbagModel(metadata=metadata)
    data.load(path, contents="/vehicle/analog/speed_pulse")


def generate_dummy_rosbag2(
    bag_path,
    topic_name="/chatter",
    topic_type="std_msgs/msg/String",
    sample_rate=10.0,
    storage_id="sqlite3",
):
    """Generate dummy rosbag2 for testing."""
    import rosbag2_py
    import std_msgs.msg as _msg

    from rclpy.serialization import serialize_message

    from pydtk.models.rosbag2 import get_rosbag_options

    storage_options, converter_options = get_rosbag_options(
        bag_path, storage_id=storage_id
    )

    writer = rosbag2_py.SequentialWriter()
    writer.open(storage_options, converter_options)

    # create topic
    topic = rosbag2_py.TopicMetadata(
        name=topic_name, type=topic_type, serialization_format="cdr"
    )
    writer.create_topic(topic)

    msg_class = getattr(_msg, topic_type.split("/")[-1])
    for i in range(5):
        msg = msg_class()
        timestamp_in_sec = i / sample_rate

        # timestamp must be nano seconds
        writer.write(
            topic_name, serialize_message(msg), int(timestamp_in_sec * 10**9)
        )

    # close bag and create new storage instance
    del writer


@pytest.mark.extra
@pytest.mark.ros2
@pytest.mark.parametrize(
    "topic_type",
    [
        "std_msgs/msg/Bool",
        "std_msgs/msg/Byte",
        "std_msgs/msg/ByteMultiArray",
        "std_msgs/msg/Char",
        "std_msgs/msg/ColorRGBA",
        "std_msgs/msg/Empty",
        "std_msgs/msg/Float32",
        "std_msgs/msg/Float32MultiArray",
        "std_msgs/msg/Float64",
        "std_msgs/msg/Float64MultiArray",
        "std_msgs/msg/Header",
        "std_msgs/msg/Int16",
        "std_msgs/msg/Int16MultiArray",
        "std_msgs/msg/Int32",
        "std_msgs/msg/Int32MultiArray",
        "std_msgs/msg/Int64",
        "std_msgs/msg/Int64MultiArray",
        "std_msgs/msg/Int8",
        "std_msgs/msg/Int8MultiArray",
        "std_msgs/msg/MultiArrayDimension",
        "std_msgs/msg/MultiArrayLayout",
        "std_msgs/msg/String",
        "std_msgs/msg/UInt16",
        "std_msgs/msg/UInt16MultiArray",
        "std_msgs/msg/UInt32",
        "std_msgs/msg/UInt32MultiArray",
        "std_msgs/msg/UInt64",
        "std_msgs/msg/UInt64MultiArray",
        "std_msgs/msg/UInt8",
        "std_msgs/msg/UInt8MultiArray",
    ],
)
@pytest.mark.parametrize("storage_id", ["sqlite3", "mcap"])
def test_std_msgs_rosbag2_model(topic_type, storage_id):
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
    generate_dummy_rosbag2(
        bag_path=bag_path,
        topic_name=topic_name,
        topic_type=topic_type,
        sample_rate=sample_rate,
        storage_id=storage_id,
    )

    meta_path = "test/records/rosbag2_model_test/data.json"
    f = io.StringIO()
    with open(meta_path, "w") as g, redirect_stdout(f):
        Model.generate(target="metadata", from_file=bag_path)
        g.write(f.getvalue())

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # check data is loadable
    data = GenericRosbag2Model(metadata=metadata)
    data.load(contents=topic_name)
    assert len(data.data["data"]) == 5

    # NOTE(kan-bayashi): Seek with mcap does not work well in 2022/12/27
    # TODO(kan-bayashi): May next ROS2 support seek with mcap format
    #   https://github.com/ros2/rosbag2/pull/1205
    if storage_id == "sqlite3":
        # NOTE(kan-bayashi): 0.01 for margin
        data.load(contents=topic_name, start_timestamp=0.2)
        assert len(data.data["data"]) == 3

    data.load(contents=topic_name, target_frame_rate=5)
    # NOTE(kan-bayashi): timestamp = 0 is not included, is it OK?
    assert len(data.data["data"]) == 2

    # check data is loadable as generator
    # NOTE(kan-bayashi): target_frame_rate is stored at running before so we need to overwrite here
    items = [
        item
        for item in data.load(
            contents=topic_name, as_generator=True, target_frame_rate=None
        )
    ]
    assert len(items) == 5
    items = [
        item
        for item in data.load(
            contents=topic_name, as_generator=True, target_frame_rate=5
        )
    ]
    # NOTE(kan-bayashi): timestamp = 0 is not included, is it OK?
    assert len(items) == 2

    # check data is convertable
    data.to_dataframe()
    data.to_ndarray()

    # check file is loadable
    ext = "db3" if storage_id == "sqlite3" else "mcap"
    f = io.StringIO()
    with open(meta_path, "w") as g, redirect_stdout(f):
        Model.generate(
            target="metadata", from_file=os.path.join(bag_path, f"data_0.{ext}")
        )
        g.write(f.getvalue())

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # check data is loadable
    model = GenericRosbag2Model(metadata=metadata)
    model.load(contents=topic_name)


def generate_dummy_rosbag2_autoware_auto(
    bag_path,
    topic_name,
    topic_type,
    sample_rate=10.0,
    storage_id="sqlite3",
):
    """Generate dummy rosbag2 including autoware.auto msgs for testing."""
    import rosbag2_py
    from rclpy.serialization import serialize_message

    from pydtk.models.rosbag2 import get_rosbag_options

    storage_options, converter_options = get_rosbag_options(bag_path, storage_id)

    writer = rosbag2_py.SequentialWriter()
    writer.open(storage_options, converter_options)

    # create topic
    topic = rosbag2_py.TopicMetadata(
        name=topic_name,
        type=topic_type,
        serialization_format="cdr",
    )
    writer.create_topic(topic)

    # write messages
    import autoware_auto_msgs.msg as _msg

    msg_class = getattr(_msg, topic_type.split("/")[-1])

    for i in range(10):
        msg = msg_class()
        timestamp_in_sec = i / sample_rate

        # timestamp must be nano seconds
        writer.write(
            topic_name,
            serialize_message(msg),
            int(timestamp_in_sec * 10**9),
        )

    # close bag and create new storage instance
    del writer


try:
    import autoware_auto_msgs.msg as autoware_auto_msg  # NOQA

    is_autoware_auto_installed = True

except ImportError:
    is_autoware_auto_installed = False


@pytest.mark.extra
@pytest.mark.ros2
@pytest.mark.skipif(
    not is_autoware_auto_installed, reason="autoware.auto msgs is not installed."
)
@pytest.mark.parametrize(
    "topic_type",
    [
        "autoware_auto_msgs/msg/BoundingBoxArray",
        "autoware_auto_msgs/msg/BoundingBox",
        "autoware_auto_msgs/msg/DiagnosticHeader",
        "autoware_auto_msgs/msg/PointClusters",
        "autoware_auto_msgs/msg/Trajectory",
        "autoware_auto_msgs/msg/VehicleOdometry",
        "autoware_auto_msgs/msg/BoundingBoxArray",
        "autoware_auto_msgs/msg/HADMapBin",
        "autoware_auto_msgs/msg/Quaternion32",
        "autoware_auto_msgs/msg/TrajectoryPoint",
        "autoware_auto_msgs/msg/VehicleStateCommand",
        "autoware_auto_msgs/msg/Complex32",
        "autoware_auto_msgs/msg/HighLevelControlCommand",
        "autoware_auto_msgs/msg/RawControlCommand",
        "autoware_auto_msgs/msg/VehicleControlCommand",
        "autoware_auto_msgs/msg/VehicleStateReport",
        "autoware_auto_msgs/msg/ControlDiagnostic",
        "autoware_auto_msgs/msg/MapPrimitive",
        "autoware_auto_msgs/msg/Route",
        "autoware_auto_msgs/msg/VehicleKinematicState",
    ],
)
@pytest.mark.parametrize("storage_id", ["sqlite3", "mcap"])
def test_autoware_auto_msgs_rosbag2_model(topic_type, storage_id):
    """Test the metadata and data loader for rosbag2 including autoware.auto msgs."""
    import shutil

    from pydtk.bin.sub_commands.model import Model
    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag2 import GenericRosbag2Model

    # generate dummy rosbag2 for testing
    bag_path = "test/records/rosbag2_autoware_auto_model_test/data"
    topic_name = topic_type
    sample_rate = 10.0
    if os.path.exists(bag_path):
        shutil.rmtree(bag_path)
    generate_dummy_rosbag2_autoware_auto(
        bag_path=bag_path,
        topic_name=topic_name,
        topic_type=topic_type,
        sample_rate=sample_rate,
        storage_id=storage_id,
    )

    meta_path = "test/records/rosbag2_autoware_auto_model_test/data.json"
    f = io.StringIO()
    with open(meta_path, "w") as g, redirect_stdout(f):
        Model.generate(target="metadata", from_file=bag_path)
        g.write(f.getvalue())

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # check data is loadable
    model = GenericRosbag2Model(metadata=metadata)
    model.load(contents=topic_name)

    # check data is loadable with generator
    [_ for _ in model.load(contents=topic_type, as_generator=True)]

    # check data is convertable
    model.to_dataframe()
    model.to_ndarray()

    # check file is loadable
    ext = "db3" if storage_id == "sqlite3" else "mcap"
    f = io.StringIO()
    with open(meta_path, "w") as g, redirect_stdout(f):
        Model.generate(
            target="metadata", from_file=os.path.join(bag_path, f"data_0.{ext}")
        )
        g.write(f.getvalue())

    # load metadata
    metadata = MetaDataModel()
    metadata.load(meta_path)

    # check data is loadable
    model = GenericRosbag2Model(metadata=metadata)
    model.load(contents=topic_name)


if __name__ == "__main__":
    # test_metadata_model()
    # test_csv_model()
    # test_std_msgs_rosbag_model()
    # test_sensor_msgs_nav_sat_fix_rosbag_model()
    # test_geometry_msgs_accel_stamped_rosbag_model()
    # test_sensor_msgs_pointcloud2_rosbag_model()
    # test_autoware_can_msgs_can_packet_rosbag_model()
    test_pointcloud_pcd_model()
