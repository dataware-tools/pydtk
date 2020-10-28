#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test base reader script with Pytest."""

import pytest


def test_base_reader():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/016_00000000030000000240/data/camera_01_timestamps.csv'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path)

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_base_reader_rosbag():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/B05_17000000010000000829/data/records.bag'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents='/vehicle/gnss')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_base_reader_rosbag_accel():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/B05_17000000010000000829/data/records.bag'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents='/vehicle/acceleration')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_base_reader_rosbag_can():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/meti2019/ssd7.bag'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents='/vehicle/can_raw')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_separated_data():
    """Run the base reader test."""
    import numpy as np
    from pydtk.models import MetaDataModel
    from pydtk.io import BaseFileReader
    metadata_path = 'test/records/sample/separated_data/records.bag.json'
    metadata = MetaDataModel()
    metadata.load(metadata_path)
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(metadata=metadata, contents='/points_concat_downsampled')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_load_from_metadata_dict():
    """Run the base reader test."""
    import numpy as np
    from pydtk.models import MetaDataModel
    from pydtk.io import BaseFileReader
    metadata_path = 'test/records/B05_17000000010000000829/data/records.bag.json'
    metadata = MetaDataModel()
    metadata.load(metadata_path)
    metadata = metadata.data
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(metadata=metadata,
                                            contents='/vehicle/analog/speed_pulse')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_load_from_db():
    """Load from database."""
    from pydtk.db import V3DBHandler as DBHandler
    from pydtk.io import BaseFileReader

    dataset_id = 'Driving Behavior Database'
    record_id = 'B05_17000000010000000829'
    content = '/vehicle/acceleration'
    start_timestamp = 1517463303.0
    end_timestamp = 1517463303.5

    # Get DBHandler
    handler = DBHandler(
        db_class='meta',
        db_engine='sqlite',
        db_username='',
        db_password='',
        db_host='test/test_v3.db',
        read_on_init=False
    )
    handler.read(
        where='database_id like "{0}"'
              ' and record_id like "{1}"'
              ' and contents like "{2}"'
        .format(dataset_id, record_id, content)
    )

    # Get the corresponding metadata
    metadata = next(handler)
    metadata.update({
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp
    })

    # Get FileReader
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(metadata)

    assert len(timestamps) > 0
    assert len(data) > 0
    assert len(columns) > 0
    pass


if __name__ == '__main__':
    # test_base_reader()
    # test_base_reader_rosbag()
    # test_separated_data()
    # test_load_from_metadata_dict()
    # test_base_reader_rosbag_accel()
    # test_load_from_db()
    test_base_reader_rosbag_can()