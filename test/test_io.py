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


def test_base_writer():
    """Run the base writer test."""
    from pydtk.io import BaseFileWriter
    import json
    import os
    import numpy as np

    csv_path = 'test/records/jera/test.csv'
    data = np.array([
        ["DATE", "TIME", "q= 0.01 [Yen]", "q = 0.25 [Yen]", "q = 0.50 [Yen]", "q = 0.75 [Yen]", "q = 0.99 [Yen]"],
        ["2020 / 11 / 03", "00:00", 2.7814903259, 4.0177507401, 4.795976162, 5.4033985138, 8.051410675],
        ["2020 / 11 / 03", "00:30", 2.6788742542, 3.86374259, 4.5651106834, 5.2544236183, 7.7826366424],
        ["2020 / 11 / 03", "01:00", 2.5762581825, 3.7097344398, 4.3342452049, 5.105448722799999, 7.5138626099],
        ["2020 / 11 / 03", "01:30", 2.4808146954, 3.7635395527, 4.2475838661, 4.9490222930000005, 7.6304917336],
        ["2020 / 11 / 03", "02:00", 2.3853712082, 3.8173446655, 4.1609225273, 4.7925958633, 7.7471208572],
        ["2020 / 11 / 03", "02:30", 2.4931795597, 3.8574020863, 4.1738054753, 4.8718442916, 7.7336649895],
        ["2020 / 11 / 03", "03:00", 2.6009879112, 3.897459507, 4.1866884232, 4.95109272, 7.7202091217],
        ["2020 / 11 / 03", "03:30", 2.6634838581, 3.9712848664, 4.2355024815, 4.903665781, 7.9435429573],
        ["2020 / 11 / 03", "04:00", 2.725979805, 4.0451102257, 4.2843165398, 4.856238842, 8.1668767929],
        ["2020 / 11 / 03", "04:30", 2.8283495903, 4.103662014, 4.4018087387, 4.9847819805, 8.2466163635],
        ["2020 / 11 / 03", "05:00", 2.9307193756, 4.1622138023, 4.5193009377, 5.113325119, 8.3263559341],
        ["2020 / 11 / 03", "05:30", 3.0198979377, 4.2312819957, 4.4376957417, 5.17502141, 8.4372282028],
        ["2020 / 11 / 03", "06:00", 3.1090764999, 4.3003501892000005, 4.3560905457, 5.236717701, 8.5481004715],
        ["2020 / 11 / 03", "06:30", 2.9130270481, 4.3895642757, 4.5798389912, 5.3267338276, 8.878644466399997],
        ["2020 / 11 / 03", "07:00", 2.7169775963, 4.4787783623, 4.8035874367, 5.416749954199999, 9.2091884613]
    ])

    if os.path.isfile(csv_path):
        os.remove(path=csv_path)

    metadata_path = 'test/records/jera/test.csv.json'
    with open(metadata_path, 'r') as fd:
        metadata = json.load(fd)

    writer = BaseFileWriter()
    writer.write(data=data, metadata=metadata)


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
