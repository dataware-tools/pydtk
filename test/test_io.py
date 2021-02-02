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


def test_semi_structured_reader():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import SemiStructuredDataFileReader

    path = 'test/records/json_model_test/json_test.json'
    reader = SemiStructuredDataFileReader()
    timestamps, data, columns = reader.read(path=path)

    assert isinstance(data, dict)


def test_base_writer():
    """Run the base writer test."""
    from pydtk.io import BaseFileWriter, BaseFileReader
    from pydtk.models import MetaDataModel
    import json
    import os
    import numpy as np

    data = np.array([
        ["DATE", "TIME", "q= 0.01 [Yen]", "q = 0.25 [Yen]", "q = 0.50 [Yen]", "q = 0.75 [Yen]", "q = 0.99 [Yen]"],
        ["2020 / 11 / 03", "00:00", 2.7814903259, 4.0177507401, 4.795976162, 5.4033985138, 8.051410675],
        ["2020 / 11 / 03", "00:30", 2.6788742542, 3.86374259, 4.5651106834, 5.2544236183, 7.7826366424],
        ["2020 / 11 / 03", "01:00", 2.5762581825, 3.7097344398, 4.3342452049, 5.105448722799999, 7.5138626099],
        ["2020 / 11 / 03", "01:30", 2.4808146954, 3.7635395527, 4.2475838661, 4.9490222930000005, 7.6304917336]
    ])

    metadata = {
        "description": "forecast",
        "database_id": "JERA Forecast",
        "record_id": "test",
        "type": "raw_data",
        "path": "/opt/pydtk/test/records/jera/test.csv",
        "content-type": "text/csv",
        "contents": {
            "test": {
                "tags": [
                    "test1",
                    "test2"]
            }
        }
    }

    def _helper_test_base_writer(data, metadata, metadata_is_model):
        csv_path = metadata['path']
        metadata_path = csv_path + '.json'
        if metadata_is_model:
            metadata = MetaDataModel(metadata)

        if os.path.isfile(csv_path):
            os.remove(path=csv_path)
        if os.path.isfile(csv_path):
            os.remove(path=metadata_path)

        writer = BaseFileWriter()
        writer.write(data=data, metadata=metadata)

        reader = BaseFileReader()
        timestamps, new_data, columns = reader.read(metadata=metadata)
        np.testing.assert_array_equal(data, new_data)

        with open(metadata_path, 'r') as fd:
            new_metadata = json.load(fd)

        # NOTE: json.dumps(MetaDataModel(metadata_dict).data) is not equal json.dumps(metadata_dict)
        if metadata_is_model:
            metadata_comparable = json.dumps(metadata.data)
            new_metadata_comparable = json.dumps(MetaDataModel(new_metadata).data)
        else:
            metadata_comparable = json.dumps(metadata)
            new_metadata_comparable = json.dumps(new_metadata)

        if metadata_comparable != new_metadata_comparable:
            raise ValueError('Saving metadata failed! (src and dest is unmatch)')

    _helper_test_base_writer(data, metadata, True)
    _helper_test_base_writer(data, metadata, False)


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
