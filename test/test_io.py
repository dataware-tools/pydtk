#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test base reader script with Pytest."""

import pytest


def test_base_reader():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/annotation_model_test/annotation_test.csv'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path)

    assert isinstance(data, np.ndarray)


def test_base_reader_non_ndarray():
    """Run the base reader test."""
    from pydtk.io import BaseFileReader

    path = 'test/records/json_model_test/json_test.json'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, as_ndarray=False)

    assert isinstance(data, dict)


def test_select_model():
    """Run test for selecting suitable models."""
    from pydtk.io import BaseFileReader
    from pydtk.models import MetaDataModel
    from pydtk.models.csv import GenericCsvModel
    from pydtk.models.json_model import GenericJsonModel
    from pydtk.models.image import GenericImageModel
    from pydtk.models.movie import GenericMovieModel

    # GenericCsvModel
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={'path': 'abc.csv'}))(),
        GenericCsvModel
    )
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={
            'path': 'abc.csv',
            'data_type': 'questionnaire',
            'content_type': 'text/csv',
            'contents': {"generic-csv": {'columns': ['a', 'b', 'c']}},
        }))(),
        GenericCsvModel
    )

    # GenericJsonModel
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={'path': 'abc.json'}))(),
        GenericJsonModel
    )
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={
            'path': 'abc.json',
            'data_type': 'questionnaire',
            'content_type': 'text/json',
            'contents': {"generic-json": {'keys': ['a', 'b', 'c']}},
        }))(),
        GenericJsonModel
    )

    # GenericImageModel
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={'path': 'abc.jpg'}))(),
        GenericImageModel
    )
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={
            'path': 'abc.jpg',
            'data_type': 'raw_data',
            'content_type': 'image/jpeg',
            'contents': {"generic-image": {'width': 640, 'height': 480}},
        }))(),
        GenericImageModel
    )

    # GenericMovieModel
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={'path': 'abc.mp4'}))(),
        GenericMovieModel
    )
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={
            'path': 'abc.mp4',
            'data_type': 'raw_data',
            'content_type': 'video/mp4',
            'contents': {"generic-movie": {'length': '10'}},
        }))(),
        GenericMovieModel
    )


@pytest.mark.extra
@pytest.mark.ros
def test_select_model_ros():
    """Run test for selecting suitable models."""
    from pydtk.io import BaseFileReader
    from pydtk.models import MetaDataModel
    from pydtk.models.rosbag import GenericRosbagModel

    # GenericRosbagModel
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={'path': 'abc.bag'}))(),
        GenericRosbagModel
    )
    assert isinstance(
        BaseFileReader._select_model(MetaDataModel(data={
            'path': 'abc.bag',
            'data_type': 'raw_data',
            'content_type': 'application/rosbag',
            'contents': {"topic1": {'count': 1}},
        }))(),
        GenericRosbagModel
    )



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
        "description": "description",
        "record_id": "test",
        "type": "raw_data",
        "path": "/opt/pydtk/test/dumps/test_base_writer.csv",
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
    path = 'test/records/rosbag_model_test/data/records.bag'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents='/vehicle/gnss')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_base_reader_rosbag_accel():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/rosbag_model_test/data/records.bag'
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents='/vehicle/acceleration')

    assert isinstance(data, np.ndarray)


@pytest.mark.extra
@pytest.mark.ros
def test_base_reader_rosbag_can():
    """Run the base reader test."""
    import numpy as np
    from pydtk.io import BaseFileReader
    path = 'test/records/can_model_test/test.bag'
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
    metadata_path = 'test/records/rosbag_model_test/data/records.bag.json'
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
    from pydtk.db import V4DBHandler as DBHandler
    from pydtk.io import BaseFileReader

    record_id = 'rosbag_model_test'
    target_content = '/vehicle/acceleration'
    start_timestamp = 1517463303.0
    end_timestamp = 1517463303.5

    # Get DBHandler
    handler = DBHandler(
        db_class='meta',
        db_engine='tinymongo',
        db_host='test/test_v4',
        base_dir_path='/opt/pydtk/test',
        read_on_init=False,
    )
    handler.read(pql='record_id == "{}"'.format(record_id))

    # Get the corresponding metadata
    for metadata in handler:
        for content in metadata['contents'].keys():
            if content != target_content:
                continue
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
