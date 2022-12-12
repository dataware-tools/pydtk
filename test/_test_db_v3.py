#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import pytest


def _test_create_db():
    """Create DB of records directory."""
    from pydtk.db import V1MetaDBHandler
    from pydtk.models import MetaDataModel

    handler = V1MetaDBHandler("test/meta_db.arrow")

    paths = [
        "test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json",
        "test/records/B05_17000000010000000829/data/records.bag.json",
        "test/records/sample/data/records.bag.json",
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)

    # Get dfs
    _ = handler.get_content_df()
    _ = handler.get_file_df()
    _ = handler.get_record_id_df()

    # Save
    handler.save()


def _test_load_db():
    """Load DB."""
    from pydtk.db import V1MetaDBHandler

    handler = V1MetaDBHandler("test/meta_db.arrow")

    try:
        for sample in handler:
            for column in handler.columns:
                assert column["name"] in sample.keys()
    except EOFError:
        pass


def _test_db_and_io():
    """Load DB and load file."""
    from pydtk.db import V1MetaDBHandler
    from pydtk.io import BaseFileReader, NoModelMatchedError

    handler = V1MetaDBHandler("test/meta_db.arrow")
    reader = BaseFileReader()

    try:
        for sample in handler:
            print(sample)
            try:
                timestamps, data = reader.read(**sample)
                assert len(timestamps) == len(data)
            except NoModelMatchedError as e:
                print(str(e))
                continue
    except EOFError:
        pass


def _test_create_db_v2():
    """Create DB of records directory."""
    from pydtk.db import V2MetaDBHandler
    from pydtk.models import MetaDataModel

    handler = V2MetaDBHandler(
        db_engine="sqlite", db_host="test/test_v2.db", base_dir_path="test"
    )

    paths = [
        "test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json",
        "test/records/B05_17000000010000000829/data/records.bag.json",
        "test/records/sample/data/records.bag.json",
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data, check_unique=True)

    # Get dfs
    _ = handler.get_content_df()
    _ = handler.get_file_df()
    _ = handler.get_record_id_df()

    # Save
    handler.save(remove_duplicates=True)


def _test_load_db_V2():
    """Load DB."""
    from pydtk.db import V2MetaDBHandler

    handler = V2MetaDBHandler(
        db_engine="sqlite", db_host="test/test_v2.db", base_dir_path="test"
    )

    try:
        for sample in handler:
            for column in handler.columns:
                assert column["name"] in sample.keys()
    except EOFError:
        pass


def _test_db_and_io_v2():
    """Load DB and load file."""
    from pydtk.db import V2MetaDBHandler
    from pydtk.io import BaseFileReader, NoModelMatchedError

    handler = V2MetaDBHandler(
        db_engine="sqlite", db_host="test/test_v2.db", base_dir_path="test"
    )
    reader = BaseFileReader()

    try:
        for sample in handler:
            print(
                'loading content "{0}" from file "{1}"'.format(
                    sample["contents"], sample["path"]
                )
            )
            try:
                timestamps, data, columns = reader.read(**sample)
                assert len(timestamps) == len(data)
            except NoModelMatchedError as e:
                print(str(e))
                continue
    except EOFError:
        pass


def _test_db_search_v2():
    """Load DB and search file."""
    from pydtk.db import V2MetaDBHandler

    handler = V2MetaDBHandler(
        db_engine="sqlite",
        db_host="test/test_v2.db",
        base_dir_path="test",
        read_on_init=False,
    )
    handler.read(where="start_timestamp > 1520000000 and end_timestamp < 1500000000")
    records = handler.get_record_id_df().to_dict("records")
    assert len(records) == 0

    handler.read(where='tags like "%camera%" or tags like "%lidar%"')
    records = handler.get_record_id_df().to_dict("records")
    assert len(records) > 0


def _test_custom_df_v2():
    """Create a custom dataframe."""
    import pandas as pd

    from pydtk.db import V2BaseDBHandler, V2MetaDBHandler
    from pydtk.io import BaseFileReader, NoModelMatchedError

    meta_db = V2MetaDBHandler(
        db_engine="sqlite", db_host="test/test_v2.db", base_dir_path="test"
    )
    reader = BaseFileReader()

    # meta_db.read(where='tags like "%gnss%"')

    try:
        for sample in meta_db:
            print(
                'loading content "{0}" from file "{1}"'.format(
                    sample["contents"], sample["path"]
                )
            )
            try:
                # Initialize DB for storing features
                feats_db = V2BaseDBHandler(
                    db_engine="sqlite",
                    db_host="test/test.db",
                    df_name=sample["contents"],
                    read_on_init=False,
                )

                # Load data from file
                timestamps, data, columns = reader.read(**sample)

                # Create DataFrame
                timestamps_df = pd.Series(timestamps, name="timestamp")
                data_df = pd.DataFrame(data, columns=columns)
                df = pd.concat([timestamps_df, data_df], axis=1)

                # Add to DB
                feats_db.df = df
                feats_db.save(remove_duplicates=True)

            except NoModelMatchedError:
                continue
            except Exception as e:
                print(
                    'Failed to process content "{0}" from file "{1}"'.format(
                        sample["contents"], sample["path"]
                    )
                )
                print(e)
                continue
    except EOFError:
        pass


def test_create_db_v3():
    """Create DB of records directory."""
    from pydtk.db import V3DBHandler
    from pydtk.models import MetaDataModel

    handler = V3DBHandler(
        db_class="meta",
        db_engine="sqlite",
        db_host="test/test_v3.db",
        base_dir_path="test",
    )

    paths = [
        "test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json",
        "test/records/B05_17000000010000000829/data/records.bag.json",
        "test/records/sample/data/records.bag.json",
        "test/records/meti2019/ssd7.bag.json",
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)

    # Get dfs
    _ = handler.get_content_df()
    _ = handler.get_file_df()
    _ = handler.get_record_id_df()

    # Save
    handler.save(remove_duplicates=True)


def test_load_db_v3():
    """Load DB."""
    from pydtk.db import V3DBHandler

    handler = V3DBHandler(
        db_class="meta",
        db_engine="sqlite",
        db_host="test/test_v3.db",
        base_dir_path="test",
    )

    assert handler.count_total == len(handler.df)

    content_columns = handler._config[handler._df_class]["content_columns"]

    try:
        for sample in handler:
            for column in handler.columns:
                if column["name"] in ["uuid_in_df", "creation_time_in_df"]:
                    continue
                if column["name"] in content_columns:
                    assert (
                        column["name"]
                        in next(iter(list(sample["contents"].values()))).keys()
                    )
                else:
                    assert column["name"] in sample.keys()
    except EOFError:
        pass


@pytest.mark.extra
@pytest.mark.cassandra
def test_load_timeseries_cassandra_v3():
    """Load DB."""
    from pydtk.db import V3DBHandler

    try:
        handler = V3DBHandler(
            db_class="time_series",
            db_engine="cassandra",
            db_database="statistics",
            db_username="pydtk",
            db_password="pydtk",
            df_name="span_3600",
            read_on_init=False,
        )
        handler.read()
        pass
    except Exception as e:
        print(e)


def test_create_db_v3_with_env_var():
    """Create DB of records directory."""
    import os

    from pydtk.db import V3DBHandler
    from pydtk.models import MetaDataModel

    # Set environment variables
    os.environ["PYDTK_META_DB_ENGINE"] = "sqlite"
    os.environ["PYDTK_META_DB_HOST"] = "test/test_v3_env.db"

    handler = V3DBHandler(db_class="meta", base_dir_path="test")

    paths = [
        "test/records/016_00000000030000000240/data/camera_01_timestamps.csv.json",
    ]

    # Load metadata and add to DB
    for path in paths:
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)

    # Save
    handler.save(remove_duplicates=True)

    assert os.path.exists("test/test_v3_env.db")


def test_load_db_v3_with_env_var():
    """Load DB."""
    import os

    from pydtk.db import V3DBHandler

    # Set environment variables
    os.environ["PYDTK_META_DB_ENGINE"] = "sqlite"
    os.environ["PYDTK_META_DB_HOST"] = "test/test_v3_env.db"

    handler = V3DBHandler(db_class="meta")

    try:
        for sample in handler:
            print(sample)
    except EOFError:
        pass


def test_get_handler_v3():
    """Check if DBHandler class works properly."""
    from pydtk.db import V3DBHandler, V3MetaDBHandler, V3TimeSeriesDBHandler
    # from pydtk.db.v3 import StatisticsCassandraDBHandler

    handler = V3DBHandler(
        db_class="meta",
        db_engine="sqlite",
        db_host="test/test.db",
        base_dir_path="/",
        read_on_init=False,
    )
    assert isinstance(handler, V3MetaDBHandler)

    handler = V3DBHandler(
        db_class="time_series",
        db_engine="sqlite",
        db_host="test/test.db",
        read_on_init=False,
    )
    assert isinstance(handler, V3TimeSeriesDBHandler)

    # handler = V3DBHandler(
    #     db_class='statistics',
    #     db_engine='cassandra',
    #     db_host='192.168.1.98:30079',
    #     db_username='pydtk',
    #     db_password='pydtk',
    #     db_name='statistics',
    #     database_id='Driving Behavior Database',
    #     span=3600,
    #     read_on_init=False
    # )
    # assert isinstance(handler, StatisticsCassandraDBHandler)


@pytest.mark.extra
@pytest.mark.cassandra
def test_get_search_engine_v3():
    """Check if DBSearchEngine class works properly."""
    from pydtk.db import V3DBHandler, V3DBSearchEngine
    from pydtk.db.v3 import (
        StatisticsCassandraDBHandler,
        TimeSeriesCassandraDBSearchEngine,
    )

    handler = V3DBHandler(
        db_class="statistics",
        db_engine="cassandra",
        db_host="192.168.1.98:30079",
        db_username="pydtk",
        db_password="pydtk",
        db_name="statistics",
        database_id="Driving Behavior Database",
        span=3600,
        read_on_init=False,
    )
    assert isinstance(handler, StatisticsCassandraDBHandler)

    search_engine = V3DBSearchEngine(handler)
    assert isinstance(search_engine, TimeSeriesCassandraDBSearchEngine)


def test_get_db_handler_from_env():
    """Get a suitable db_handler from environment variable."""
    from pydtk.db import V3DBHandler as DBHandler

    _ = DBHandler(db_class="meta")
    pass


def test_db_group_by():
    """Test DB."""
    from pydtk.db import V3DBHandler as DBHandler

    handler = DBHandler(
        db_class="meta",
        db_engine="sqlite",
        db_host="test/test_v3.db",
        read_on_init=False,
    )

    for key in ["record_id", "database_id"]:
        handler.read(group_by=key)
        assert len(set(handler.df[key].to_list())) == len(handler.df)

    handler.read(group_by="database_id", where='contents like "unknown"')
    assert len(handler.df) == 0
    assert len(handler.df) == handler.count_total


def test_db_v3_migration():
    """Test DB migration."""
    from pydtk.db import V3DBHandler as DBHandler

    for i in range(2):
        db_handler = DBHandler(
            db_class="database_id",
            db_engine="sqlite",
            db_host="test/test_v3.db",
            df_name="test_migration",
            read_on_init=False,
        )
        db_handler.add_data({"database_id": "aaa", "column-{}".format(i): "aaa"})
        db_handler.save()


if __name__ == "__main__":
    # test_create_db_v3()
    # test_load_db_v3()
    # test_create_db_v3_with_env_var()
    # test_load_db_v3_with_env_var()
    # test_get_handler_v3()
    # test_get_db_handler_from_env()
    # test_db_group_by()
    test_db_v3_migration()
