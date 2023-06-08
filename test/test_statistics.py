#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test base statistic calculation script with Pytest."""

import pytest


@pytest.mark.extra
@pytest.mark.ros
def test_base_statistic_calculation():
    """Run the base statistic calculation test."""
    import pandas as pd

    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span)
    stat_df = calculator.statistic_tables(timestamps, data, columns)

    assert isinstance(stat_df, pd.core.frame.DataFrame)


def _test_v2_db_statistic():
    """Run the v2 DB statistics test."""
    import pandas as pd

    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span)
    stat_df = calculator.statistic_tables(timestamps, data, columns)

    assert isinstance(stat_df, pd.core.frame.DataFrame)

    from requests.exceptions import ConnectionError

    from pydtk.db.v2 import TimeSeriesDBHandler

    try:
        db_handler = TimeSeriesDBHandler(df_name="test_statistics_span_0.3")
        db_handler.df = stat_df
        db_handler.save()
    except ConnectionError as e:
        print(str(e))


def _test_v2_db_statistic_search():
    """Run the v2 DB statistics search test."""
    from requests.exceptions import ConnectionError

    from pydtk.db import V2TimeSeriesDBHandler, V2TimeSeriesDBSearchEngine

    span = 60
    content = "/vehicle/acceleration"

    try:
        db_handler = V2TimeSeriesDBHandler(db_name="statistics", read_on_init=False)
        engine = V2TimeSeriesDBSearchEngine(db_handler=db_handler, content=content, span=span)
        engine.add_condition('"/vehicle/acceleration/accel_linear_x/min" > 0')
        candidates = engine.search()
        print(candidates)

    except ConnectionError as e:
        print(str(e))


def _test_v3_db_statistic():
    """Run the v3 DB statistics test."""
    import pandas as pd
    from cassandra.cluster import NoHostAvailable

    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span)
    stat_df = calculator.statistic_tables(timestamps, data, columns)

    assert isinstance(stat_df, pd.core.frame.DataFrame)

    from pydtk.db import V3TimeSeriesCassandraDBHandler

    try:
        db_handler = V3TimeSeriesCassandraDBHandler(df_name="test_statistics_span_0")
        db_handler.df = stat_df
        db_handler.save()
    except NoHostAvailable as e:
        print(str(e))


def _test_v3_db_statistic_search():
    """Run the v3 DB statistics search test."""
    import time

    from cassandra.cluster import NoHostAvailable

    from pydtk.db import V3TimeSeriesCassandraDBHandler, V3TimeSeriesCassandraDBSearchEngine

    try:
        db_handler = V3TimeSeriesCassandraDBHandler(read_on_init=False)
        engine = V3TimeSeriesCassandraDBSearchEngine(db_handler=db_handler)
        engine.add_condition('"/vehicle/acceleration/accel_linear_x/min" > 0')
        t1 = time.time()
        candidates = engine.search()
        t2 = time.time()
        print(candidates)
        print("Search execution time: {0:.03f} sec.".format(t2 - t1))

    except NoHostAvailable as e:
        print(str(e))


@pytest.mark.extra
@pytest.mark.ros
def test_v3_db_statistic_sqlite():
    """Run the v3 DB statistics test."""
    import pandas as pd

    from pydtk.db import V3DBHandler as DBHandler
    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")
    # timestamps, data, columns = reader.read(path=path, contents='/vehicle/analog/speed_pulse')

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span)
    stat_df = calculator.statistic_tables(timestamps, data, columns)
    stat_df["record_id"] = "B05_17000000010000000829"

    assert isinstance(stat_df, pd.core.frame.DataFrame)

    db_handler = DBHandler(
        db_class="time_series",
        db_engine="sqlite",
        db_host="test/test_statistics.db",
        db_username="",
        db_password="",
        db_name="",
        read_on_init=False,
    )
    db_handler.df = stat_df
    db_handler.save(remove_duplicates=True)


@pytest.mark.extra
@pytest.mark.ros
def test_v3_db_statistic_sqlite_2():
    """Run the v3 DB statistics test."""
    import pandas as pd

    from pydtk.db import V3DBHandler as DBHandler
    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")
    # timestamps, data, columns = reader.read(path=path, contents='/vehicle/analog/speed_pulse')

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span)
    stat_df = calculator.statistic_tables(timestamps, data, columns)
    stat_df["record_id"] = "B05_17000000010000000829"

    assert isinstance(stat_df, pd.core.frame.DataFrame)

    db_handler = DBHandler(
        database_id="test",
        span=0.3,
        db_class="statistics",
        db_engine="sqlite",
        db_host="test/test_statistics.db",
        db_username="",
        db_password="",
        db_name="",
        read_on_init=False,
    )
    db_handler.df = stat_df
    db_handler.save(remove_duplicates=True)


@pytest.mark.extra
@pytest.mark.ros
def test_v3_db_statistic_search_sqlite():
    """Run the v3 DB statistics search test."""
    import time

    from pydtk.db import V3DBHandler as DBHandler
    from pydtk.db import V3DBSearchEngine as DBSearchEngine

    db_handler = DBHandler(
        database_id="test",
        span=0.3,
        db_class="statistics",
        db_engine="sqlite",
        db_host="test/test_statistics.db",
        db_username="",
        db_password="",
        db_name="",
        read_on_init=False,
    )
    engine = DBSearchEngine(db_handler=db_handler)
    engine.add_condition('"/vehicle/acceleration/accel_linear_x/min" < 0')
    t1 = time.time()
    candidates = engine.search()
    t2 = time.time()
    print(candidates)
    print("Search execution time: {0:.03f} sec.".format(t2 - t1))


@pytest.mark.extra
@pytest.mark.ros
def test_base_statistic_calculation_with_sync_timestamp():
    """Run the base statistic calculation test."""
    from pydtk.io import BaseFileReader
    from pydtk.statistics import BaseStatisticCalculation

    path = "test/records/rosbag_model_test/data/records.bag"
    reader = BaseFileReader()
    timestamps, data, columns = reader.read(path=path, contents="/vehicle/acceleration")

    target_span = 0.3
    calculator = BaseStatisticCalculation(target_span, sync_timestamps=False)
    stat_df = calculator.statistic_tables(timestamps, data, columns)
    result = stat_df.to_dict(orient="record")

    calculator_sync = BaseStatisticCalculation(target_span, sync_timestamps=True)
    stat_df_sync = calculator_sync.statistic_tables(timestamps, data, columns)
    result_sync = stat_df_sync.to_dict(orient="record")

    for without_sync, with_sync in zip(result, result_sync):
        assert without_sync["timestamp"] // target_span * target_span == with_sync["timestamp"]


if __name__ == "__main__":
    # test_v3_db_statistic_sqlite()
    # test_v3_db_statistic_sqlite_2()
    # test_v3_db_statistic_search_sqlite()
    test_base_statistic_calculation_with_sync_timestamp()
