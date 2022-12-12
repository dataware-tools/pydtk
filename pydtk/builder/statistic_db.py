#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)


import logging
import time
from functools import partial
from multiprocessing import Pool

import fire
from tqdm import tqdm

from pydtk.db import V3DBHandler as DBHandler
from pydtk.io import BaseFileReader
from pydtk.statistics import BaseStatisticCalculation


def _analysis(*args, **kwargs):
    try:
        main(*args, **kwargs)
    except Exception as e:
        print(e)


def batch_analysis(
    database_id,
    span=60.0,
    num_jobs=1,
    meta_db_base_dir=None,
    meta_db_engine=None,
    meta_db_host=None,
    meta_db_username=None,
    meta_db_password=None,
    meta_db_name=None,
    output_db_engine=None,
    output_db_host=None,
    output_db_username=None,
    output_db_password=None,
    output_db_name=None,
    verbose=False,
):
    """Make Statistics Dataframe Table with all contents.

    Args:
        database_id (str): ID of the target database (e.g. "Driving Behavior Database")
        span (float): Size of divided frame[sec]
        num_jobs (int): Number of jobs to work in parallel
        meta_db_base_dir (str): base directory of path
        meta_db_engine (str): Database engine of metadata
        meta_db_host (str): HOST of database of metadata
        meta_db_username (str): Username for the database of metadata
        meta_db_password (str): Password for the database of metadata
        meta_db_name (str): Database name of metadata
        output_db_engine (str): Database engine for storing statistics data
        output_db_host (str): HOST of database of statistics data
        output_db_username (str): Username for the database of statistics data
        output_db_password (str): Password for the database of statistics data
        output_db_name (str): Database name of statistics data
        verbose (bool): Verbose mode

    """
    # Prepare method
    analysis = partial(
        _analysis,
        database_id,
        span=span,
        meta_db_base_dir=meta_db_base_dir,
        meta_db_engine=meta_db_engine,
        meta_db_host=meta_db_host,
        meta_db_username=meta_db_username,
        meta_db_password=meta_db_password,
        meta_db_name=meta_db_name,
        output_db_engine=output_db_engine,
        output_db_host=output_db_host,
        output_db_username=output_db_username,
        output_db_password=output_db_password,
        output_db_name=output_db_name,
        verbose=verbose,
    )

    # Load meta DB
    meta_db_handler = DBHandler(
        db_class="meta",
        db_engine=meta_db_engine,
        db_host=meta_db_host,
        db_name=meta_db_name,
        db_username=meta_db_username,
        db_password=meta_db_password,
        database_id=database_id,
        base_dir_path=meta_db_base_dir,
    )

    # Get list of contents
    contents = meta_db_handler.df["contents"].to_list()

    # Process one-by-one
    with Pool(num_jobs) as pool:
        pool.map(analysis, contents)


def main(
    database_id,
    q_content,
    span=60.0,
    meta_db_base_dir=None,
    meta_db_engine=None,
    meta_db_host=None,
    meta_db_username=None,
    meta_db_password=None,
    meta_db_name=None,
    output_db_engine=None,
    output_db_host=None,
    output_db_username=None,
    output_db_password=None,
    output_db_name=None,
    verbose=False,
):
    """Make Statistics Dataframe Table.

    Args:
        database_id (str): ID of the target database (e.g. "Driving Behavior Database")
        q_content (str): Content name of query
        span (float): Size of divided frame[sec]
        meta_db_base_dir (str): base directory of path
        meta_db_engine (str): Database engine of metadata
        meta_db_host (str): HOST of database of metadata
        meta_db_username (str): Username for the database of metadata
        meta_db_password (str): Password for the database of metadata
        meta_db_name (str): Database name of metadata
        output_db_engine (str): Database engine for storing statistics data
        output_db_host (str): HOST of database of statistics data
        output_db_username (str): Username for the database of statistics data
        output_db_password (str): Password for the database of statistics data
        output_db_name (str): Database name of statistics data
        verbose (bool): Verbose mode

    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)

    t_n, t_b = time.time(), time.time()

    # Load meta DB
    meta_db_handler = DBHandler(
        db_class="meta",
        db_engine=meta_db_engine,
        db_host=meta_db_host,
        db_name=meta_db_name,
        db_username=meta_db_username,
        db_password=meta_db_password,
        database_id=database_id,
        base_dir_path=meta_db_base_dir,
        read_on_init=False,
    )
    logging.info("Loading content: {}".format(q_content))
    meta_db_handler.read(where='contents like "{}"'.format(q_content))
    reader = BaseFileReader()
    calculator = BaseStatisticCalculation(span, sync_timestamps=True)
    t_n, t_p = time.time(), t_n
    logging.info("Loaded index and filtered files.({0:.03f} secs)".format(t_n - t_p))

    # Initialize DB-Handler
    stat_db_handler = DBHandler(
        db_class="statistics",
        db_engine=output_db_engine,
        db_host=output_db_host,
        db_name=output_db_name,
        db_username=output_db_username,
        db_password=output_db_password,
        database_id=database_id,
        span=span,
        read_on_init=False,
    )

    # Read data and write calculated data in DB
    def write_stat_to_db(item):
        # Load data from file and get statistical table
        timestamps, data, columns = reader.read(metadata=item)
        stat_df = calculator.statistic_tables(timestamps, data, columns)
        stat_df.insert(0, "record_id", item["record_id"])

        # Write to DB
        stat_db_handler.df = stat_df
        stat_db_handler.save()

    tqdm.pandas(desc="Load files, calculate and write")
    for sample in meta_db_handler:
        write_stat_to_db(sample)
    t_n, t_p = time.time(), t_n
    logging.info(
        "Calculated statistics and wrote to DB.({0:.03f} secs)".format(t_n - t_p)
    )

    logging.info("Done.(Total: {0:.03f} secs)".format(t_n - t_b))


def batch_script():
    """Function for tool.poetry.scripts."""
    fire.Fire(batch_analysis)


def script():
    """Function for tool.poetry.scripts."""
    fire.Fire(main)


if __name__ == "__main__":
    fire.Fire(main)
    # fire.Fire(batch_analysis)
