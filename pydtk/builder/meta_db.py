#!/usr/bin/env python3

# Copyright Toolkit Authors

import logging
import os
import time
from pathlib import Path

import fire
from tqdm import tqdm

from pydtk.db import V3DBHandler, V4DBHandler
from pydtk.db.v4.engines import DB_ENGINES
from pydtk.models import MetaDataModel


def _find_json(db_dir):
    """Search with tags.

    Args:
        db_dir (str): Directory path which has database files.
    Returns:
        (list): List of json file path.

    """
    p = Path(db_dir)
    json_list = list(p.glob("**/*.json"))
    json_list.sort()
    return json_list


def main(
    target_dir,
    database_id="default",
    base_dir=None,
    output_db_engine=None,
    output_db_host=None,
    output_db_username=None,
    output_db_password=None,
    output_db_name=None,
    verbose=False,
):
    """Create meta_db.

    Args:
        target_dir (str): Path to database directory
        database_id (str): ID of the database (e.g. "Driving Behavior Database")
        base_dir (str): Directory path where each file-path will be based on
        output_db_engine (str): Database engine
        output_db_host (str): HOST of database
        output_db_username (str): Username for the database
        output_db_password (str): Password for the database
        output_db_name (str): Database name
        verbose (bool): Verbose mode

    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Check
    if not os.path.isdir(target_dir):
        raise IOError("No such directory: {}".format(target_dir))

    # Select DB-Handler
    db_engine = os.environ.get("PYDTK_META_DB_ENGINE", None)
    if output_db_engine is not None:
        db_engine = output_db_engine
    if db_engine is None:
        DBHandler = V4DBHandler
    else:
        if db_engine in DB_ENGINES.keys():
            DBHandler = V4DBHandler
        else:
            DBHandler = V3DBHandler

    # Search metadata files
    t0 = time.time()
    logging.info("Searching json files...")
    json_list = _find_json(target_dir)
    t1 = time.time()
    logging.info("Found {0} files.({1:.03f} secs)".format(len(json_list), t1 - t0))

    # Preparation
    base_dir_path = base_dir if base_dir is not None else target_dir
    handler = DBHandler(
        db_class="meta",
        db_engine=output_db_engine,
        db_host=output_db_host,
        db_username=output_db_username,
        db_password=output_db_password,
        db_name=output_db_name,
        database_id=database_id,
        base_dir_path=base_dir_path,
    )

    # Append metadata to db
    logging.info("Loading metadata...")
    for path in tqdm(json_list, desc="Load metadata", leave=False):
        if not MetaDataModel.is_loadable(path):
            logging.warning("Failed to load metadata file: {}, skipped".format(path))
            continue
        metadata = MetaDataModel()
        metadata.load(path)
        handler.add_data(metadata.data)
    t2 = time.time()
    logging.info("Finished loading metadata.({0:.03f} secs)".format(t2 - t1))

    # Export
    logging.info("Saving DB file...")
    handler.save(remove_duplicates=True)
    t3 = time.time()
    logging.info("Finished saving DB file.({0:.03f} secs)".format(t3 - t2))

    # Display
    logging.info("Done.(Total: {0:.03f} secs)".format(t3 - t0))


def script():
    """Function for tool.poetry.scripts."""
    fire.Fire(main)


if __name__ == "__main__":
    fire.Fire(main)
