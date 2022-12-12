#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)

import argparse
import json
import logging
import pickle
import time
from pathlib import Path

import pandas as pd

from pydtk.utils import utils


def _make_record_id_list(file_df, content_df):
    """Show record id list.

    Returns:
        (list): List of record id.

    """
    record_id_list = file_df["record_id"].unique()
    duration_list, tags_list = [], []
    start_timestamp_list, end_timestamp_list = [], []
    for record_id in record_id_list:
        logging.debug("Making record id indexing: %s" % record_id)
        mask = file_df["record_id"].apply(lambda x: record_id in x)
        record_id_df = file_df[mask]
        start_timestamp, end_timestamp = utils.take_time_and(record_id_df)
        duration = max(0.0, end_timestamp - start_timestamp)
        tags = []
        for tag_list in content_df["tag"]:
            for tag in tag_list:
                if tag not in tags:
                    tags += [tag]

        duration_list += [duration]
        start_timestamp_list += [start_timestamp]
        end_timestamp_list += [end_timestamp]
        tags_list += [tags]

    record_id_df = pd.DataFrame(
        data={
            "record_id": record_id_list,
            "duration": duration_list,
            "start_timestamp": start_timestamp_list,
            "end_timestamp": end_timestamp_list,
            "tags": tags_list,
        },
        columns=["record_id", "duration", "start_timestamp", "end_timestamp", "tags"],
    )
    return record_id_df


def find_json(db_dir):
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


def _filter_json_list(json_list, start, end):
    """Now only support Driving Behavior Databse(DBDB)."""
    json_list_filtered = []
    for json_file in json_list:
        hdd_id = str(json_file).split("/")[-3].split("_")[0]
        if hdd_id.startswith("B") or hdd_id.startswith("W"):
            hdd_id = "400"
        if start <= int(hdd_id) <= end:
            json_list_filtered += [json_file]
    return json_list_filtered


def build_df_list(db_dir, pkl_file, start, end):
    """Search with tags.

    Args:
        db_dir (str): Directory path which has database files.
        pkl_file (str): Pickle file path to save.

    """
    logging.info("Making pickle: %s" % pkl_file)
    t0 = time.time()
    json_list = find_json(db_dir)
    if not (start == 0 and end == 999):
        json_list = _filter_json_list(json_list, start, end)

    t1 = time.time()
    logging.info("Find %d files.(%d secs)" % (len(json_list), t1 - t0))

    file_columns = [
        "path",
        "record_id",
        "type",
        "content-type",
        "start_timestamp",
        "end_timestamp",
    ]
    # "description", "database_id"

    # Initialize list
    file_path_list, file_record_id_list = [], []
    file_type_list, file_content_type_list = [], []
    file_start_timestamp_list, file_end_timestamp_list = [], []

    content_record_id_list, content_path = [], []
    content_contents_list, content_tag_list = [], []
    content_msg_type_list = []

    for json_path in json_list:
        with open(json_path) as f:
            df = json.load(f)
        # Add file list
        file_path_list += [df["path"]]
        file_record_id_list += [df["record_id"]]
        file_type_list += [df["type"]]
        file_content_type_list += [df["content-type"]]
        file_start_timestamp_list += [df["start_timestamp"]]
        file_end_timestamp_list += [df["end_timestamp"]]

        # Add content list
        for content in df["contents"]:
            content_record_id_list += [df["record_id"]]
            content_path += [df["path"]]
            content_contents_list += [content]
            try:
                content_msg_type_list += [df["contents"][content]["msg_type"]]
            except KeyError:
                content_msg_type_list += ["None"]
            content_tag_list += [df["contents"][content]["tags"]]
    t2 = time.time()
    logging.info("Finish loading json.(%d secs)" % (t2 - t1))

    # 一気に列を結合したほうが早いらしい
    file_df = pd.DataFrame(
        data={
            "path": file_path_list,
            "record_id": file_record_id_list,
            "type": file_type_list,
            "content-type": file_content_type_list,
            "start_timestamp": file_start_timestamp_list,
            "end_timestamp": file_end_timestamp_list,
        },
        columns=file_columns,
    )

    content_df = pd.DataFrame(
        data={
            "record_id": content_record_id_list,
            "path": content_path,
            "content": content_contents_list,
            "msg_type": content_msg_type_list,
            "tag": content_tag_list,
        },
        columns=["record_id", "path", "content", "msg_type", "tag"],
    )

    record_id_df = _make_record_id_list(file_df, content_df)
    t3 = time.time()
    logging.info("Finish making list of record id.(%d secs)" % (t3 - t2))

    with open(pkl_file, "wb") as f:
        pickle.dump(
            {
                "file_df": file_df,
                "content_df": content_df,
                "record_id_df": record_id_df,
            },
            f,
            protocol=2,
        )
    t4 = time.time()
    logging.info("Finish saving pickle file.(Total: %d secs)" % (t4 - t0))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script is bababa")
    parser.add_argument(
        "--db_dir",
        default="/data_pool_1/DrivingBehaviorDatabase/records",
        help="Path to database directory",
    )
    parser.add_argument(
        "--pkl", default="pydtk_DBDB.pkl", help="Pickle file path to save"
    )
    parser.add_argument("--start", default=0, type=int, help="Start HDD ID for index")
    parser.add_argument("--end", default=999, type=int, help="End HDD ID for index")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    db_dir, pkl = args.db_dir, args.pkl
    start, end = args.start, args.end
    build_df_list(db_dir, pkl, start, end)
