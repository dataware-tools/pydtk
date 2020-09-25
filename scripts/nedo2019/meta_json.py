#!/usr/bin/env python3

# Copyright Toolkit Authors

import argparse
from collections import OrderedDict
import json
import logging
import os
import re
import rosbag


def load_bag_info(file):
    """Load bag file and get information."""
    info = {}
    with rosbag.Bag(file, "r") as bag:
        info["start_time"] = bag.get_start_time()
        info["end_time"] = bag.get_end_time()
        # get topic names
        info["topic_info"] = bag.get_type_and_topic_info()
    return info


def get_time_info(file):
    """Get time information from bag file."""
    if file.endswith("mp4"):
        bagfile = file.replace(".mp4", ".bag")
    if file.endswith("csv"):
        bagfile = os.path.basename(file).replace(".csv", ".bag")
        bagfile = os.path.join(os.path.dirname(file), "../data", bagfile)
    elif file.endswith(".bag"):
        bagfile = file
    info = load_bag_info(bagfile)
    return info["start_time"], info["end_time"]


def get_contents_info(file):
    """Get topic info from rosbag."""
    contents = OrderedDict()
    bag_info = load_bag_info(file)
    topic_info = bag_info["topic_info"]
    topics = [topic for topic in topic_info[1].keys()]
    topics.sort()
    for topic in topics:
        if topic.startswith("/mobileye"):  # mobileye data is not supported at this version
            continue
        contents[topic] = {}
        contents[topic]["msg_type"] = topic_info[1][topic].msg_type
        contents[topic]["msg_md5sum"] = topic_info[0][topic_info[1][topic].msg_type]
        contents[topic]["count"] = topic_info[1][topic].message_count
        contents[topic]["frequency"] = topic_info[1][topic].frequency
        contents[topic]["tags"] = re.split('[_/-]', topic[1:])
    return contents


def get_record_id(file):
    """Get record ID from file name or path."""
    record_id = os.path.basename(file).split(".")[0]
    return record_id


def make(template, file, metafile=None):
    """Make metafile with a json format."""
    if metafile is None:
        metafile = file + ".json"
    logging.debug("Making metadata: %s" % metafile)
    with open(template, "r") as f:
        meta = json.load(f)
    meta["record_id"] = get_record_id(file)
    meta["type"] = "annotation" if file.endswith("csv") else "raw_data"
    meta["path"] = file
    try:
        meta["start_timestamp"], meta["end_timestamp"] = get_time_info(file)
        if file.endswith(".csv"):
            meta["content-type"] = "text/csv"
            meta["contents"] = {"risk_annotation": {"tags": ["risk_score", "scene_description", "risk_factor"]}}
        elif file.endswith(".mp4"):
            meta["content-type"] = "video/mp4"
            meta["contents"] = {"camera/front-center": {"tags": ["camera", "front", "center", "image"]}}
        elif file.endswith(".bag"):
            meta["content-type"] = "application/rosbag"
            meta["contents"] = get_contents_info(file)
        with open(metafile, mode="w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4, separators=(',', ': '))
        logging.debug("Finish writing: %s" % metafile)
    except rosbag.bag.ROSBagException:
        logging.info("Pass writing: %s. Because of no message in the rosbag." % metafile)


def dump_meta():
    """Make metadata of file in the root_dir."""
    template = "template.json"
    root_dir = "/work5/share/NEDO/distribution/data/records"
    record_id_list = os.listdir(root_dir)
    data_types = ["data", "annotation"]
    data_types = data_types[1:]
    for data_type in data_types:
        data_dir_list = [os.path.join(root_dir, record_id, data_type) for record_id in record_id_list]
        data_dir_list = [dirpath for dirpath in data_dir_list if os.path.isdir(dirpath)]
        data_dir_list.sort()

        for data_dir in data_dir_list:
            files = [os.path.join(data_dir, file) for file in os.listdir(data_dir)]
            for file in files:
                if os.path.isfile(file) and not file.endswith("json"):
                    make(template, file)


def get_arguments():
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="Make json file for NUDrive toolkit")
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_arguments()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    dump_meta()
