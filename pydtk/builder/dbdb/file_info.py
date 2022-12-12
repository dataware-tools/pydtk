#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)

import argparse
import json
import logging
import os
import re
from collections import OrderedDict

import rosbag


def _read_rosbag(file):
    info = {}
    with rosbag.Bag(file, "r") as bag:
        info["start_time"] = bag.get_start_time()
        info["end_time"] = bag.get_end_time()
        # get topic names
        info["topic_info"] = bag.get_type_and_topic_info()
    return info


def _read_timestamp(file):
    """Get start and end time from timestamp csv file."""
    try:
        with open(file, "r") as f:
            rows = f.readlines()
            starttime, endtime = float(rows[0].split(",")[0]), float(
                rows[-1].split(",")[0]
            )
        starttime, endtime = starttime / (10**3), endtime / (10**3)
    except IOError:
        starttime, endtime = "Nan", "Nan"
    return starttime, endtime


def _get_time_info(file):
    """Get start and end time."""
    if file.endswith(".mp4"):
        file = file.replace(".mp4", "_timestamps.csv")
        starttime, endtime = _read_timestamp(file)
    elif file.endswith(".bag"):
        bag_info = _read_rosbag(file)
        starttime, endtime = bag_info["start_time"], bag_info["end_time"]
    elif file.endswith("timestamps.csv"):
        starttime, endtime = _read_timestamp(file)
    else:
        starttime, endtime = "no info", "no info"
    return starttime, endtime


def _check_camera_contents(file):
    # camera_id = int(os.path.basename(file).split("_")[1])
    camera_id = re.split("[._]", os.path.basename(file))
    camera_id = int(camera_id[1])

    camera_info_dict = [
        "no info",
        "camera/front-center",
        "camera/front-left",
        "camera/front-right",
        "camera/rear-center",
        "camera/rear-left",
        "camera/rear-right",
        "camera/driver-face",
        "camera/driver-foot",
        "camera/driver-face-black_and_white",
    ]
    contents = camera_info_dict[camera_id]
    tags = re.split("[/-]", contents)
    return contents, tags


def _check_contents(file):
    contents = OrderedDict()
    if file.endswith("aac"):
        contents["microphone"] = {"tags": ["microphone", "sound"]}
    elif os.path.basename(file).startswith("camera"):
        contents_name, contents_tags = _check_camera_contents(file)
        if file.endswith("mp4"):
            contents_tags.append("image")
        elif file.endswith("timestamps.csv"):
            contents_tags.append("timestamps")
        contents[contents_name] = {"tags": contents_tags}
    elif file.endswith("bag"):
        bag_info = _read_rosbag(file)
        topic_info = bag_info["topic_info"]
        topics = [topic for topic in topic_info[1].keys()]
        topics.sort()
        for topic in topics:
            if topic.startswith(
                "/mobileye"
            ):  # mobileye data is not supported at this version
                continue
            contents[topic] = {}
            contents[topic]["msg_type"] = topic_info[1][topic].msg_type
            contents[topic]["msg_md5sum"] = topic_info[0][topic_info[1][topic].msg_type]
            contents[topic]["count"] = topic_info[1][topic].message_count
            contents[topic]["frequency"] = topic_info[1][topic].frequency
            contents[topic]["tags"] = re.split("[_/-]", topic[1:])
    return contents


def _check_content_type(file):
    type_dict = {
        "aac": "audio/aac",
        "csv": "text/csv",
        "mp4": "video/mp4",
        "bag": "application/rosbag",
    }
    extention = file.split(".")[-1]
    try:
        content_type = type_dict[extention]
    except KeyError:
        content_type = "unknown"
    return content_type


def dump_json(file_path, json_path):
    """Write file information in json."""
    logging.debug("Loading %s" % file_path)
    attributes = OrderedDict()

    attributes["description"] = "Driving Database"
    attributes["database_id"] = "Driving Behavior Database"
    attributes["record_id"] = file_path.split("/")[-3]
    attributes["type"] = "raw_data"  # 収録データ
    attributes["path"] = file_path
    attributes["start_timestamp"], attributes["end_timestamp"] = _get_time_info(
        file_path
    )

    attributes["content-type"] = _check_content_type(file_path)
    attributes["contents"] = _check_contents(file_path)

    with open(json_path, mode="w") as f:
        json.dump(attributes, f, ensure_ascii=False, indent=4, separators=(",", ": "))
    logging.debug("Finish writing: %s" % json_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make json file for NU data tool")
    parser.add_argument("file_path", help="Path to drive ID  directory")
    parser.add_argument("--json_path", default="", help="Path to an output json file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    args = parser.parse_args()
    json_path = args.json_path if not args.json_path == "" else args.file_path + ".json"

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    dump_json(args.file_path, json_path)
