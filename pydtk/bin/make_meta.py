#!/usr/bin/env python3

import argparse
import json
import logging
import os

from collections import defaultdict

from pydtk.io import NoModelMatchedError
from pydtk.models import MODELS_BY_PRIORITY


common_item = {
    "database_id": "Database ID",
    "description": "Description",
    "path": "File to path",
    "type": "Type of data",
    "contents": "Contents",
}


def make_meta_interactively(template=None):
    """Make metadata with the interactive command."""
    if template is None:
        template = defaultdict(dict)
    meta = defaultdict(dict)
    for key in common_item.keys():
        if key in template.keys():
            meta[key] = str(input(f"{common_item[key]} [{template[key]}]: ") or template[key])
        else:
            meta[key] = input(f"{common_item[key]}: ")
    return meta


def make_meta(file, template=None):
    """Make metadata with a template."""
    meta = template if type(template) is dict else defaultdict(dict)
    meta["path"] = file
    if "contents" in meta.keys():
        meta["contents"] = _get_contents_info(file)
    if "start_timestamp" in meta.keys() and "end_timestamp" in meta.keys():
        meta["start_timestamp"], meta["end_timestamp"] = _get_timestamps_info(file)
    return meta


def _get_contents_info(file_path):
    """Get contents infomation from model.

    Args:
        file_path (str): path to the file

    Returns:
        (dict): contents info

    """
    model = _select_model(file_path)
    contents = model.generate_contents_meta(path=file_path)
    return contents


def _get_timestamps_info(file_path):
    """Get contents infomation from model.

    Args:
        file_path (str): path to the file

    Returns:
        (list): [start_timestamp, end_timestamp]

    """
    model = _select_model(file_path)
    timetamps_info = model.generate_timestamp_meta(path=file_path)
    return timetamps_info


def _select_model(file_path):
    """Select a proper model based on the given file-metadata.

    Args:
        file_path (str): File path

    """
    priorities = MODELS_BY_PRIORITY.keys()
    for priority in sorted(priorities, reverse=True):
        for model in MODELS_BY_PRIORITY[priority]:
            if model.is_loadable(path=file_path):
                return model
    raise NoModelMatchedError('No suitable model found for loading data: {}'.format(file_path))


def get_arguments():
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="Metadata maker.")
    parser.add_argument(
        "-it",
        action="store_true",
        help="interactive mode",
    )
    parser.add_argument(
        "--template",
        type=str,
        default=None,
        help="path to json has metadata template",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="file to make metadata",
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        default="./",
        help="output directory",
    )
    return parser.parse_args()


def main():
    """Make metadata."""
    # set logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s (%(module)s:%(lineno)d) %(levelname)s: %(message)s",
    )
    args = get_arguments()
    if args.template is not None:
        with open(args.template, "r") as f:
            template = json.load(f)
    else:
        template = None

    if args.it:
        meta = make_meta_interactively(template)
        meta_json = input("output json: ")
    else:
        if args.file is None:
            raise ValueError("following arguments are required: --file")
        meta = make_meta(args.file, template)
        meta_json = os.path.join(args.out_dir, os.path.basename(args.file) + ".json")

    with open(meta_json, "wt") as f:
        json.dump(meta, f, indent=4)
    logging.info(f"Dumped: {meta_json}")


if __name__ == "__main__":
    main()
