#!/usr/bin/env python3

import argparse
import json
import logging

from collections import defaultdict

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
            meta[key] = str(input(f"{common_item[key]}[{template[key]}]: ") or template[key])
        else:
            meta[key] = input(f"{common_item[key]}: ")
    return meta


def make_meta(path, template=None):
    """MAke metadata with a template."""
    meta = template if type(template) is dict else defaultdict(dict)
    meta["path"] = path
    if "contents" not in meta.keys():
        meta["contents"] = _get_contents_template(path)
    return meta


def _get_contents_template(file_path):
    """Get contents infomation from model.

    Args:
        file_path (str): path to the file

    """
    metadata = defaultdict(dict)
    metadata["path"] = file_path
    model = _select_model(metadata)
    contents = model._contents
    return contents


def _select_model(file_metadata):
    """Select a proper model based on the given file-metadata.

    Args:
        file_metadata (object): an MetaDataModel object

    """
    priorities = MODELS_BY_PRIORITY.keys()
    for priority in sorted(priorities, reverse=True):
        for model in MODELS_BY_PRIORITY[priority]:
            if model.is_loadable(** file_metadata):
                return model
    raise NoModelMatchedError('No suitable model found for loading data: {}'.
                               format(file_metadata))


def get_arguments():
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="Metadata maker.")
    parser.add_argument("-it",
        # type=bool,
        action="store_true",
        help="interactive mode",
    )
    parser.add_argument("--template",
        type=str,
        default=None,
        help="path to json has metadata template",
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
            template = json.loads(f)
    else:
        template = None

    if args.it:
        meta = make_meta_interactively(template)
    else:
        meta = make_meta(template)


if __name__ == "__main__":
    main()
