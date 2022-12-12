#!/usr/bin/env python3

# Copyright Toolkit Authors

import importlib
import json
import os
import pprint


class Model(object):
    """Model related operations."""

    @staticmethod
    def list(**kwargs):
        """List models."""
        from pydtk.models import MODELS_BY_PRIORITY

        print("Available models with priorities:")
        pprint.pprint(MODELS_BY_PRIORITY)

    @staticmethod
    def is_available(
        file: str,
        content: str = None,
    ):
        """Test if available models exist against the given file.

        Args:
            file (str): Path to a file
            content (str): Content in the file to read

        """
        from pydtk.io import BaseFileReader, NoModelMatchedError

        reader = BaseFileReader()
        try:
            _ = reader.read(path=file, contents=content, as_ndarray=False)
            print("True")
        except NoModelMatchedError:
            print("False")

    @staticmethod
    def generate(
        target: str,
        model: str = None,
        from_file: str = None,
        template: str = None,
        database_id: str = "default",
        record_id: str = None,
        content: str = "content",
        base_dir: str = None,
    ):
        """Generate template or metadata from a model or a file.

        Args:
            target (str): What to to generate ('template' or 'metadata')
            model (str): Model to use (e.g., 'pydtk.models.csv.GenericCsvModel')
            from_file (str): File path to generate metadata from
            template (str): JSON file to use as a template of metadata
            database_id (str): Database ID
            record_id (str): Record ID
            content (str): Content (key of dict `contents`)
            base_dir (str): Base directory

        """
        assert target in [
            "template",
            "metadata",
        ], 'Target must be either "template" or "metadata"'

        from pydtk.db import DBHandler
        from pydtk.io import BaseFileReader
        from pydtk.models import MetaDataModel
        from pydtk.utils.utils import load_config

        default_config = load_config("v4").bin.make_meta
        data = {k: None for k in default_config["common_item"].keys()}

        db_handler = DBHandler(db_class="meta", database_id=database_id)
        config = db_handler.config

        if target == "template":
            data.update({column["name"]: None for column in config["columns"]})

            if template:
                data.update(template)

        elif target == "metadata":
            if template:
                f = open(template, "r")
                template_data = f.read()
                f.close()
                data.update(json.loads(template_data))

            if from_file is None:
                raise ValueError(
                    'Please specify a file to generate metadata from using option "--from-file"'
                )

            path = os.path.abspath(from_file)
            if base_dir:
                path = os.path.relpath(from_file, base_dir)

            data["path"] = path

            # Get model object
            if model is None:
                metadata = MetaDataModel(data={**data, "path": from_file})
                model = BaseFileReader._select_model(metadata)
            else:
                model = getattr(
                    importlib.import_module(".".join(model.split(".")[:-1])),
                    model.split(".")[-1],
                )

            # Get contents and timestamps
            try:
                data["contents"] = model.generate_contents_meta(
                    path=from_file, content_key=content
                )
            except NotImplementedError:
                pass
            try:
                (
                    data["start_timestamp"],
                    data["end_timestamp"],
                ) = model.generate_timestamp_meta(path=from_file)
            except NotImplementedError:
                pass

        # Post process
        if record_id is not None:
            data["record_id"] = record_id

        # Display
        print(json.dumps(data, indent=4, default=_default_json_handler))


def _default_json_handler(o):
    return o.__str__()
