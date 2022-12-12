#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Toolkit Authors

import logging
import os
from copy import deepcopy
from pathlib import Path

from tqdm import tqdm

from . import BaseDBHandler as _BaseDBHandler


class MetaDBHandler(_BaseDBHandler):
    """Handler for metadb."""

    df_name = "meta_df"

    def __init__(self, *args, **kwargs):
        super(MetaDBHandler, self).__init__(*args, **kwargs)
        self.base_dir_path = os.path.dirname(os.path.realpath(self.path_to_db_file))

    def _preprocess_item(self, data_in):
        """Preprocess.

        Args:
            data_in (dict): a dict containing metadata

        Returns:
            (list): list of serialized dicts

        """
        assert "contents" in data_in.keys()
        assert isinstance(data_in["contents"], dict)

        # Convert absolute path to relative path
        if "path" in data_in.keys():
            # noinspection PyTypeChecker
            data_path = Path(data_in["path"])
            try:
                relative_path = data_path.relative_to(self.base_dir_path)
                data_in["path"] = relative_path
            except ValueError as e:
                logging.warning(
                    "Could not resolve relative path to file: {}".format(data_path)
                )
                logging.warning(str(e))

        # Parse contents
        data_out = []
        for content_name, content_info in data_in["contents"].items():
            data = deepcopy(data_in)
            data["content"] = content_name
            data.update(content_info)
            del data["contents"]
            data_out.append(data)

        return data_out

    def add_list_of_data(self, data_in, **kwargs):
        """Add listed data to db.

        Args:
            data_in (list): a list of dicts containing data

        """
        self.logger.info("(Preprocess) Pre-processing metadata")
        data_flat = []
        for data_item in tqdm(data_in):
            data_flat += self._preprocess_item(data_item)
        super().add_list_of_data(data_flat, **kwargs)

    def __next__(self):
        """Return the next item."""
        data = super().__next__()

        # Convert relative path to absolute path
        if "path" in data.keys():
            data["path"] = os.path.join(self.base_dir_path, data["path"])

        return data

    def get_content_df(self):
        """Return content_df."""
        return self.content_df

    def get_file_df(self):
        """Return file_df."""
        return self.file_df

    def get_record_id_df(self):
        """Return record_id_df."""
        return self.record_id_df

    @property
    def content_df(self):
        """Return content_df.

        Columns: record_id, path, content, msg_type, tag

        """
        df = self.df[["record_id", "path", "content", "msg_type", "tags"]]
        df = df.to_pandas_df(strings=True, virtual=True)
        df = df.rename(
            columns={"tags": "tag"}
        )  # FIXME: use virtual-column before converting to pandas df
        df["tag"] = df["tag"].apply(lambda x: list(set(x.split(":"))))
        df["path"] = df["path"].apply(lambda x: os.path.join(self.base_dir_path, x))
        return df

    @property
    def file_df(self):
        """Return file_df.

        Columns: path, record_id, type, content_type, start_timestamp, end_timestamp

        """
        # FIXME: Use VAEX's aggregation function instead of pandas'
        df = self.df[
            [
                "path",
                "record_id",
                "type",
                "content_type",
                "start_timestamp",
                "end_timestamp",
            ]
        ]
        df = df.to_pandas_df(strings=True, virtual=True)
        df = df.groupby(["path"], as_index=False).agg(
            {
                "record_id": "first",
                "type": "first",
                "content_type": "first",
                "start_timestamp": "min",
                "end_timestamp": "max",
            }
        )
        df["path"] = df["path"].apply(lambda x: os.path.join(self.base_dir_path, x))
        return df

    @property
    def record_id_df(self):
        """Return record_id_df.

        Columns: 'record_id', 'duration', 'start_timestamp', 'end_timestamp', 'tags'

        """
        df = self.df[["record_id", "start_timestamp", "end_timestamp", "tags"]]
        df["duration"] = df.end_timestamp - df.start_timestamp
        df = df.to_pandas_df(strings=True, virtual=True)

        # FIXME: Use VAEX's aggregation function instead of pandas'
        df = df.groupby(["record_id"], as_index=False).agg(
            {
                "duration": "max",
                "start_timestamp": "min",
                "end_timestamp": "max",
                "tags": ":".join,
            }
        )
        df["tags"] = df["tags"].apply(lambda x: list(set(x.split(":"))))
        return df
