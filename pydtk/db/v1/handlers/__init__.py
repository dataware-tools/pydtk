#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""V1DBHandler."""


import hashlib
import logging
from copy import deepcopy

import numpy as np
import vaex
from tqdm import tqdm

from pydtk.utils.utils import (
    deserialize_dict_1d,
    dicts_to_listed_dict_2d,
    dtype_string_to_dtype_object,
    listed_dict_to_dict_1d,
    load_config,
    serialize_dict_1d,
)


class BaseDBHandler(object):
    """Handler for db."""

    __version__ = "v1"
    df_name = "base_df"

    def __init__(self, path_to_db_files, path_to_output_db_file=None):
        """Initialize V1BaseDBHandler.

        Args:
            path_to_db_files (list): list fo db-files with extension '.arrow'
            path_to_output_db_file (str): output database for saving

        """
        if isinstance(path_to_db_files, str):
            path_to_db_files = [path_to_db_files]

        # Check file extension
        for file in path_to_db_files:
            if not file.endswith(".arrow"):
                raise ValueError('DB file path must ends with ".arrow"')

        self.path_to_db_file = path_to_db_files[0]
        if path_to_output_db_file is not None:
            self.path_to_db_file = path_to_output_db_file
        self._cursor = 0
        self.logger = logging.getLogger(__name__)

        # load config
        self._config = load_config(self.__version__)
        self.columns = self._config[self.df_name]["columns"]

        # initialize dataframe
        try:
            self._df = vaex.open_many(path_to_db_files)
        except IOError:
            self._df = None

    def __iter__(self):
        """Return iterator."""
        return self

    def __next__(self):
        """Return the next item."""
        if self._df is None:
            raise EOFError
        if self._cursor >= len(self.df):
            raise EOFError

        # Grab data
        data = self.df.take([self._cursor]).to_dict()

        # Delete internal column
        if "uuid_in_df" in data.keys():
            del data["uuid_in_df"]

        # Post-processes
        data = listed_dict_to_dict_1d(data)
        data = deserialize_dict_1d(data)
        for key, value in data.items():
            if isinstance(value, np.ndarray):
                if np.ma.is_masked(value):
                    data.update({key: None})

        # Increment
        self._cursor += 1

        return data

    def _get_uuid_from_item(self, item):
        """Return UUID of the given item.

        Args:
            item (dict): dict containing data

        Returns:
            (str): UUID

        """
        pre_hash = "".join(
            [
                "{:.09f}".format(item[c["name"]])
                if isinstance(item[c["name"]], float)
                else str(item[c["name"]])
                for c in self.columns
                if c["name"] in item.keys()
            ]
        )
        pre_hash = pre_hash.encode("utf-8")
        uuid = hashlib.md5(pre_hash).hexdigest()
        return uuid

    def _preprocess_list_of_dicts(self, data_in):
        """Preprocess list of dicts.

        Args:
            data_in (list): list of dicts containing data

        Returns:
            (dict): dict of lists = listed dict

        """
        data = deepcopy(data_in)

        # Serialize (convert list to str)
        self.logger.info("(Preprocess) Serializing...")
        for item in tqdm(data):
            item = serialize_dict_1d(item)

            # Add df_uuid
            item["uuid_in_df"] = self._get_uuid_from_item(item)

            # Add missing columns
            for column in self.columns:
                column_name, column_dtype = column["name"], column["dtype"]
                if column_name not in item.keys():
                    item.update({column_name: None})
                else:
                    dtype_obj = dtype_string_to_dtype_object(column_dtype)
                    if item[column_name] is not None and not isinstance(
                        item[column_name], dtype_obj
                    ):
                        try:
                            item[column_name] = dtype_obj(item[column_name])
                        except ValueError:
                            item[column_name] = np.nan

        # Convert dict to listed dict
        self.logger.info("(Preprocess) Converting...")
        data = dicts_to_listed_dict_2d(data)

        return data

    def _append_listed_dict_to_df(self, data, check_unique=True):
        """Append pre-processed dict to self._df.

        Args:
            data (dict): data to add
            check_unique (bool): if True, it will be checked that the data is unique in the db

        """
        if self._df is None:
            self._df = vaex.from_dict(data)
        else:
            if check_unique:
                # TODO(hdl-members): support unique-check for multiple items
                df_uuid = data["uuid_in_df"][0]
                if len(self.df[self.df.uuid_in_df.str.equals(df_uuid)]) > 0:
                    logging.warning(
                        "Given data already exist in dataframe: {}".format(df_uuid)
                    )
                    return
            self.df = self.df.concat(vaex.from_dict(data))

    def add_data(self, data_in, **kwargs):
        """Add data to db.

        Args:
            data_in (dict): a dict containing data

        """
        self.add_list_of_data([data_in], **kwargs)

    def add_list_of_data(self, data_in, **kwargs):
        """Add list of data to db.

        Args:
            data_in (list): a list of dicts containing data

        """
        data = self._preprocess_list_of_dicts(data_in)
        self.logger.info("Adding data to DB...")
        self._append_listed_dict_to_df(data, **kwargs)
        self.logger.info("Successfully finished adding data to DB")

    def save(self, path=None):
        """Save data to storage.

        Args:
            path (str): database path to save (default: overwrite to the original file)

        """
        if path is None:
            path = self.path_to_db_file
        self._df.export(path)

    @property
    def df(self):
        """Return df."""
        if self._df is None:
            return vaex.dataframe.DataFrameLocal(
                "df",
                self.path_to_db_file,
                [c["name"] for c in self.columns] + ["uuid_in_df"],
            )
        return self._df

    @df.setter
    def df(self, value):
        """Setter for self.df."""
        self._df = value
