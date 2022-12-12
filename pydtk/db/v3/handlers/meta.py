#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Toolkit Authors

import hashlib
import logging
import os
from copy import deepcopy
from pathlib import Path

from tqdm import tqdm

from . import BaseDBHandler as _BaseDBHandler
from . import register_handler


@register_handler(
    db_classes=["meta"], db_engines=["sqlite", "mysql", "mariadb", "postgresql"]
)
class MetaDBHandler(_BaseDBHandler):
    """Handler for metadb."""

    _database_id: str = ""
    _df_class = "meta_df"
    _df_name = "meta_df"

    def __init__(self, database_id="default", base_dir_path=None, **kwargs):
        """Initialize MetaDBHandler.

        Args:
            database_id (str): ID of the database (e.g. "Driving Behavior Database")
            base_dir_path (str): base directory for path

        """
        self._database_id = database_id
        super(MetaDBHandler, self).__init__(**kwargs)

        # Solve base-dir
        base_dir = base_dir_path
        if base_dir is None:
            if self._config.current_db["engine"] == "sqlite":
                base_dir = os.path.dirname(self._config.current_db["host"])
            else:
                base_dir = "/"
        self.base_dir_path = os.path.realpath(base_dir)

        # Prepare another DB-handler for storing a list of database_id
        self._database_id_db_handler = DatabaseIDDBHandler(
            **{**kwargs, "read_on_init": False}
        )

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_META_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_META_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_META_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_META_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_META_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)

    def _serialize_contents_and_solve_path(self, data_in):
        """Serialize contents and fix absolute path to relative one.

        Args:
            data_in (dict): a dict containing metadata

        Returns:
            (list): list of serialized dicts

        """
        assert "contents" in data_in.keys()
        assert isinstance(data_in["contents"], dict)

        content_columns = self._config[self._df_class]["content_columns"]

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

        # Serialize contents
        data_out = []
        for content_name, content_info in data_in["contents"].items():
            for column_name in content_info.keys():
                if column_name not in content_columns:
                    raise KeyError(
                        'Unrecognized key "{0}" found in content "{1}".'
                        "  Please check the config file".format(
                            column_name, content_name
                        )
                    )
            data = deepcopy(data_in)
            data["contents"] = content_name
            data.update(content_info)
            data_out.append(data)

        return data_out

    def _deserialize_contents_and_solve_path(self, data_in):
        """Deserialize contents and fix relative path to absolute one.

        Args:
            data_in (dict): a dict containing metadata

        Returns:
            (dict): metadata with deserialized content

        """
        assert "contents" in data_in.keys()
        assert isinstance(data_in["contents"], str)

        content_columns = self._config[self._df_class]["content_columns"]

        data_out = data_in

        # Deserialize contents
        content_name = data_out["contents"]
        data_out["contents"] = {content_name: {}}
        for column_name in content_columns:
            data_out["contents"][content_name][column_name] = data_out[column_name]
            del data_out[column_name]

        # Convert relative path to absolute path
        if "path" in data_out.keys():
            data_out["path"] = os.path.join(self.base_dir_path, data_out["path"])

        return data_out

    def add_list_of_data(self, data_in, **kwargs):
        """Add listed data to db.

        Args:
            data_in (list): a list of dicts containing data

        """
        self.logger.info("(Preprocess) Pre-processing metadata")
        data_flat = []
        for data_item in tqdm(data_in, desc="Pre-process", leave=False):
            data_flat += self._serialize_contents_and_solve_path(data_item)
        super().add_list_of_data(data_flat, **kwargs)

    def __next__(self):
        """Return the next item."""
        data = super().__next__()

        # Deserialize content
        data = self._deserialize_contents_and_solve_path(data)

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

    def save(self, *args, **kwargs):
        """Save function."""
        super().save(*args, **kwargs)
        self._database_id_db_handler.add_data(
            {"database_id": self._database_id, "df_name": self._df_name}
        )
        self._database_id_db_handler.save(remove_duplicates=True)

    @property
    def content_df(self):
        """Return content_df.

        Columns: record_id, path, content, msg_type, tag

        """
        df = self.df[["record_id", "path", "contents", "msg_type", "tags"]]
        df = df.rename(columns={"contents": "content", "tags": "tag"})
        df["tag"] = df["tag"].apply(lambda x: list(set(x.split(";"))))
        df["path"] = df["path"].apply(lambda x: os.path.join(self.base_dir_path, x))
        return df

    @property
    def file_df(self):
        """Return file_df.

        Columns: path, record_id, type, content_type, start_timestamp, end_timestamp

        """
        df = self.df[
            [
                "path",
                "record_id",
                "data_type",
                "content_type",
                "start_timestamp",
                "end_timestamp",
            ]
        ]
        df = df.rename(columns={"data_type": "type", "content_type": "content-type"})
        df = df.groupby(["path"], as_index=False).agg(
            {
                "record_id": "first",
                "type": "first",
                "content-type": "first",
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
        df = df.groupby(["record_id"], as_index=False).agg(
            {"start_timestamp": "min", "end_timestamp": "max", "tags": ":".join}
        )
        df["duration"] = df.end_timestamp - df.start_timestamp
        df["tags"] = df["tags"].apply(lambda x: list(set(x.split(";"))))
        return df

    @property
    def _df_name(self):
        """Return _df_name."""
        template = self._config[self._df_class]["df_name"]
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode("utf-8"), digest_size=self._config.hash.digest_size
        ).hexdigest()
        return template.format(**{"database_id": database_id_hashed})

    @_df_name.setter
    def _df_name(self, value):
        """Setter for self._df_name."""
        raise RuntimeError("Setting df_name is not supported in MetaDBHandler")


@register_handler(
    db_classes=["database_id"], db_engines=["sqlite", "mysql", "mariadb", "postgresql"]
)
class DatabaseIDDBHandler(_BaseDBHandler):
    """Handler for database-id."""

    _df_class = "database_id_df"
    _df_name = "database_id_df"

    def _initialize_engine(
        self,
        db_engine=None,
        db_host=None,
        db_name=None,
        db_username=None,
        db_password=None,
    ):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_META_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_META_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_META_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_META_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_META_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)
