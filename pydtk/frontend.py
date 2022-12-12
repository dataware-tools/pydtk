#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle

from pydtk.db import V3MetaDBHandler as MetaDBHandler


class LoadPKL:
    """Class for database search."""

    def __init__(self, pkl):
        self.pickle = pkl
        with open(pkl, "rb") as f:
            df_dict = pickle.load(f)
            self.content_df = df_dict["content_df"]
            self.file_df = df_dict["file_df"]
            self.record_id_df = df_dict["record_id_df"]

    def get_record_id_info(self):
        """Show record id list.

        Returns:
            (list): List of record id.

        """
        record_id_list = []
        for index, row in self.record_id_df.iterrows():
            id_dict = {
                "record_id": row["record_id"],
                "duration": row["duration"],
                "start_timestamp": row["start_timestamp"],
                "end_timestamp": row["end_timestamp"],
                "tags": row["tags"],
            }
            record_id_list += [id_dict]
        return record_id_list


class LoadDB(object):
    """Class for database search."""

    def __init__(self, path_to_db):
        super(LoadDB, self).__init__()
        self.path_to_db = path_to_db
        self.db_handler = MetaDBHandler(path_to_db)

    def get_record_id_info(self):
        """Show record info.

        Returns:
            (list): List of record information.

        """
        return self.db_handler.record_id_df.to_dict("records")
