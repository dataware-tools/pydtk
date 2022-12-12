#!/usr/bin/env python3

# Copyright Toolkit Authors

import pprint


class IO(object):
    """IO related operations."""

    @staticmethod
    def read(
        file: str,
        content: str = None,
    ):
        """Read a given file.

        Args:
            file (str): Path to a file
            content (str): Content in the file to read

        """
        from pydtk.io import BaseFileReader

        reader = BaseFileReader()
        timestamps, data, columns = reader.read(
            path=file, contents=content, as_ndarray=False
        )

        pprint.pprint(data)
