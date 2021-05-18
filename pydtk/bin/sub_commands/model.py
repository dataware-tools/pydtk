#!/usr/bin/env python3

# Copyright Toolkit Authors

import pprint
from pydtk.models import MODELS_BY_PRIORITY


class Model(object):
    """Model related operations."""

    @staticmethod
    def list(
        **kwargs
    ):
        """List models."""
        print('Available models with priorities:')
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
        from pydtk.io import BaseFileReader
        from pydtk.io import NoModelMatchedError
        reader = BaseFileReader()
        try:
            _ = reader.read(path=file, contents=content, as_ndarray=False)
            print('True')
        except NoModelMatchedError:
            print('False')
