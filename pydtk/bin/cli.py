#!/usr/bin/env python3

# Copyright Toolkit Authors

import sys

import fire


def _call_as_subcommand(component):
    fire.Fire(component, command=sys.argv[2:], name=sys.argv[1])


class CLI(object):
    """CLI of PyDTK.

    PyDTK is a toolkit for managing, retrieving, and processing data
    """

    @staticmethod
    def db():
        """Database related operations."""
        from pydtk.bin.sub_commands.db import DB
        _call_as_subcommand(DB)

    @staticmethod
    def model():
        """Model related operations."""
        from pydtk.bin.sub_commands.model import Model
        _call_as_subcommand(Model)

    @staticmethod
    def io():
        """IO related operations."""
        from pydtk.bin.sub_commands.io import IO
        _call_as_subcommand(IO)


def script():
    """Function for tool.poetry.scripts."""
    if len(sys.argv) > 1:
        fire.Fire(CLI, command=sys.argv[1:2])
    else:
        fire.Fire(CLI)


if __name__ == "__main__":
    script()
