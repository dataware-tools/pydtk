#!/usr/bin/env python3

# Copyright Toolkit Authors

import logging
import re
import sys

import fire


def _call_as_subcommand(component):
    fire.Fire(component, command=sys.argv[2:], name=sys.argv[1])


def _check_pep515(args):
    """Check if argv contains `<number>_<number>`.

    Args:
        args (List[str]): input arguments

    """
    match = re.compile(r"^[0-9]+(_[0-9]+)+$")
    for arg in args:
        if match.fullmatch(arg) is not None:
            raise ValueError(
                f'Invalid value "{arg}": values with format "<number>_<number>" is not allowed'
            )


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

    @staticmethod
    def version():
        """Show PyDTK's version."""
        import pydtk

        print(f"Version: {pydtk.__version__}")
        print(f"Commit ID: {pydtk.__commit_id__}")

    @staticmethod
    def status():
        """Status check operations."""
        from pydtk.bin.sub_commands.status import STATUS

        _call_as_subcommand(STATUS)


def script():
    """Function for tool.poetry.scripts."""
    verbose = False
    if "-v" in sys.argv:
        verbose = True
        del sys.argv[sys.argv.index("-v")]
    if "--verbose" in sys.argv:
        verbose = True
        del sys.argv[sys.argv.index("--verbose")]

    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
        sys.tracebacklimit = 0

    # Check args
    _check_pep515(sys.argv)

    if len(sys.argv) > 1:
        fire.Fire(CLI, command=sys.argv[1:2])
    else:
        fire.Fire(CLI)


if __name__ == "__main__":
    script()
