#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""V4DBHandler."""

import importlib
import logging
import os

DB_ENGINES = {}  # key: db_class, value: dict( key: db_engine, value: handler )

logger = logging.getLogger(__name__)


def register_engines():
    """Register engines."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__))):
        if not os.path.isfile(os.path.join(os.path.dirname(__file__), filename)):
            continue
        if filename == "__init__.py":
            continue

        try:
            engine_name = str(os.path.splitext(filename)[0])
            module_name = os.path.join("pydtk.db.v4.engines", engine_name).replace(
                os.sep, "."
            )
            DB_ENGINES[engine_name] = importlib.import_module(module_name)
        except ModuleNotFoundError:
            logger.warning("Failed to load DB-engine {}".format(filename))


register_engines()
