#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Search engine for statistics."""

from .. import TimeSeriesDBHandler as _TimeSeriesDBHandler
from . import BaseDBSearchEngine as _BaseDBSearchEngine


class TimeSeriesDBSearchEngine(_BaseDBSearchEngine):
    """Search engine for TimeSeriesDB."""

    def __init__(self, db_handler: _TimeSeriesDBHandler, content: str, span: float):
        """Initialize StatisticsDBSearchEngine.

        Args:
            db_handler (V2TimeSeriesDBHandler): database handler
            content (str): target content (e.g. /vehicle/acceleration)
            span (float): span of statistics in seconds (e.g. 60)

        """
        super(TimeSeriesDBSearchEngine, self).__init__(db_handler)
        self._content = content
        self._span = span

    def add_condition(self, condition: str):
        """Add a search condition.

        Args:
            condition (str): search condition
                             (e.g. '"/vehicle/acceleration/accel_linear_x/mean" > 0')

        """
        if '"' not in condition:
            raise ValueError(
                "column name must be quoted "
                '(e.g. "/vehicle/acceleration/accel_linear_x/mean" > 0)'
            )
        self._conditions.append(condition)

    @property
    def select(self):
        """Return select query.

        Returns:
            (str): query

        """
        table_name = (
            self._db_handler._config.statistics_df["name"]
            .replace("/", "\/")  # NOQA
            .replace("{span}", "{0:.01f}".format(self._span).replace(".", "\."))  # NOQA
            .replace("{record_id}", ".*")
            .replace("{content}", self._content.replace("/", "\/"))  # NOQA
        )
        return "select * from /{}/".format(table_name)
