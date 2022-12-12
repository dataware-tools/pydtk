#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Search engine for time-series data."""

from .. import StatisticsCassandraDBHandler as _StatisticsCassandraDBHandler
from .. import TimeSeriesCassandraDBHandler as _TimeSeriesCassandraDBHandler
from . import BaseDBSearchEngine as _BaseDBSearchEngine
from . import register_engine


@register_engine(
    db_handlers=[_TimeSeriesCassandraDBHandler, _StatisticsCassandraDBHandler]
)
class TimeSeriesCassandraDBSearchEngine(_BaseDBSearchEngine):
    """Search engine for V3TimeSeriesCassandraDB."""

    def __init__(self, db_handler: _TimeSeriesCassandraDBHandler):
        """Initialize TimeSeriesCassandraDBSearchEngine.

        Args:
            db_handler (V3TimeSeriesCassandraDBHandler): database handler

        """
        super(TimeSeriesCassandraDBSearchEngine, self).__init__(db_handler)
        self._span = db_handler._span

    def add_condition(self, condition: str):
        """Add a search condition.

        Args:
            condition (str): search condition
                             (e.g. "'/vehicle/acceleration/accel_linear_x/mean' > 0")

        """
        if "record_id" in condition:
            if "=" not in condition:
                raise ValueError(
                    'record_id must be filtered using operator "=" '
                    "(e.g. record_id = '242_16000000080000000570' )"
                )
            if "like" in condition.lower():
                raise ValueError(
                    'record_id must be filtered using operator "=" '
                    "(e.g. record_id = '242_16000000080000000570' )"
                )
            if "'" not in condition:
                raise ValueError(
                    "string must be single-quoted "
                    "(e.g. record_id = '242_16000000080000000570' )"
                )
        else:
            if '"' not in condition:
                raise ValueError(
                    "column name must be double-quoted "
                    '(e.g. "/vehicle/acceleration/accel_linear_x/mean" > 0)'
                )
        self._conditions.append(condition)

    @property
    def query(self):
        """Return query.

        Returns:
            (str): query

        """
        if self.condition == "":
            return self.select
        return "{0} where {1} allow filtering".format(self.select, self.condition)
