#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Base preprocess modules."""

from abc import ABCMeta


class BasePreprocess(metaclass=ABCMeta):
    """Base Preprocess."""

    def __init__(self):
        pass

    def processing(self, timestamps, data):
        """Process signals."""
        raise NotImplementedError()
