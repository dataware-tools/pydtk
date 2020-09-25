#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Dummy preprocessing function."""

from .preprocess import BasePreprocess


class PassThrough(BasePreprocess):
    """Pass through processing."""

    def __init__(self):
        super(PassThrough, self)

    def processing(self, timestamps, values):
        """No processing.

        Args:
            timestamps ():
            values (ndarray): Signal sequence.

        Return: Same as input.

        """
        return timestamps, values


class AddBias(BasePreprocess):
    """Add bias processing."""

    def __init__(self, bias):
        super(AddBias, self)
        self.bias = bias

    def processing(self, timestamps, values):
        """Add constant bias.

        Args:
            timestamps ():
            values (ndarray): Signal sequence.

        Return: Same as input.

        """
        return timestamps, values + self.bias
