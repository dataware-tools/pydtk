# -*- coding: utf-8 -*-

# Copyright Toolkit Authors
# Created by Tomoki Hayashi

"""Test script example with Pytest."""

import pytest


def test_dummy():
    """Run the most simple dummy test."""
    text = "hello, world!"
    assert isinstance(text, str)


@pytest.mark.parametrize(
    "a, b, c", [
        ("this", "is", "test"),
        ("that", "is", "test"),
        ("it", "is", "test"),
    ])
def test_pytest_decorator(a, b, c):
    """Run test with pytest decorator."""
    print(f"{a} {b} {c}")
