#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""Test metadata loader script with Pytest."""


def test_dict_reg_match():
    """Test for dict_reg_match function."""
    from pydtk.utils.utils import dict_reg_match

    dict_1 = {
        'camera/.*': {
            'tags': ['.*'],
            'msg_type': 'std_msgs/Float32'
        }
    }
    dict_2 = {
        "camera/front-center": {
            "tags": [
                "camera",
                "front",
                "center",
                "image"
            ],
            "msg_type": "std_msgs/Float32",
            "msg_md5sum": "73fcbf46b49191e672908e50842a83d4",
        }
    }
    assert dict_reg_match(dict_1, dict_2) is True


def test_dict_reg_match_2():
    """Test for dict_reg_match function."""
    from pydtk.utils.utils import dict_reg_match

    dict_1 = {
        '.*annotation': {
            'tags': ['.*']
        }
    }
    dict_2 = {
        "camera/front-center": {
            "tags": [
                "camera",
                "front",
                "center",
                "image"
            ]
        }
    }
    assert dict_reg_match(dict_1, dict_2) is False


if __name__ == '__main__':
    test_dict_reg_match_2()
