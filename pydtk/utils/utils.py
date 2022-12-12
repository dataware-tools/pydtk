#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi, Daiki Hayashi)

import contextlib
import os
import re
import sys

import attrdict
import yaml
from tqdm import tqdm

DTYPE_MAP = {
    "string[]": str,
    "string": str,
    "str": str,
    "text": str,
    "int": int,
    "float": float,
    "double": float,
    "boolean": bool,
    "bool": bool,
    "obj": object,
    "object": object,
    "dict": object,
    "datetime": object,
    "none": None,
}


def load_config(name):
    """Load config from name.

    Args:
        name (str): name of config (e.g. 'v1')

    Returns:
        (AttrDict): config

    """
    with open(
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "conf", name + ".yaml"
        ),
        "r",
    ) as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    return attrdict.AttrDict(config)


def tag_filter(tag_list, base_df):
    """Search with tags.

    Args:
        base_df (DataFrame): Search target.
        tag_list (list): List of search tag.

    Returns:
        (DataFrame): Searched DataFrame.

    """
    result_df = base_df
    for tag in tag_list:
        mask = result_df["tag"].apply(lambda x: tag in x)
        result_df = result_df[mask]
    return result_df


def get_record_id_list(file_df):
    """Get record id list from file list.

    Args:
        file_df (DataFrame): Search target.

    Returns:
        record_id_list(list): List of record id

    """
    return file_df["record_id"].unique()


def search_content(content_df, content: str):
    """Search with content.

    Args:
        content_df (DataFrame): Table including content element.
        content (str): Content name as a query.

    Returns:
        (DataFrame): Contents DataFrame including a query content.

    """
    mask = content_df["content"].apply(lambda x: content == x)
    return content_df[mask]


def take_time_and(file_df):
    """Get time.

    Args:
        file_df (DataFrame): Search target.

    Returns:
        start_timestamp (float)
        end_timestamp (float)

    """
    mask = (file_df["start_timestamp"] != "no info") & (
        file_df["start_timestamp"] != "Nan"
    )
    start_timestamp = file_df[mask]["start_timestamp"].max()
    mask = (file_df["end_timestamp"] != "no info") & (file_df["end_timestamp"] != "Nan")
    end_timestamp = file_df[mask]["end_timestamp"].min()

    return start_timestamp, end_timestamp


def dict_reg_match(a, b):
    """Match two dicts with regex expression.

    Args:
        a (str, list, dict): dict 1 with regex expression
        b (str, list, dict): dict 2

    Returns:
        (bool): True if the two dict matches

    """
    # type check
    if type(a) is not type(b):
        return False

    if isinstance(a, str):
        return re.fullmatch(a, b) is not None

    if isinstance(a, list):
        return any([any([dict_reg_match(a_, b_) for b_ in b]) for a_ in a])

    if isinstance(a, dict):
        return all(
            [
                any(
                    [
                        dict_reg_match(av, bv)
                        for bk, bv in b.items()
                        if re.fullmatch(ak, bk) is not None
                    ]
                )
                for ak, av in a.items()
            ]
        )


def serialize_dict_1d(dict_in):
    """Convert list in dict to str.

    Args:
        dict_in (dict): input dict

    Returns:
        (dict): serialized dict

    """
    assert isinstance(dict_in, dict)
    dict_out = dict_in
    for key, value in dict_out.items():
        if isinstance(value, list):
            dict_out.update({key: ";".join(value)})
    return dict_out


def deserialize_dict_1d(dict_in):
    """Convert str of list to list.

    Args:
        dict_in (dict): input dict

    Returns:
        (dict): deserialized dict

    """
    assert isinstance(dict_in, dict)
    dict_out = dict_in
    for key, value in dict_out.items():
        if isinstance(value, str):
            if ";" in value:
                dict_out.update({key: value.split(";")})
    return dict_out


def dict_to_listed_dict_1d(dict_in):
    """Convert dict to listed dict where each value is a list containing one element.

    Args:
        dict_in (dict): input dict

    Returns:
        (dict): listed dict

    """
    assert isinstance(dict_in, dict)
    dict_out = dict_in
    for key, value in dict_out.items():
        dict_out.update({key: [value]})
    return dict_out


def dicts_to_listed_dict_2d(dicts_in):
    """Convert dicts to listed dict where each value is a list containing multiple elements.

    Args:
        dicts_in (list): list of dicts

    Returns:
        (dict): listed dict

    """
    assert isinstance(dicts_in, list)
    dict_out = dict()
    for item in tqdm(dicts_in, desc="dicts_to_listed_dict_2d", leave=False):
        for key, value in item.items():
            if key not in dict_out.keys():
                dict_out.update({key: [value]})
            else:
                dict_out[key].append(value)
    return dict_out


def listed_dict_to_dict_1d(dict_in):
    """Convert listed dict to dict.

    Args:
        dict_in (dict): input dict (listed dic)

    Returns:
        (dict): dict

    """
    assert isinstance(dict_in, dict)
    dict_out = dict_in
    for key, value in dict_out.items():
        dict_out.update({key: value[0]})
    return dict_out


def dtype_string_to_dtype_object(dtype):
    """Return object corresponds to the input string."""
    try:
        return DTYPE_MAP[dtype]
    except KeyError:
        return object


def _deepmerge_append_list_unique(config, path, base, nxt):
    """A list strategy to append unique elements of nxt."""
    if len(base) == 0:
        return nxt
    if len(nxt) == 0:
        return base
    if isinstance(base[0], dict) or isinstance(nxt[0], dict):
        return [item for item in base] + [item for item in nxt if item not in base]
    if isinstance(base[0], list) or isinstance(nxt[0], list):
        return [item for item in base] + [item for item in nxt if item not in base]
    return list(set(base + nxt))


@contextlib.contextmanager
def smart_open(filename: str = None, mode: str = "r", *args, **kwargs):
    """Open files and i/o streams transparently.

    Reference:
    https://stackoverflow.com/questions/17602878/how-to-handle-both-with-open-and-sys-stdout-nicely

    Args:
        filename (str): file path (if None, stdout is used)
        mode (str): file open option

    Returns:
        (file-pointer)

    """
    if filename is None or filename == "-":
        if "r" in mode:
            stream = sys.stdin
        else:
            stream = sys.stdout
        if "b" in mode:
            fh = stream.buffer
        else:
            fh = stream
        close = False
    else:
        fh = open(filename, mode, *args, **kwargs)
        close = True

    try:
        yield fh
    finally:
        if close:
            try:
                fh.close()
            except AttributeError:
                pass
