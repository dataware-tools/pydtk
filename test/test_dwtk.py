#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors
# Created by Yusuke Adachi

from dwtk import frontend
from dwtk.utils import utils
import pickle


def _test_search():
    """Run the dwtk.search.tag test."""
    with open("test/small_dflist.pkl", "rb") as f:
        df_dict = pickle.load(f)

    content_df = df_dict["content_df"]
    file_df = df_dict["file_df"]

    # dwtk.search.tag でタグ検索ができる
    tag_list = ["driver", "camera"]
    filtered_df = utils.tag_filter(tag_list, content_df)
    print("Filter with %s" % str(tag_list))
    print(filtered_df)

    # 該当するrecord_idを抽出
    record_id_list = filtered_df["record_id"].unique()
    for record_id in record_id_list:
        print("Hit: %s" % record_id)

    # 特定のrecord_idのファイルリストを表示
    record_id = record_id_list[0]
    record_id_file_df = file_df[file_df["record_id"] == record_id]
    print(record_id_file_df)


def _test_dataware_list():
    """Run the dwtk.search.LoadPKL test."""
    front = frontend.LoadPKL("test/small_dflist.pkl")
    record_id_list = front.get_record_id_info()
    print(record_id_list)


if __name__ == '__main__':
    pass
