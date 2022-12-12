#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

import pickle

import vaex


def _test_df_to_vaex():
    """Convert pandas dataframe to vaex."""

    def _serialize(element):
        if isinstance(element, list):
            return ":".join(element)
        return element

    with open("test/small_dflist.pkl", "rb") as f:
        df_dict = pickle.load(f)

    # load Pandas DataFrame and Serialize
    content_ = df_dict["content_df"].applymap(_serialize).to_dict("list")
    file_ = df_dict["file_df"].applymap(_serialize).to_dict("list")
    record_id_ = df_dict["record_id_df"].applymap(_serialize).to_dict("list")

    # Create Vaex DataFrame
    content_df = vaex.from_dict(content_)
    file_df = vaex.from_dict(file_)
    record_id_df = vaex.from_dict(record_id_)

    # Export as .arrow
    content_df.export("test/content_df.arrow")
    file_df.export("test/file_df.arrow")
    record_id_df.export("test/record_id_df.arrow")


def _test_load_vaex_df():
    """Load vaex dataframe."""
    import time

    start_time = time.time()
    _ = vaex.open("test/content_df.arrow")  # content_df
    _ = vaex.open("test/file_df.arrow")  # file_df
    record_id_df = vaex.open("test/record_id_df.arrow")  # record_id_df
    print("Load time: {0:.04f}".format(time.time() - start_time))

    df = record_id_df

    start_time = time.time()
    _ = df.filter(df.start_timestamp < 1485304492.0 or df.end_time < 1485304492.0)
    print("Filter time: {0:.04f}".format(time.time() - start_time))


def _test_serve_graphql():
    """Serve."""
    content_df = vaex.open("test/content_df.arrow")

    # Serve
    content_df.graphql.serve()

    # # Avoid shutting down the thread
    # from tornado.ioloop import IOLoop
    # IOLoop.instance().start()


if __name__ == "__main__":
    pass
