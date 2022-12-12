#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)

import argparse
import os
import pickle
import shutil

import pandas as pd


def concat_df(pkl_files, out_pkl):
    """Concatinate some pickle files.

    Args:
        pkl_files (list): List of picke files to concatinate.
        out_pkl (str): Output pickle file.

    """
    for i, pkl in enumerate(pkl_files):
        with open(pkl, "rb") as f:
            df_dict = pickle.load(f)
            if i == 0:
                file_df = df_dict["file_df"]
                content_df = df_dict["content_df"]
                record_id_df = df_dict["record_id_df"]
            else:
                file_df = pd.concat([file_df, df_dict["file_df"]])
                content_df = pd.concat([content_df, df_dict["content_df"]])
                record_id_df = pd.concat([record_id_df, df_dict["record_id_df"]])

    if os.path.isfile(out_pkl):
        shutil.move(out_pkl, out_pkl + ".bak")
    with open(out_pkl, "wb") as f:
        pickle.dump(
            {
                "file_df": file_df,
                "content_df": content_df,
                "record_id_df": record_id_df,
            },
            f,
            protocol=2,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script is bababa")
    parser.add_argument("in_pkls", help="Pickle files to concatinate.", nargs="*")
    parser.add_argument("--out_pkl", help="Output pickle file.")
    args = parser.parse_args()
    pkl_list, out_pkl = args.in_pkls, args.out_pkl

    concat_df(pkl_list, out_pkl)
