#!/usr/bin/env python3

# Copyright Toolkit Authors (Yusuke Adachi)

import argparse
import logging
import os
import pickle
import shutil


def change_path_df(in_pkl, out_pkl):
    """Change absolute path to relative path.

    Args:
        in_pkl (str): Input picke file.
        out_pkl (str): Output pickle file.

    """
    in_pkl_path = os.path.abspath(in_pkl)
    base_dir_path = os.path.dirname(in_pkl_path)
    logging.info("Loading %s" % in_pkl)
    with open(in_pkl, "rb") as f:
        df_dict = pickle.load(f)

    for key in df_dict:
        try:
            df_dict[key]["path"] = df_dict[key]["path"].str.replace(base_dir_path, ".")
        except KeyError:
            logging.info("Skipped. %s has no path info." % key)

    if os.path.isfile(out_pkl):
        bak_pkl = out_pkl + ".bak"
        logging.info("Backup %s to %s" % (out_pkl, bak_pkl))
        shutil.move(out_pkl, bak_pkl)
    with open(out_pkl, "wb") as f:
        pickle.dump(df_dict, f, protocol=2)
    logging.info("Save to %s" % out_pkl)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="This script change absolute path \
                                                  to relative path."
    )
    parser.add_argument("--in-pkl", help="Pickle files to be changed.")
    parser.add_argument("--out-pkl", help="Output pickle file.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")
    args = parser.parse_args()
    pkl_list, out_pkl = args.in_pkl, args.out_pkl

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    change_path_df(pkl_list, out_pkl)
