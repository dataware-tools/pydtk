#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess
import sys
import time


def _check_bag(file):
    try:
        with rosbag.Bag(file, "r") as bag:
            duration = bag.get_end_time() - bag.get_start_time()
    except UnicodeDecodeError as e:
        logging.error(f"UnicodeDecodeError: {e}")
        return 11

    if duration < 60 * 5:  # 5[min]
        return 12
    return 0


def main(args):
    """Load and dump front camera image."""
    t0 = time.time()
    returncode = _check_bag(args.bag)
    t1 = time.time()
    logging.info("Time (Method 2): {0:.03f}".format(t1 - t0))
    sys.exit(returncode)


def _run_docker(args):
    """Run a docker container and convert bag on the container."""
    image = "hdwlab/pydtk:master"
    pwd = os.getcwd()
    uid = str(os.getuid())
    gid = str(os.getgid())

    bag_vol = "/" + args.bag.split("/")[1]

    command = ["docker", "run", "--rm",
               f"--volume={pwd}:/home/hdl:rw",
               f"--volume={bag_vol}:{bag_vol}:rw",
               f"-u {uid}:{gid}",
               image, '"$@"',
               "/home/hdl/check_bag.py",
               "--bag", args.bag,
            #    "--topic", args.topic,
               ]
    command += ["-v"] if args.verbose else ""

    result = subprocess.run(" ".join(command), shell=True)
    returncode = result.returncode
    sys.exit(returncode)


def get_arguments():
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="Description of the script.")
    parser.add_argument("--docker", action="store_true",
                        help="use docker container.")
    parser.add_argument("--bag", type=str, required=True,
                        help="input bag file.")
    # parser.add_argument("--topic", type=str, required=True,
    #                     help="topic to extract.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Dry run to check number of total processes.")
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_arguments()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not args.docker:
        import rosbag
        from pydtk.io import BaseFileReader
        main(args)
    else:
        _run_docker(args)
