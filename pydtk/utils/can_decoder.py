#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""CAN Decoder."""

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import time

import bitstring


class BitAssign(object):
    """A class for storing assignment information of a single bit."""

    def __init__(
        self,
        data_label,
        data_name,
        can_id,
        data_position,
        data_length,
        minimum_renewal_time,
        minimum_renewal_time_comment,
        unit,
        resolution,
        resolution_comment,
        offset_physical,
        offset_cpu,
        signed,
    ):
        """Initialize BitAssign.

        Args:
            data_label (str): Data label
            data_name (str): Data name
            can_id (str): ID of CAN
            data_position (str): The first position of the corresponding bits
            data_length (str): Number of bits corresponding to the data
            minimum_renewal_time (str): Optional
            minimum_renewal_time_comment (str): Optional
            unit (str): Unit of the data
            resolution (str): Resolution of the data
            resolution_comment (str): Optional
            offset_physical (str): Offset to add after converting to a physical value
            offset_cpu (str): Offset to add before converting to a physical value
            signed (str): Whether the data is signed or not

        """
        self.data_label = data_label.replace(" ", "")
        self.data_name = data_name
        self.can_id = can_id.replace(" ", "").lower()
        self.data_position = data_position.replace(" ", "")
        self.data_length = data_length.replace(" ", "")
        self.minimum_renewal_time = minimum_renewal_time.replace(" ", "")
        self.minimum_renewal_time_comment = minimum_renewal_time_comment
        self.unit = unit.replace(" ", "")
        self.resolution = resolution.replace(" ", "")
        self.resolution_comment = resolution_comment
        self.offset_physical = offset_physical.replace(" ", "")
        self.offset_cpu = offset_cpu.replace(" ", "")
        self.signed = signed

        self.reformat()

    def reformat(self):
        """Remove unregular characters from bit assign information."""
        self.data_position = re.sub("[^0-9|.|-]", "", self.data_position)
        self.data_length = re.sub("[^0-9|.|-]", "", self.data_length)
        self.minimum_renewal_time = re.sub("[^0-9|.|-]", "", self.minimum_renewal_time)
        self.resolution = re.sub("[^0-9|.|-]", "", self.resolution)
        self.offset_physical = re.sub("[^0-9|.|-]", "", self.offset_physical)

        if self.resolution == "" or self.resolution == "-":
            self.resolution = "1.0"

        if self.offset_physical == "" or self.offset_physical == "-":
            self.offset_physical = "0"


class BitAssignInfo(object):
    """A class for handling CAN assignment list."""

    def __init__(self, path_to_assign_list=None):
        """Initialize BitAssignInfo."""
        self.logger = logging.getLogger(type(self).__name__)
        self.signal_list = {}
        self.path_to_assign_list = path_to_assign_list
        if path_to_assign_list is not None:
            self.load_from_csv(path_to_assign_list)

    def load_from_csv(self, path_to_assign_list):
        """Load can bit assign from csv.

        Args:
            path_to_assign_list (str): Path to csv

        """
        # Check
        if path_to_assign_list == "":
            self.logger.warning("empty path is given")
            return

        # Check if the file exists
        if not os.path.exists(path_to_assign_list):
            self.logger.warning("file does not exists: {}".format(path_to_assign_list))
            return

        # Prepare
        signal_list = {}

        # Load the file
        with open(path_to_assign_list, "rU") as f:
            reader = csv.reader(f)
            _ = next(reader)  # Header
            for row in reader:
                bit_assign_info = BitAssign(*row)
                signal_list.update({row[0]: bit_assign_info})

        self.signal_list = signal_list
        self.logger.debug(
            "loaded {0} signal assignment from file: {1}".format(
                len(signal_list), path_to_assign_list
            )
        )

        if len(signal_list) == 0:
            self.logger.error(
                "Could not load bit assign list: {}".format(path_to_assign_list)
            )
            sys.exit(1)

    def bit_assigns_from_can_id(self, can_id):
        """Returns list of bit assign information which corresponds to the given CAN ID.

        Args:
            can_id (str): ID of CAN

        """
        # Check
        if len(self.signal_list) == 0:
            if self.path_to_assign_list != "":
                self.load_from_csv(self.path_to_assign_list)
            else:
                self.logger.error("please set the path to can bit assign list")

        # Find corresponding bit assign and return
        bit_assigns = []
        for bit_assign in self.signal_list.values():
            # self.logger.debug("can_id (assign): {0}, can_id (data): {1}"
            #                   .format(bit_assign.can_id, can_id.replace(" ", "")))
            if can_id.replace(" ", "") == bit_assign.can_id:
                bit_assigns.append(bit_assign)
        return bit_assigns


class CANData(object):
    """A class for handling the decoded CAN signals."""

    def __init__(self, time_in_nsec, data_label, data):
        """Initialize CanData.

        Args:
            time_in_nsec (str): Time in nano-seconds
            data_label (str): Data label
            data (str): Data

        """
        self.time_in_nsec = time_in_nsec
        self.time_in_datetime = datetime.datetime.fromtimestamp(
            float(time_in_nsec) / 1.0e9
        )
        self.data_label = data_label
        self.data = data


class CANDecoder(object):
    """CAN csv file decoder."""

    def __init__(self, path_to_assign_list=None):
        """Initialize CANDecoder.

        Args:
            path_to_assign_list (str): Path to bit-assign info file (csv)

        """
        self.logger = logging.getLogger(type(self).__name__)
        self.path_to_assign_list = path_to_assign_list
        self.path_to_csv = ""
        self.bit_assign_info = BitAssignInfo(path_to_assign_list)
        self.data = []
        self.f = None
        self.reader = None

    def load_bit_assign_list(self, path_to_assign_list):
        """Load bit-assign list from a file.

        Args:
            path_to_assign_list (str): Path to the file

        """
        self.path_to_assign_list = path_to_assign_list
        self.bit_assign_info = BitAssignInfo(path_to_assign_list)

    def open_csv(self, path_to_csv=""):
        """Open a csv file and prepare file-pointer.

        Args:
            path_to_csv (str): Path to the csv file containing CAN data

        """
        if path_to_csv == "":
            path_to_csv = self.path_to_csv
        else:
            self.path_to_csv = path_to_csv

        # Check file existence
        if not os.path.exists(path_to_csv):
            self.logger.warning("file does not exists: {}".format(path_to_csv))
            sys.exit(1)

        f = open(path_to_csv, "rU")
        reader = csv.reader(f)
        _ = next(reader)  # Skip header
        self.f = f
        self.reader = reader

    def close_csv(self):
        """Close the file."""
        self.f.close()
        self.reader = None

    def analyze_line(self):
        """Returns list of CANData in current line.

        Returns:
            (list): A list of CANData

        """
        # check
        if self.f is None:
            self.logger.error("file is not opened yet")
            return None

        if self.f.closed:
            self.logger.error("file is already closed")
            return None

        # Prepare
        data = []

        # Get line
        row = next(self.reader)
        if not row:
            raise EOFError

        # Analyze
        for bit_assign in self.bit_assign_info.bit_assigns_from_can_id(row[1]):
            data.append(
                CANData(
                    row[0], bit_assign.data_label, self.unpack_data(row[3], bit_assign)
                )
            )

        return data

    def analyze_csv(self, path_to_csv="", start_time=None, end_time=None):
        """Returns list of CANData.

        Args:
            path_to_csv (str): Path to a csv file containing CAN data
            start_time (datetime): Start-time to extract data
            end_time (datetime): End-time to extract data

        Returns:
            (list): A list of CANData

        """
        if path_to_csv == "":
            path_to_csv = self.path_to_csv
        else:
            self.path_to_csv = path_to_csv

        # check file existence
        if not os.path.exists(path_to_csv):
            self.logger.warning("file does not exists: {}".format(path_to_csv))
            return None

        # prepare
        data = []
        line_index = 1
        line_datetime = None

        # start analyzing
        self.logger.debug("started loading csv file")
        with open(path_to_csv, "rU") as f:
            reader = csv.reader(f)
            _ = next(reader)  # Header

            for row in reader:
                timestamp = row[2]
                can_id = (
                    bitstring.ConstBitArray(uint=int(row[5]), length=32)
                    .hex.lstrip("0")
                    .lower()
                )
                _ = int(row[6])  # can_length
                can_data = "".join(
                    [
                        bitstring.ConstBitArray(uint=int(b), length=8).hex
                        for b in row[7:15]
                    ]
                )
                for bit_assign in self.bit_assign_info.bit_assigns_from_can_id(can_id):
                    data.append(
                        CANData(
                            timestamp,
                            bit_assign.data_label,
                            self.unpack_data(can_data, bit_assign),
                        )
                    )
                if line_index % 500000 == 0:
                    self.logger.debug("{} lines decoded".format(line_index))
                line_index += 1

        # check
        if line_datetime is not None and line_datetime < end_time:
            self.logger.error(
                "This csv file ends {0} before end_time {1}".format(
                    line_datetime, end_time
                )
            )
            return []

        # return
        return data

    @staticmethod
    def unpack_data(data_string, bit_assign):
        """Decodes data_string to signal value.

        Args:
            data_string (str): Data string
            bit_assign (BitAssign): A BitAssign object

        Returns:
            (str): Decoded value

        """
        c = bitstring.ConstBitArray(hex=data_string.replace(" ", ""))
        if "." in bit_assign.data_position:
            data_position_1 = int(bit_assign.data_position.split(".")[0])
            data_position_2 = int(bit_assign.data_position.split(".")[1])
        else:
            data_position_1 = int(bit_assign.data_position)
            data_position_2 = 0
        start_position = data_position_1 * 8 - data_position_2
        data_len = int(bit_assign.data_length)  # bits

        data = bitstring.ConstBitArray(
            bin=c.bin[(start_position - data_len) : start_position]
        )

        if bit_assign.signed == "1":
            base_value = data.int
        else:
            base_value = data.uint

        value = base_value * float(bit_assign.resolution) + float(
            bit_assign.offset_physical
        )

        return value


class CANDeserializer(object):
    """Deserialize CAN data."""

    def __init__(self, signal_list=None):
        """Initialize CANDeserializer.

        Args:
            signal_list (list): A list of signals (str) to deserialize

        """
        self.logger = logging.getLogger(type(self).__name__)
        self.signal_list = (
            ["brake_pos", "turn_signal"] if signal_list is None else signal_list
        )

    def deserialize(self, can_data, start_time=None, end_time=None, fps=5):
        """Deserializes given CAN data from start_time to end_time with given fps.

        Args:
            can_data (list): A list of CANData objects
            start_time (datetime): Start-time to deserialize data
            end_time (datetime): End-time to deserialize data
            fps (int): Fps to deserialize

        Returns:
            (dict): Deserialized data

        """
        # check
        if len(can_data) == 0:
            self.logger.error("empty can data is given")
            return None

        if start_time is None:
            start_time = can_data[0].time_in_datetime
            self.logger.debug("setting start_time as: {}".format(start_time))

        if end_time is None:
            end_time = can_data[-1].time_in_datetime
            self.logger.debug("setting end_time as: {}".format(end_time))

        # prepare
        current_time_start = start_time
        current_time_end = start_time + datetime.timedelta(
            seconds=(float(1.0) / float(fps))
        )
        current_index = 0
        # end_of_data_flag = False

        # prepare tables
        previous = {signal: None for signal in self.signal_list}
        result = {signal: [] for signal in self.signal_list}
        result["time_in_datetime"] = []

        # loop
        self.logger.debug("started can data deserialization")
        while current_index < len(can_data):

            # initialize signal_temp dictionary
            signal_temp = {signal: [] for signal in self.signal_list}
            signal_temp_previous = {signal: [] for signal in self.signal_list}

            # store can signals
            for can_signal in can_data[current_index:]:
                if current_time_start <= can_signal.time_in_datetime < current_time_end:
                    if can_signal.data_label in self.signal_list:
                        signal_temp[can_signal.data_label].append(can_signal.data)
                    current_index += 1
                else:
                    if can_signal.time_in_datetime < current_time_start:
                        if can_signal.data_label in self.signal_list:
                            signal_temp_previous[can_signal.data_label].append(
                                can_signal.data
                            )
                        current_index += 1
                    elif can_signal.time_in_datetime > current_time_end:
                        break

            # calculate average and store it
            for signal in self.signal_list:
                # store previous signals if exists
                if len(signal_temp_previous[signal]) != 0:
                    previous[signal] = sum(signal_temp_previous[signal]) / float(
                        len(signal_temp_previous[signal])
                    )

                # store current signals
                if len(signal_temp[signal]) == 0:
                    if len(result[signal]) == 0:
                        if previous[signal] is None:
                            result[signal].append(None)
                        else:
                            result[signal].append(previous[signal])
                    else:
                        result[signal].append(result[signal][-1])
                else:
                    result[signal].append(
                        sum(signal_temp[signal]) / float(len(signal_temp[signal]))
                    )

            # store time
            result["time_in_datetime"].append(current_time_start)

            # break
            if (
                current_index >= len(can_data)
                or can_data[current_index].time_in_datetime > end_time
            ):
                self.logger.debug("escaping")
                break

            # next step
            current_time_start = current_time_end
            current_time_end = current_time_end + datetime.timedelta(
                seconds=(float(1.0) / float(fps))
            )

        # return
        return result


def main(args):
    """Main function.

    Args:
        args (ArgumentParser): arguments

    """
    # Logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    can_decoder = CANDecoder(args.assign_list)
    can_data = can_decoder.analyze_csv(args.path_to_csv)

    if args.verbose:
        for data in can_data:
            print(
                "{0}, {1}, {2}".format(
                    data.unix_time_in_millisecond, data.data_label, data.data
                )
            )

    can_deserializer = CANDeserializer()
    signal_list = can_deserializer.signal_list
    can_data_deserialized = can_deserializer.deserialize(can_data, fps=10)

    # print
    print(
        "{0:>20},{1}".format(
            "Timestamp", ",".join(["{0:>16}".format(k) for k in signal_list])
        )
    )
    for idx, stamp in enumerate(can_data_deserialized["time_in_datetime"]):
        print(
            "{0:>20},{1}".format(
                str(int(time.mktime(stamp.timetuple()))) + stamp.strftime("%f000"),
                ",".join(
                    [
                        "{0:>16,.03f}".format(can_data_deserialized[k][idx])
                        for k in signal_list
                    ]
                ),
            )
        )


if __name__ == "__main__":
    # parser
    parser = argparse.ArgumentParser(
        description="Decodes a CAN signal csv file according to a given bit assign list"
    )
    parser.add_argument("path_to_csv")
    parser.add_argument(
        "-a",
        "--assign-list",
        type=str,
        default="./bit_assign_list.csv",
        help="Path to bit assing list (csv file)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    args = parser.parse_args()

    main(args)
