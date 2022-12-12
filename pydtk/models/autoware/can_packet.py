#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC

import bitstring

from pydtk.models import register_model
from pydtk.models.rosbag import GenericRosbagModel as _GenericRosbagModel
from pydtk.utils.can_decoder import CANDecoder


@register_model(priority=2)
class AutowareCanMsgsCANPacketRosbagModel(_GenericRosbagModel, ABC):
    """A model for a rosbag file containing autoware_can_msgs/CANPacket."""

    _contents = {".*": {"msg_type": "autoware_can_msgs/CANPacket"}}
    _config = {"path_to_assign_list": None}
    can_decoder = None

    def __init__(self, path_to_assign_list=None, **kwargs):
        super(AutowareCanMsgsCANPacketRosbagModel, self).__init__(**kwargs)
        self._config["path_to_assign_list"] = path_to_assign_list
        if path_to_assign_list is not None:
            self.can_decoder = CANDecoder(path_to_assign_list)

    def configure(self, path_to_assign_list=None, **kwargs):
        """Configure the model.

        Args:
            path_to_assign_list (str): Path to bit-assign list (csv)

        """
        if path_to_assign_list is not None:
            self._config["path_to_assign_list"] = path_to_assign_list
            self.can_decoder = CANDecoder(path_to_assign_list)

    def msg_to_data(self, msg, **kwargs):
        """Convert a message to data."""
        if self.can_decoder is not None:
            can_id = (
                bitstring.ConstBitArray(uint=int(msg.id), length=32)
                .hex.lstrip("0")
                .lower()
            )
            can_data = bitstring.ConstBitArray(bytes=msg.dat).hex

            data = []
            for bit_assign in self.can_decoder.bit_assign_info.signal_list.values():
                if can_id.replace(" ", "") == bit_assign.can_id:
                    data.append(self.can_decoder.unpack_data(can_data, bit_assign))
                else:
                    data.append(None)

            return data
        else:
            return [msg.count, msg.id, msg.len, msg.dat, msg.flag, msg.time]

    @property
    def columns(self):
        """Return columns."""
        if self.can_decoder is not None:
            return [
                bit_assign.data_label
                for bit_assign in self.can_decoder.bit_assign_info.signal_list.values()
            ]
        else:
            return ["count", "id", "len", "dat", "flag", "time"]
