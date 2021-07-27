#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

from abc import ABC
import cv2
import numpy as np
import os

from pydtk.models import BaseModel, register_model, MetaDataModel
from pydtk.models.csv import CameraTimestampCsvModel


@register_model(priority=1)
class GenericMovieModel(BaseModel, ABC):
    """A generic model for a movie file."""

    _content_type = 'video/mp4'
    _data_type = None   # allow any data-type
    _file_extensions = ['.mp4']
    _contents = None

    def __init__(self, **kwargs):
        super(GenericMovieModel, self).__init__(**kwargs)

    def _load(self, path, start_timestamp=None, end_timestamp=None, **kwargs):
        """Load a movie file.

        Args:
            path (str): path to a movie file
            start_timestamp (float): timestamp to start loading
            end_timestamp (float): timestamp to end loading

        """
        self.path = path

        # Check existence of the file
        if not os.path.isfile(path):
            raise IOError('No such file: {}'.format(path))

        frames = []
        timestamps = []

        cap = cv2.VideoCapture(path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = None
        height = None
        n_channels = None

        while True:
            # Reed the first frame
            ret, frame = cap.read()
            if ret:
                height, width, n_channels = frame.shape
            else:
                break

            # Get timestamp
            frame_current = cap.get(cv2.CAP_PROP_POS_FRAMES)
            sec_current = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

            # Fix sec_current
            if frame_current > 0 and sec_current == 0.0:
                sec_current = frame_current / fps

            # Filter by timestamp
            if start_timestamp is not None and sec_current < start_timestamp:
                continue
            if end_timestamp is not None and sec_current > end_timestamp:
                break

            # Store frames and timestamps
            frames.append(frame)
            timestamps.append(sec_current)

        # Close stream
        cap.release()

        self.data = {
            'timestamps': timestamps,
            'data': frames,
            'fps': fps,
            'width': width,
            'height': height,
            'n_channels': n_channels
        }

    def _save(self, path, **kwargs):
        """Save ndarray data to a mp4 file.

        Args:
            path (str): Path to the output file

        """
        fps = self.data['fps'] if 'fps' in self.data.keys() else 30
        if 'fps' in kwargs.keys():
            fps = kwargs['fps']

        width = self.data['width'] if 'width' in self.data.keys() else 224
        if 'width' in kwargs.keys():
            width = kwargs['width']

        height = self.data['height'] if 'height' in self.data.keys() else 224
        if 'height' in kwargs.keys():
            height = kwargs['height']

        frames = self.data['data']
        fmt = cv2.VideoWriter_fourcc(*'mp4v')
        size = (width, height)
        writer = cv2.VideoWriter(path, fmt, fps, size)

        for frame in frames:
            assert frame.shape[0] == height, \
                'Resolution mismatched! (height {} != {})'.format(height, frame.shape[0])
            assert frame.shape[1] == width, \
                'Resolution mismatched! (width {} != {})'.format(width, frame.shape[1])
            writer.write(frame)

        writer.release()

    def to_ndarray(self):
        """Return data as ndarray."""
        return np.array(self.data['data'])

    @property
    def timestamps(self):
        """Return timestamps as ndarray."""
        return np.array(self.data['timestamps'])

    def load_metadata(self, path):
        """Load and return metadata."""
        metadata = None
        for ext in MetaDataModel._file_extensions:
            metadata_filepath = path + ext
            if os.path.isfile(metadata_filepath):
                metadata = MetaDataModel()
                metadata.load(metadata_filepath)
        if metadata is None:
            raise IOError('No metadata found for file: {}'.format(path))
        return metadata

    def downsample_frames(self, timestamps, target_frame_rate=2.0):
        """Downsample data into the target sampling rate.

        Args:
            timestamps (ndarray): timestamps [sec]

        Returns:
            downsampled_timestamps (ndarray): timestamps [sec]

        """
        downsampled_timestamps = timestamps
        fps_previous_index = 0
        for i, timestamp in enumerate(timestamps):
            fps_current_index = timestamp // (1.0 / float(target_frame_rate))
            if fps_current_index == fps_previous_index:
                downsampled_timestamps[i] = -1
                continue
            else:
                fps_previous_index = fps_current_index

        return downsampled_timestamps

    @classmethod
    def generate_contents_meta(cls, path, content_key='content'):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (dict): contents metadata

        """
        # Load file
        cap = cv2.VideoCapture(path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        n_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Load the first frame and get information
        ret, frame = cap.read()
        frame_info = {}
        if ret:
            height, width, n_channels = frame.shape
            frame_info.update({
                'height': height,
                'width': width,
                'n_channels': n_channels
            })

        # Get duration
        cap.set(cv2.CAP_PROP_POS_FRAMES, n_frames)
        duration = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

        # Close the file
        cap.release()

        # Generate metadata
        contents = {
            content_key: {
                'tags': ['video'],
                'fps': fps,
                'n_frames': n_frames,
                'duration': duration,
                **frame_info
            }
        }

        return contents

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        # Load file
        cap = cv2.VideoCapture(path)
        n_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(cv2.CAP_PROP_POS_FRAMES, n_frames)
        duration = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

        # Close the file
        cap.release()

        return 0, duration


@register_model(priority=2)
class GenericMovieWithCameraTimestampCsvModel(GenericMovieModel, ABC):
    """A generic model for a movie file with a camera-timestamp-csv."""

    _content_type = 'video/mp4'
    _data_type = None   # allow any data-type
    _file_extensions = ['.mp4']
    _contents = {'camera/.*': {'tags': ['.*']}}

    def __init__(self, **kwargs):
        super(GenericMovieWithCameraTimestampCsvModel, self).__init__(**kwargs)

    @classmethod
    def _is_loadable(cls, path='', **kwargs):
        """Check data by file format."""
        timestamps_path = path.replace('.mp4', '_timestamps.csv')
        if not os.path.isfile(timestamps_path):
            return False
        return True

    def _load(self, path, start_timestamp=None, end_timestamp=None,
              target_frame_rate=2.0, resize_rate=0.5, raw=False, **kwargs):
        """Load a movie file.

        Args:
            path (str): path to a movie file
            start_timestamp (float): timestamp to start loading
            end_timestamp (float): timestamp to end loading
            target_frame_rate (float): frame rate for downsampling
            resize_rate (float): scale of image resize
            raw (bool): raw image or resized image

        """
        self.path = path

        timestamps_path = path.replace('.mp4', '_timestamps.csv')
        timestamps_metadata = self.load_metadata(timestamps_path)
        timestamps_reader = CameraTimestampCsvModel(metadata=timestamps_metadata)
        timestamps_reader._load(timestamps_path)
        timestamps = timestamps_reader.timestamps

        cap = cv2.VideoCapture(path)
        frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if not raw:
            timestamps = self.downsample_frames(timestamps, target_frame_rate=target_frame_rate)
        assert frames == len(timestamps)

        frame_ids = np.arange(0, frames)
        frame_ids = frame_ids[np.logical_and(timestamps >= start_timestamp, timestamps < end_timestamp)]
        timestamps = timestamps[frame_ids]

        data = []
        for frame_id in frame_ids:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_id)
            ret, frame = cap.read()
            if ret:
                if not raw:
                    frame = cv2.resize(frame, dsize=None, fx=resize_rate,
                                       fy=resize_rate, interpolation=cv2.INTER_LINEAR)
                frame = frame[:, :, ::-1]  # Convert BGR to RGB
                frame = frame.transpose((2, 0, 1))  # Reshape: [H, W, C] -> [C, H, W]
                data.append(frame)

        self.data = {'timestamps': timestamps, 'data': data}

    def _save(self, dir_path, contents=None, **kwargs):
        """Save ndarray data to a csv file.

        Args:
            dir_path (str): path to the output directory

        """
        os.makedirs(dir_path, exist_ok=True)
        base_filename = os.path.basename(self.path)
        timestamps, data = self.timestamps, self.to_ndarray()
        for timestamp, frame in zip(timestamps, data):
            # filename = base_filename + "_" + str(timestamp) + ".jpg"
            filename = "{}_{:.3f}.{}".format(base_filename, timestamp, "jpg")
            output_image = os.path.join(dir_path, filename)
            cv2.imwrite(output_image, frame)

    @classmethod
    def generate_contents_meta(cls, path, content_key='content'):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (list): contents metadata

        """
        raise NotImplementedError

    @classmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        raise NotImplementedError
