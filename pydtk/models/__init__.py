#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""pydtk modules."""

import importlib
import json
import logging
import os
import pprint
import re
from abc import ABCMeta, abstractmethod

import six

from pydtk.utils.utils import dict_reg_match

MODELS_BY_PRIORITY = {}  # key: priority, value: model class

logger = logging.getLogger(__name__)


def register_models():
    """Register models."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__))):
        if filename == "__init__.py":
            continue

        try:
            importlib.import_module(
                os.path.join("pydtk.models", os.path.splitext(filename)[0]).replace(
                    os.sep, "."
                )
            )
        except (ModuleNotFoundError, ImportError):
            logger.warning("Failed to load models in {}".format(filename))


def register_model(priority=0):
    """Regist a model."""

    def decorator(cls):
        if priority not in MODELS_BY_PRIORITY.keys():
            MODELS_BY_PRIORITY.update({priority: []})
        MODELS_BY_PRIORITY[priority].append(cls)
        return cls

    return decorator


class UnsupportedFileError(BaseException):
    """Error for unsupported file."""

    pass


class MetaDataModel(object):
    """A model for a metadata file."""

    _file_extensions = [".json"]
    _data = dict()

    _key_map = {
        "content_type": "content-type",
        "data_type": "type",
    }
    _key_map_inv = {v: k for k, v in _key_map.items()}

    def __init__(self, data=None, **kwargs):
        super(MetaDataModel, self).__init__(**kwargs)
        if data is not None:
            if isinstance(data, MetaDataModel):
                self.data = data.data
            elif isinstance(data, dict):
                self.data = data
            else:
                raise TypeError(
                    "Unsupported type of data: {}".format(type(data).__name__)
                )

    @classmethod
    def is_loadable(cls, path="", **kwargs):
        """Check file format."""
        _, ext = os.path.splitext(path)
        if ext not in cls._file_extensions:
            return False
        return True

    def load(self, path, **kwargs):
        """Load json format metadata."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.data = data

    def save(self, path):
        """Save metadata as json."""
        _data = {}
        for key, value in self.data.items():
            if key not in self._key_map.keys():
                _data.update({key: value})
            else:
                _data.update({self._key_map[key]: value})

        with open(path, "w") as f:
            json.dump(_data, f)

    @property
    def data(self):
        """Return data."""
        return self._data

    @data.setter
    def data(self, data):
        """Setter for self._data."""
        _data = {}
        for key, value in data.items():
            if key not in self._key_map_inv.keys():
                _data.update({key: value})
            else:
                _data.update({self._key_map_inv[key]: value})
        self._data = _data

    def to_dict(self):
        """Returns the model properties as a dict."""
        result = {}

        for attr, _ in six.iteritems(self.__dict__):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(
                    map(lambda x: x.to_dict() if hasattr(x, "to_dict") else x, value)
                )
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(
                    map(
                        lambda item: (item[0], item[1].to_dict())
                        if hasattr(item[1], "to_dict")
                        else item,
                        value.items(),
                    )
                )
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Returns the string representation of the model."""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`."""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal."""
        if not isinstance(other, MetaDataModel):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal."""
        return not self == other


class BaseModel(metaclass=ABCMeta):
    """Base model.

    Priority for being selected as a reader of a file.
    The higher this value is, the more likely this model will be chosen as a file loader.
    """

    _priority = 0

    _content_type = None  # e.g. 'text/csv', 'text/.*'
    _data_type = None  # e.g. 'raw_data', '.*'
    _file_extensions = []  # e.g. ['mp4', 'avi']
    _contents = None  # Supported contents (e.g. {'camera/.*': {'tags': ['.*']}})
    _data = None  # for storing data
    _config = None  # for model configurations
    _metadata = None  # e.g. MetaDataModel()
    _columns = None  # Name of each columns in the ndarray returned by `to_ndarray`

    @abstractmethod
    def __init__(self, metadata=None, data=None, **kwargs):
        self.logger = logging.getLogger(name=type(self).__name__)
        if metadata is not None:
            self.metadata = metadata
        if data is not None:
            self.data = data

    def configure(self, **kwargs):
        """Configure this model.

        Args:
            **kwargs: settings (key: key in self._config, value: value of self._config[key])

        """
        for key, value in kwargs.items():
            if key in self._config.keys():
                self._config[key] = value
            else:
                raise KeyError("Unknown key: {}".format(key))

    @classmethod
    def _is_loadable(
        cls, path="", contents=None, content_type=None, data_type=None, **kwargs
    ):
        """Check data by file format."""
        return True

    @classmethod
    def is_loadable(
        cls, path="", contents=None, content_type=None, data_type=None, **kwargs
    ):
        """Check if the given file is loadable.

        Args:
            path (str): path to the target file
            contents (dict or str): content to load (e.g. {'camera/front': 'tags': {...}})
            content_type (str): content-type (e.g. 'text/csv')
            data_type (str): data-type (e.g. 'raw_data')

        """
        # check by file extension
        _, ext = os.path.splitext(path)
        if None not in cls._file_extensions and ext not in cls._file_extensions:
            return False

        # check by content-type
        if cls._content_type is not None:
            if content_type is None:
                if not re.fullmatch(cls._content_type, ""):
                    return False
            else:
                if not re.fullmatch(cls._content_type, content_type):
                    return False

        # check by data-type
        if cls._data_type is not None:
            if data_type is None:
                if not re.fullmatch(cls._data_type, ""):
                    return False
            else:
                if not re.fullmatch(cls._data_type, data_type):
                    return False

        # check by contents
        if cls._contents is not None:
            if contents is None:
                if isinstance(cls._contents, str):
                    if re.fullmatch(cls._contents, "") is None:
                        return False
                if isinstance(cls._contents, list):
                    if (
                        any(
                            [re.fullmatch(_contents, "") for _contents in cls._contents]
                        )
                        is False
                    ):
                        return False
                if isinstance(cls._contents, dict):
                    if re.fullmatch(next(iter(cls._contents)), "") is None:
                        return False
            else:
                if isinstance(contents, dict) and len(contents.keys()) > 1:
                    logging.warning("Loading multiple contents is not supported")
                    return False
                if isinstance(contents, list) and len(contents) > 1:
                    logging.warning("Loading multiple contents is not supported")
                    return False
                if isinstance(contents, list) and len(contents) == 1:
                    contents = contents[0]
                if isinstance(cls._contents, str) and isinstance(contents, str):
                    if re.fullmatch(cls._contents, contents) is None:
                        return False
                if isinstance(cls._contents, str) and isinstance(contents, dict):
                    if re.fullmatch(cls._contents, next(iter(contents))) is None:
                        return False
                if isinstance(cls._contents, list) and isinstance(contents, str):
                    if (
                        any(
                            [
                                re.fullmatch(_contents, contents)
                                for _contents in cls._contents
                            ]
                        )
                        is False
                    ):
                        return False
                if isinstance(cls._contents, list) and isinstance(contents, dict):
                    if (
                        any(
                            [
                                re.fullmatch(_contents, next(iter(contents)))
                                for _contents in cls._contents
                            ]
                        )
                        is False
                    ):
                        return False
                if isinstance(cls._contents, dict) and isinstance(contents, str):
                    if re.fullmatch(next(iter(cls._contents)), contents) is None:
                        return False
                if isinstance(cls._contents, dict) and isinstance(contents, dict):
                    if dict_reg_match(cls._contents, contents) is False:
                        return False

        # check data by file format
        if not cls._is_loadable(
            path=path,
            contents=contents,
            content_type=content_type,
            data_type=data_type,
            **kwargs
        ):
            return False

        # in case all check passed
        return True

    @property
    def data(self):
        """Gets the data of this class object.

        Returns:
            (object): any object

        """
        return self._data

    @data.setter
    def data(self, data):
        """Sets the data to this class object.

        Args:
            data (object): any object

        """
        self._data = data

    @property
    def metadata(self):
        """Gets the metadata of this class object.

        Returns:
            (object): a MetaDataModel object

        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata):
        """Sets the metadata to this class object.

        Args:
            metadata (any): dict or a MetaDataModel object

        """
        assert any([isinstance(metadata, dict), isinstance(metadata, MetaDataModel)])
        if isinstance(metadata, dict):
            metadata = MetaDataModel(data=metadata)
        self._metadata = metadata

    @abstractmethod
    def _load(
        self, path, contents=None, start_timestamp=None, end_timestamp=None, **kwargs
    ):
        """Load data from a file.

        Args:
            path (str): path to the input file
            contents (dict or str): content to load
            start_timestamp (float): timestamp to start loading in sec.
            end_timestamp (float): timestamp to end loading in sec.

        """
        raise NotImplementedError

    def load(self, path=None, as_generator=False, **kwargs):
        """Load data from a file.

        Args:
            path (str): file-path to load (if None, one in the metadata will be used)
            content (str): content to load
            start_timestamp (float): timestamp to start loading in sec. (optional)
            end_timestamp (float): timestamp to end loading in sec. (optional)
            as_generator (bool): load data as a generator.

        """
        if path is None:
            if self.metadata is None:
                raise ValueError("filepath is not provided")
            else:
                path = self.metadata.data["path"]

        # prepare metadata for loading
        metadata = self.metadata if self.metadata is not None else MetaDataModel()
        metadata.data.update({"path": path})

        # add extra information
        metadata.data.update(kwargs)

        # update metadata
        self.metadata = metadata

        # check if this model can load the data
        if not self.is_loadable(**metadata.data):
            raise UnsupportedFileError(
                'Model "{0}" does not support loading file: {1}'.format(
                    type(self).__name__, path
                )
            )

        if as_generator:

            def load_as_generator():
                yield from self._load_as_generator(**metadata.data)

            return load_as_generator()
        else:
            self._load(**metadata.data)

    @abstractmethod
    def _save(self, path, **kwargs):
        """Save data to a file.

        Args:
            path (str): path to the output file

        """
        raise NotImplementedError

    def save(self, path=None, **kwargs):
        """Save data to a file."""
        if path is None:
            if self.metadata is None:
                raise ValueError("filepath is not provided")
            else:
                path = self.metadata.data["path"]

        # check
        _, ext = os.path.splitext(path)
        if ext.lower() not in self._file_extensions:
            raise ValueError(
                "File extension must be one of: {}".format(self._file_extensions)
            )

        # save
        self._save(path=path, **kwargs)

    @property
    def columns(self):
        """Return the name of each column in ndarray returned from `to_ndarray`."""
        postfixes = None
        content = "unknown"

        # Get post-fixes
        if self._columns is not None:
            postfixes = self._columns

        # Get content
        try:
            contents = self.metadata.data["contents"]
            if isinstance(contents, str):
                content = contents
            elif isinstance(contents, list):
                if len(contents) == 1:
                    content = contents[0]
                else:
                    raise ValueError("Loading multiple contents is not supported")
            elif isinstance(contents, dict):
                if len(contents.keys()) == 1:
                    content = list(contents.keys())[0]
                else:
                    raise ValueError("Loading multiple contents is not supported")
        except KeyError:
            raise

        # Create column names
        if postfixes is None:
            return [content]
        elif isinstance(postfixes, str):
            return [content + "/" + postfixes]
        elif isinstance(postfixes, list):
            return [content + "/" + postfix for postfix in postfixes]
        else:
            raise TypeError("Unable to handle variable `postfixes`.")

    def to_ndarray(self):
        """Return data as a ndarray.

        Returns:
            (ndarray): data

        """
        raise NotImplementedError

    @property
    def timestamps(self):
        """Return timestamps.

        Returns:
            (ndarray): timestamps

        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def generate_contents_meta(cls, path, content_key="content"):
        """Generate contents metadata.

        Args:
            path (str): File path
            content_key (str): Key of content

        Returns:
            (list): contents metadata

        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def generate_timestamp_meta(cls, path):
        """Generate contents metadata.

        Args:
            path (str): File path

        Returns:
            (list): [start_timestamp, end_timestamp]

        """
        raise NotImplementedError

    def to_dict(self):
        """Returns the model properties as a dict."""
        result = {}

        for attr, _ in six.iteritems(self.__dict__):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(
                    map(lambda x: x.to_dict() if hasattr(x, "to_dict") else x, value)
                )
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(
                    map(
                        lambda item: (item[0], item[1].to_dict())
                        if hasattr(item[1], "to_dict")
                        else item,
                        value.items(),
                    )
                )
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Returns the string representation of the model."""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`."""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal."""
        if not isinstance(other, BaseModel):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal."""
        return not self == other

    def downsample_timestamps(self, timestamps, target_frame_rate=2.0):
        """Downsample timestamps into the target sampling rate.

        Args:
            timestamps (list): timestamps [sec]
            target_frame_rate (float): target frame rate [Hz]

        Returns:
            downsampled_timestamps (list): timestamps [sec]

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
        downsampled_timestamps = [
            timestamp for timestamp in downsampled_timestamps if timestamp != -1
        ]

        return downsampled_timestamps


register_models()
