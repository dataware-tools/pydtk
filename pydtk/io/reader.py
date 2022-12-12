#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""pydtk modules."""

import os
from abc import ABCMeta

import numpy as np

from pydtk.io.errors import NoModelMatchedError
from pydtk.models import MODELS_BY_PRIORITY, MetaDataModel
from pydtk.preprocesses import PassThrough


class BaseFileReader(metaclass=ABCMeta):
    """Base file reader."""

    _model = None  # model used for loading a file

    def __init__(self, **kwargs):
        self.preprocesses = [PassThrough()]

    @classmethod
    def _select_model(cls, file_metadata):
        """Select a proper model based on the given file-metadata.

        Args:
            file_metadata (object): an MetaDataModel object

        """
        priorities = MODELS_BY_PRIORITY.keys()
        for priority in sorted(priorities, reverse=True):
            for model in MODELS_BY_PRIORITY[priority]:
                if model.is_loadable(**file_metadata.data):
                    return model
        raise NoModelMatchedError(
            "No suitable model found for loading data: {}".format(file_metadata)
        )

    @property
    def model(self):
        """Return model."""
        return self._model

    @model.setter
    def model(self, model):
        self._model = model

    def add_preprocess(self, preprocess):
        """Add preprocessing function."""
        self.preprocesses += [preprocess]

    def read(
        self,
        metadata=None,
        as_generator=False,
        model_kwargs=None,
        as_ndarray=True,
        **kwargs
    ):
        """Read a file which corresponds to the given metadata.

        Args:
            metadata (MetaDataModel or dict): metadata of the data to load
            as_generator (bool): load data as a generator.
            model_kwargs (dict): kwargs to pass to the selected model

        Kwargs:
            path (str): path to a file
            contents (str or dict): content to load
            start_timestamp (float): start-timestamp
            end_timestamp (float): end-timestamp

        Returns:
            (object): an object of the corresponding model

        """
        if model_kwargs is None:
            model_kwargs = {}
        if metadata is None:
            if "path" not in kwargs.keys():
                raise ValueError("Either metadata or path must be specified")
            # Look for the corresponding metadata file
            for ext in MetaDataModel._file_extensions:
                metadata_filepath = kwargs["path"] + ext
                if os.path.isfile(metadata_filepath):
                    metadata = MetaDataModel()
                    metadata.load(metadata_filepath)
            if metadata is None:
                raise IOError("Could not find metadata file")
        else:
            metadata = MetaDataModel(metadata)

        # Replace 'contents' in metadata to specify which content to load
        contents = (
            metadata.data["contents"] if "contents" in metadata.data.keys() else None
        )
        if "contents" in kwargs.keys():
            if isinstance(kwargs["contents"], dict):
                contents = kwargs["contents"]
            if isinstance(kwargs["contents"], str):
                contents = next(
                    iter(
                        [{k: v for k, v in contents.items() if k == kwargs["contents"]}]
                    )
                )
            if len(contents) == 0:
                raise ValueError("No corresponding contents exist")

        # Replace other attributes with the given arguments
        metadata.data.update(kwargs)
        metadata.data.update({"contents": contents})

        # Select a suitable model and load data
        self.model = self._select_model(metadata)
        self.model = self.model(metadata=metadata, **model_kwargs)

        if as_generator:

            def load_sample_wise():
                for sample in self.model.load(as_generator=as_generator):
                    # Parse data
                    timestamp = np.array(sample["timestamps"])
                    data = np.array(sample["data"])
                    columns = self.model.columns
                    yield timestamp, data, columns

            return load_sample_wise()
        else:
            self.model.load()

            # Parse data
            timestamps = self.model.timestamps
            if as_ndarray:
                data = self.model.to_ndarray()
            else:
                data = self.model.data

            # Apply pre-processes
            for preprocess in self.preprocesses:
                timestamps, data = preprocess.processing(timestamps, data)

            columns = self.model.columns
            return timestamps, data, columns
