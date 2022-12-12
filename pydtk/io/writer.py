#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""pydtk modules."""

from abc import ABCMeta

from pydtk.io.errors import NoModelMatchedError
from pydtk.models import MODELS_BY_PRIORITY, MetaDataModel
from pydtk.preprocesses import PassThrough


class BaseFileWriter(metaclass=ABCMeta):
    """Base file writer."""

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

    def write(self, metadata=None, data=None, model_kwargs=None, **kwargs):
        """Write a file which corresponds to the given metadata.

        Args:
            metadata (dict or MetaDataModel): metadata of the data to save
            data (numpy array): data
            model_kwargs (dict): kwargs to pass to the selected model

        Returns:
            void

        """
        if model_kwargs is None:
            model_kwargs = {}

        # Check metadata is valid
        if metadata is None:
            raise ValueError("Metadata must be specified")
        if type(metadata) is dict:
            if "path" not in metadata.keys():
                raise ValueError("Metadata must have path key")
            else:
                metadata = MetaDataModel(metadata)
        elif type(metadata) is MetaDataModel:
            pass
        else:
            raise ValueError("Type of metadata must be dict or MetaDataModel")

        metadata.save(metadata.data["path"] + metadata._file_extensions[0])

        # Select a suitable model and save data
        self.model = self._select_model(metadata)
        self.model = self.model(metadata=metadata, data=data, **model_kwargs)
        self.model.save()
