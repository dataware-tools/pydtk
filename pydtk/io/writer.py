#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""pydtk modules."""

from abc import ABCMeta
import pdb
import os

from pydtk.models import MODELS_BY_PRIORITY, MetaDataModel
from pydtk.preprocesses import PassThrough


class NoModelMatchedError(BaseException):
    """Error of module matching."""

    pass


class BaseFileWriter(metaclass=ABCMeta):
    """Base file writer."""

    _model = None   # model used for loading a file

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
        raise NoModelMatchedError('No suitable model found for loading data: {}'.
                                  format(file_metadata))

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

    def write(self,
              metadata=None,
              data=None,
              model_kwargs=None,
              **kwargs
              ):
        """write a file which corresponds to the given metadata.

        Args:
            metadata (MetaDataModel or dict): metadata of the data to save
            data (numpy array): data 
            model_kwargs (dict): kwargs to pass to the selected model

        Returns:
            void

        """
        if model_kwargs is None:
            model_kwargs = {}
        if metadata is None:
            if 'path' not in kwargs.keys():
                raise ValueError('Either metadata or path must be specified')
            # Look for the corresponding metadata file
            for ext in MetaDataModel._file_extensions:
                metadata_filepath = kwargs['path'] + ext
                if os.path.isfile(metadata_filepath):
                    metadata = MetaDataModel()
                    metadata.load(metadata_filepath)
            if metadata is None:
                raise IOError('Could not find metadata file')
        else:
            metadata = MetaDataModel(metadata)

        # Replace 'contents' in metadata to specify which content to load
        contents = metadata.data['contents'] if 'contents' in metadata.data.keys() else None
        if 'contents' in kwargs.keys():
            if isinstance(kwargs['contents'], dict):
                contents = kwargs['contents']
            if isinstance(kwargs['contents'], str):
                contents = next(iter([{k: v for k, v in contents.items()
                                       if k == kwargs['contents']}]))
            if len(contents) == 0:
                raise ValueError('No corresponding contents exist')

        # Replace other attributes with the given arguments
        metadata.data.update(kwargs)
        metadata.data.update({'contents': contents})

        # Select a suitable model and load data
        self.model = self._select_model(metadata)
        self.model = self.model(metadata=metadata, data=data, **model_kwargs)
        self.model.save(data=data)
