#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Toolkit Authors

from typing import Optional

from copy import deepcopy
import hashlib
import logging
import os
from pathlib import Path

from . import BaseDBHandler as _BaseDBHandler
from . import register_handler


@register_handler(db_classes=['meta'], db_engines=['tinydb', 'tinymongo'])
class MetaDBHandler(_BaseDBHandler):
    """Handler for metadb."""

    _database_id: str = ''
    _df_class = 'meta_df'

    def __init__(self,
                 database_id: Optional[str] = 'default',
                 base_dir_path: Optional[str] = None,
                 orient='contents',
                 **kwargs):
        """Initialize MetaDBHandler.

        Args:
            database_id (str): ID of the database (e.g. "Driving Behavior Database")
            base_dir_path (str): base directory for path
            orient (str): key name over which to iterate

        """
        self._database_id = database_id
        self.orient = orient

        # Solve base-dir
        base_dir = base_dir_path
        if base_dir is None:
            base_dir = '/'
        self.base_dir_path = os.path.realpath(base_dir)

        # Prepare another DB-handler for storing a list of database_id
        self._database_id_db_handler = DatabaseIDDBHandler(**{**kwargs, 'read_on_init': False})

        # Prepare buffer for iteration
        self._buf = []
        self._indices = []  # [[record_idx, orient_idx]]
        self._indexed = False

        super(MetaDBHandler, self).__init__(**kwargs)

    def _initialize_engine(self,
                           db_engine: Optional[str] = None,
                           db_host: Optional[str] = None,
                           db_name: Optional[str] = None,
                           db_username: Optional[str] = None,
                           db_password: Optional[str] = None):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = db_engine if db_engine is not None \
            else os.environ.get('PYDTK_META_DB_ENGINE', None)
        username = db_username if db_username is not None \
            else os.environ.get('PYDTK_META_DB_USERNAME', None)
        password = db_password if db_password is not None \
            else os.environ.get('PYDTK_META_DB_PASSWORD', None)
        host = db_host if db_host is not None \
            else os.environ.get('PYDTK_META_DB_HOST', None)
        database = db_name if db_name is not None \
            else os.environ.get('PYDTK_META_DB_DATABASE', None)

        super()._initialize_engine(engine, host, database, username, password)

    def _solve_path(self, data: dict, target: str):
        """Fix absolute path to relative one.

        Args:
            data (dict): a dict containing metadata
            target (str): 'relative' or 'absolute'

        Returns:
            (dict): a dict containing metadata with relative path

        """
        assert isinstance(data, dict)
        assert target in ['relative', 'absolute']

        # Convert absolute path to relative path
        if 'path' in data.keys():
            if target == 'relative':
                # noinspection PyTypeChecker
                data_path = Path(data['path'])
                try:
                    relative_path = data_path.relative_to(self.base_dir_path)
                    data['path'] = relative_path
                except ValueError as e:
                    logging.warning('Could not resolve relative path to file: {}'.format(data_path))
                    logging.warning(str(e))
            elif target == 'absolute':
                data['path'] = os.path.join(self.base_dir_path, data['path'])
            else:
                raise ValueError('Unrecognized target: {}'.format(target))

        return data

    def _reindex(self):
        """Re-index."""
        indices = []

        for idx, value in enumerate(self._data.values()):
            # Count for self.__len__
            if self.orient in value.keys():
                if isinstance(value[self.orient], dict) or isinstance(value[self.orient], list):
                    indices += [[idx, i] for i in range(len(value[self.orient]))]
                else:
                    indices += [[idx, 0]]
            else:
                indices += [[idx, 0]]

        self._indices = indices
        self._indexed = True

    def __len__(self):
        """Return number of orients."""
        if not self._indexed:
            self._reindex()
        return len(self._indices)

    def __next__(self):
        """Return the next item."""
        data = super().__next__()

        # Deserialize content
        data = self._solve_path(data, target='absolute')

        return data

    def __getitem__(self, idx):
        """Return the corresponding item.

        Args:
            idx (int): Index of the item

        Returns:
            (dict): A dict of metadata

        """
        if not self._indexed:
            self._reindex()

        record_idx, orient_idx = self._indices[idx]
        data = super().__getitem__(record_idx)
        data = deepcopy(data)

        if self.orient in data.keys():
            if isinstance(data[self.orient], dict):
                data[self.orient] = \
                    {list(data[self.orient].keys())[orient_idx]:
                         list(data[self.orient].values())[orient_idx]}
            if isinstance(data[self.orient], list):
                data[self.orient] = [data[self.orient][orient_idx]]

        return data

    def add_data(self, data_in: dict, **kwargs):
        """Add data to db.

        Args:
            data_in (dict): data

        """
        data_to_store = self._solve_path(data_in, target='relative')
        super().add_data(data_to_store, **kwargs)

    def remove_data(self, data):
        """Remove data from DB.

        Args:
            data (dict): data to remove

        """
        super().remove_data(data)

    def save(self, *args, **kwargs):
        """Save function."""
        super().save(*args, **kwargs)
        self._database_id_db_handler.add_data({
            'database_id': self._database_id,
            'df_name': self._df_name
        })
        self._database_id_db_handler.save()

    @property
    def data(self):
        """Return data.

        Returns:
            (list): list of dicts

        """
        return [self._solve_path(data, target='absolute') for data in self._data.values()]

    @data.setter
    def data(self, data):
        """Setter for self.data.

        Args:
            data (list): new-data

        """
        assert isinstance(data, list)
        data_in = deepcopy(data)
        indices = []

        for idx, value in enumerate(data_in):
            # Check if UUID exists
            if 'uuid_in_df' not in value.keys():
                raise ValueError('"uuid_in_df" not found in data')

            # Count for self.__len__
            if self.orient in value.keys():
                if isinstance(value[self.orient], dict) or isinstance(value[self.orient], list):
                    indices += [[idx, i] for i in range(len(value[self.orient]))]
                else:
                    indices += [[idx, 0]]
            else:
                indices += [[idx, 0]]

            # Solve path
            data_in[idx] = self._solve_path(value, target='relative')

        self._data = {record['uuid_in_df']: record for record in data_in}
        self._indices = indices
        self._indexed = True

    @property
    def _df_name(self):
        """Return _df_name.

        Returns:
            (str): name

        """
        template = self._config[self._df_class]['df_name']
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode('utf-8'),
            digest_size=self._config.hash.digest_size
        ).hexdigest()
        return template.format(**{
            'database_id': database_id_hashed
        })

    @_df_name.setter
    def _df_name(self, value):
        """Setter for self._df_name."""
        raise RuntimeError(
            'Setting df_name is not supported in MetaDBHandler'
        )


@register_handler(db_classes=['database_id'], db_engines=['tinydb', 'tinymongo'])
class DatabaseIDDBHandler(_BaseDBHandler):
    """Handler for database-id."""

    _df_class = 'database_id_df'
    _df_name = 'database_id_df'

    def _initialize_engine(self,
                           db_engine: Optional[str] = None,
                           db_host: Optional[str] = None,
                           db_name: Optional[str] = None,
                           db_username: Optional[str] = None,
                           db_password: Optional[str] = None):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = db_engine if db_engine is not None \
            else os.environ.get('PYDTK_META_DB_ENGINE', None)
        username = db_username if db_username is not None \
            else os.environ.get('PYDTK_META_DB_USERNAME', None)
        password = db_password if db_password is not None \
            else os.environ.get('PYDTK_META_DB_PASSWORD', None)
        host = db_host if db_host is not None \
            else os.environ.get('PYDTK_META_DB_HOST', None)
        database = db_name if db_name is not None \
            else os.environ.get('PYDTK_META_DB_DATABASE', None)

        super()._initialize_engine(engine, host, database, username, password)