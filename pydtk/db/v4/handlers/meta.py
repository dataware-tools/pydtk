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

from flatten_dict import flatten

from . import BaseDBHandler as _BaseDBHandler
from . import register_handler


@register_handler(db_classes=['meta'], db_engines=['tinydb', 'tinymongo', 'mongodb', 'montydb'])
class MetaDBHandler(_BaseDBHandler):
    """Handler for metadb."""

    _database_id: str = ''
    _df_class = 'meta_df'

    def __init__(self,
                 database_id: Optional[str] = 'default',
                 base_dir_path: Optional[str] = None,
                 orient='path',
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

    def _solve_path(self, path: str, target: str):
        if target == 'relative':
            # noinspection PyTypeChecker
            data_path = Path(path)
            try:
                relative_path = data_path.relative_to(self.base_dir_path)
                path = str(relative_path)
            except ValueError as e:
                logging.warning('Could not resolve relative path to file: {}'.format(data_path))
                logging.warning(str(e))
        elif target == 'absolute':
            path = os.path.join(self.base_dir_path, path)
        else:
            raise ValueError('Unrecognized target: {}'.format(target))

        return path

    def _solve_path_in_data(self, data_in: dict, target: str):
        """Fix absolute path to relative one.

        Args:
            data_in (dict): a dict containing metadata
            target (str): 'relative' or 'absolute'

        Returns:
            (dict): a dict containing metadata with relative path

        """
        assert isinstance(data_in, dict)
        assert target in ['relative', 'absolute']

        data = deepcopy(data_in)

        # Convert absolute path to relative path
        if 'path' in data.keys():
            if isinstance(data['path'], str):
                data['path'] = self._solve_path(data['path'], target=target)
            elif isinstance(data['path'], list):
                for idx in range(len(data['path'])):
                    data['path'][idx] = self._solve_path(data['path'][idx], target=target)
            else:
                raise TypeError('Unsupported type')

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
        self._cursor = 0

    def __len__(self):
        """Return number of orients."""
        if not self._indexed:
            self._reindex()
        return len(self._indices)

    def __next__(self):
        """Return the next item."""
        data = super().__next__()

        return data

    def __getitem__(self, idx, **kwargs):
        """Return the corresponding item.

        Args:
            idx (int): Index of the item

        Returns:
            (dict): A dict of metadata

        """
        if not self._indexed:
            self._reindex()

        record_idx, orient_idx = self._indices[idx]
        data = super().__getitem__(record_idx, **kwargs)

        # Deserialize content
        data = self._solve_path_in_data(data, target='absolute')

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
        data_to_store = self._solve_path_in_data(data_in, target='relative')
        super().add_data(data_to_store, **kwargs)
        self._indexed = False

    def remove_data(self, data):
        """Remove data from DB.

        Args:
            data (dict): data to remove

        """
        data_to_remove = self._solve_path_in_data(data, target='relative')
        super().remove_data(data_to_remove)
        self._indexed = False

    def read(self, *args, **kwargs):
        """Read function."""
        super().read(*args, **kwargs)
        self._indexed = False

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
        return [self._solve_path_in_data(data, target='absolute') for data in self._data.values()]

    @data.setter
    def data(self, data):
        """Setter for self.data.

        Args:
            data (list): new-data

        """
        assert isinstance(data, list)
        data_in = deepcopy(data)

        for value in data_in:
            value = self._solve_path_in_data(value, target='relative')
            self.add_data(value)

        self._indexed = False

    @property
    def _df_name(self):
        """Return _df_name.

        Returns:
            (str): name

        """
        template = self._config['_df_name'] \
            if '_df_name' in self._config.keys() else '{database_id}'
        digest_size = self._config['_hash_digest_size'] \
            if '_hash_digest_size' in self._config.keys() else 4
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode('utf-8'),
            digest_size=digest_size
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

    @property
    def _df(self):
        """Return df."""
        data = []
        for idx in range(len(self)):
            _data = self.__getitem__(idx, remove_internal_columns=False)
            assert isinstance(_data, dict)

            # Expand the contents of key `self.orient`
            if self.orient in _data.keys():
                if isinstance(_data[self.orient], list):
                    assert len(_data[self.orient]) == 1
                    if isinstance(_data[self.orient][0], dict):
                        value = flatten(next(iter(_data[self.orient])), reducer='dot')
                        _data = self._merger.merge(_data, value)
                    else:
                        _data[self.orient] = _data[self.orient][0]
                elif isinstance(_data[self.orient], dict):
                    assert len(_data[self.orient]) == 1
                    key = next(iter(_data[self.orient].keys()))
                    value = flatten(next(iter(_data[self.orient].values())), reducer='dot')
                    _data[self.orient] = key
                    _data = self._merger.merge(_data, value)
                else:
                    pass
            data.append(_data)

        df = self._df_from_dicts(data)
        return df

    @property
    def df(self):
        """Return df with display_names."""
        df = self._df
        self._to_display_names(df, inplace=True)
        return df


@register_handler(
    db_classes=['database_id'],
    db_engines=['tinydb', 'tinymongo', 'mongodb', 'montydb']
)
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

    def remove_data(self, data, cascade=True):
        """Remove data from DB.

        Args:
            data (dict): Data to remove
            cascade (bool): If True, the corresponding table will also be removed

        """
        # Delete the corresponding table containing metadata
        if cascade:
            if 'df_name' in data.keys():
                target_collection = data['df_name']
                super().drop_table(target_collection)
            else:
                logging.warning(
                    'Skipped dropping the corresponding table '
                    'as key `df_name` is not included in the given data'
                )

        super().remove_data(data)
