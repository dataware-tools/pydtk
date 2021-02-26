#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Toolkit Authors

"""V4DBHandler."""

from collections import MutableMapping
from copy import deepcopy
from datetime import datetime
import hashlib
import importlib
import inspect
import logging
import os

from attrdict import AttrDict
from deepmerge import Merger
import pandas as pd

from pydtk.utils.utils import \
    load_config, \
    dtype_string_to_dtype_object, \
    _deepmerge_append_list_unique
from pydtk.db.exceptions import DatabaseNotInitializedError
from pydtk.db.v4.engines import DB_ENGINES

DB_HANDLERS = {}  # key: db_class, value: dict( key: db_engine, value: handler )


def register_handlers():
    """Register handlers."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__))):
        if not os.path.isfile(os.path.join(os.path.dirname(__file__), filename)):
            continue
        if filename == '__init__.py':
            continue

        try:
            importlib.import_module(
                os.path.join('pydtk.db.v4.handlers',
                             str(os.path.splitext(filename)[0]).replace(os.sep, '.'))
            )
        except ModuleNotFoundError:
            logging.debug('Failed to load handlers in {}'.format(filename))


def register_handler(db_classes, db_engines):
    """Register a DB-handler.

    Args:
        db_classes (list): list of db_class names (e.g. ['meta'])
        db_engines (list): list of supported db_engines (e.g. ['tinydb', 'mongodb'])

    """

    def decorator(cls):
        for db_class in db_classes:
            if db_class not in DB_HANDLERS.keys():
                DB_HANDLERS.update({db_class: {}})
            for db_engine in db_engines:
                if db_engine not in DB_HANDLERS[db_class].keys():
                    DB_HANDLERS[db_class].update({db_engine: cls})
        return cls

    return decorator


class BaseDBHandler(object):
    """Base handler for db."""

    __version__ = 'v4'
    db_defaults = load_config(__version__).db.base
    _df_class = 'base_df'
    _config = AttrDict()
    _df_name = 'base_df'
    _columns = None

    def __new__(cls, db_class: str = None, db_engine: str = None, **kwargs) -> object:
        """Create object.

        Args:
            db_class (str): database class (e.g. 'meta')
            db_engine (str): database engine (e.g. 'sqlite')
            **kwargs: DB-handler specific arguments

        Returns:
            (object): the corresponding handler object

        """
        if cls is BaseDBHandler:
            handler = cls._get_handler(db_class, db_engine)
            return super(BaseDBHandler, cls).__new__(handler)
        else:
            return super(BaseDBHandler, cls).__new__(cls)

    @classmethod
    def _get_handler(cls, db_class, db_engine=None):
        """Returns an appropriate handler.

        Args:
            db_class (str): database class (e.g. 'meta')
            db_engine (str): database engine (e.g. 'tinydb')

        Returns:
            (handler): database handler object

        """
        # Load default config
        config = load_config(cls.__version__)

        # Check if the db_class is available
        if db_class not in DB_HANDLERS.keys():
            raise ValueError('Unsupported db_class: {}'.format(db_class))

        # Get db_engine from environment variable if not specified
        if db_engine is None:
            db_engine = os.environ.get('PYDTK_{}_DB_ENGINE'.format(db_class.upper()), None)

        # Get the default engine if not specified
        if db_engine is None:
            try:
                db_defaults = getattr(config.sql, db_class)
                db_engine = db_defaults.engine
            except (ValueError, AttributeError):
                raise ValueError('Could not find the default value')

        # Check if the corresponding handler is registered
        if db_engine not in DB_HANDLERS[db_class].keys():
            raise ValueError('Unsupported db_engine: {}'.format(db_engine))

        # Get a DB-handler supporting the engine
        return DB_HANDLERS[db_class][db_engine]

    def __init__(self,
                 db_engine=None,
                 db_host=None,
                 db_name=None,
                 db_username=None,
                 db_password=None,
                 df_name=None,
                 read_on_init=True,
                 **kwargs):
        """Initialize BaseDBHandler.

        Args:
            db_engine (str): database engine (if None, the one in the config file will be used)
            db_host (str): database HOST (if None, the one in the config file will be used)
            db_name (str): database name (if None, the one in the config file will be used)
            db_username (str): username (if None, the one in the config file will be used)
            db_password (str): password (if None, the one in the config file will be used)
            df_name (str): dataframe (table in DB) name (if None, class default value will be used)
            read_on_init (bool): if True, dataframe will be read from database on initialization

        """
        super(BaseDBHandler, self).__init__()
        self.logger = logging.getLogger(__name__)
        self._cursor = 0
        self._data = {}
        self._count_total = 0
        if df_name is not None:
            self.df_name = df_name

        # Load config
        config = load_config(self.__version__)
        self._config = ConfigDict(getattr(config, self._df_class)) \
            if hasattr(config, self._df_class) else ConfigDict()

        # Initialize deepmerger
        self._merger = Merger(
            # pass in a list of tuple, with the
            # strategies you are looking to apply
            # to each type.
            [(dict, ['merge']),
             (list, [_deepmerge_append_list_unique, 'append'])],
            # next, choose the fallback strategies,
            # applied to all other types:
            ["override"],
            # finally, choose the strategies in
            # the case where the types conflict:
            ["override"]
        )

        # Initialize database
        self._initialize_engine(db_engine, db_host, db_name, db_username, db_password)
        self._load_config_from_db()

        # Fetch table
        if read_on_init:
            self.read()

    def __len__(self):
        """Return number of recoreds."""
        return len(self.data)

    def __iter__(self):
        """Return iterator."""
        return self

    def __next__(self):
        """Return the next item."""
        if self._cursor >= len(self):
            self._cursor = 0
            raise StopIteration()

        # Grab data
        data = self[self._cursor]

        # Increment
        self._cursor += 1

        return data

    def __getitem__(self, idx, remove_internal_columns=True):
        """Return data at index idx.

        Args:
            idx (int): index of the target data
            remove_internal_columns (bool): if True, internal columns are removed

        Returns:
            (dict): data

        """
        data = self.data[idx]

        # Delete internal column
        if remove_internal_columns:
            if '_uuid' in data.keys():
                del data['_uuid']
            if '_creation_time' in data.keys():
                del data['_creation_time']

        return data

    def _initialize_engine(self,
                           db_engine=None,
                           db_host=None,
                           db_name=None,
                           db_username=None,
                           db_password=None):
        """Initialize DB engine.

        Args:
            db_engine (str): database engine (if None, the one in the config file will be used)
            db_host (str): database HOST (if None, the one in the config file will be used)
            db_name (str): database name (if None, the one in the config file will be used)
            db_username (str): username (if None, the one in the config file will be used)
            db_password (str): password (if None, the one in the config file will be used)

        """
        self._db_engine = db_engine

        if db_engine in DB_ENGINES.keys():
            self._db = DB_ENGINES[db_engine].connect(
                db_host=db_host,
                db_name=db_name,
                db_username=db_username,
                db_password=db_password,
                collection_name=self._df_name,
                handler=self,
            )
            self._config_db = DB_ENGINES[db_engine].connect(
                db_host=db_host,
                db_name=db_name,
                db_username=db_username,
                db_password=db_password,
                collection_name='--config--{}'.format(self._df_name),
                handler=self,
            )
        else:
            raise ValueError("Unsupported engine: {}".format(db_engine))

    def _load_config_from_db(self):
        """Load configs from DB."""
        try:
            candidates = []
            if self._db_engine in DB_ENGINES.keys():
                candidates = DB_ENGINES[self._db_engine].read(self._config_db, handler=self)
            if len(candidates) > 0:
                if isinstance(candidates[0][0], dict):
                    self._config = ConfigDict(candidates[0][0])
                else:
                    raise TypeError('Unexpected type')
        except Exception as e:
            logging.warning('Failed to load configs from DB: {}'.format(str(e)))

    def _save_config_to_db(self):
        """Save configs to DB."""
        try:
            if self._db_engine in DB_ENGINES.keys():
                config = dict(self._config)
                config.update({'_uuid': '__config__'})
                config = [config]
                DB_ENGINES[self._db_engine].write(self._config_db, data=config, handler=self)
        except Exception as e:
            logging.warning('Failed to save configs to DB: {}'.format(str(e)))

    def _get_uuid_from_item(self, data_in):
        """Return UUID of the given item.

        Args:
            data_in (dict or pandas.Series): dict or Series containing data

        Returns:
            (str): UUID

        """
        hash_target_columns = \
            self._config['index_columns'] if 'index_columns' in self._config.keys() else []

        item = data_in
        if isinstance(item, pd.Series):
            item = data_in.to_dict()

        pre_hash = ''.join([
            '{:.09f}'.format(item[column])
            if isinstance(item[column], float) else
            str(item[column].keys()) if isinstance(item[column], dict) else
            str(item[column])
            for column in hash_target_columns
            if column in item.keys()
        ])
        pre_hash = pre_hash.encode('utf-8')
        uuid = hashlib.md5(pre_hash).hexdigest()
        return uuid

    def _read(self, **kwargs):
        if self._db_engine is None:
            raise DatabaseNotInitializedError()
        elif self._db_engine in DB_ENGINES.keys():
            func = DB_ENGINES[self._db_engine].read
            available_args = set(inspect.signature(func).parameters.keys())
            unavailable_args = set([k for k, v in kwargs.items() if v is not None]) \
                .difference(available_args)
            if len(unavailable_args) > 0:
                logging.warning('DB-engine "{0}" does not support args: {1}'.
                                format(self._db_engine, list(unavailable_args)))
            return func(self._db, handler=self, **kwargs)
        else:
            raise ValueError('Unsupported DB engine: {}'.format(self._db_engine))

    def read(self,
             df_name=None,
             query=None,
             pql=None,
             where=None,
             group_by=None,
             order_by=None,
             limit=None,
             offset=None,
             **kwargs):
        """Read data from SQL.

        Args:
            df_name (str): Dataframe name to read
            query (str SQL query or SQLAlchemy Selectable): query to select items
            pql (PQL): Python-Query-Language to select items
            where (str): query string for filtering items
            group_by (str): column name to group
            order_by (srt): column name to sort by
            limit (int): number of items to return per a page
            offset (int): offset of cursor
            **kwargs: kwargs for function `pandas.read_sql_query`
                      or `influxdb.DataFrameClient.query`

        """
        if df_name is not None:
            self.df_name = df_name

        self.data, self._count_total = self._read(
            query=query,
            pql=pql,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            **kwargs
        )

    def _save(self, data):
        """Save data to DB.

        Args:
            data (list): data to save

        """
        if self._db_engine is None:
            raise DatabaseNotInitializedError()
        elif self._db_engine in DB_ENGINES.keys():
            DB_ENGINES[self._db_engine].write(self._db, data)
        else:
            raise ValueError('Unsupported DB engine: {}'.format(self._db_engine))

    def save(self):
        """Save data to DB."""
        self._save(list(self._data.values()))
        self._save_config_to_db()

    def _remove(self, uuid):
        """Remove data from DB.

        Args:
            uuid (str): A unique ID

        """
        if self._db_engine is None:
            raise DatabaseNotInitializedError()
        elif self._db_engine in DB_ENGINES.keys():
            DB_ENGINES[self._db_engine].remove(self._db, uuid)
        else:
            raise ValueError('Unsupported DB engine: {}'.format(self._db_engine))

    def add_data(self, data_in, strategy='overwrite', **kwargs):
        """Add data to db.

        Args:
            data_in (dict): a dict containing data
            strategy (str): 'merge' or 'overwrite'

        """
        assert strategy in ['merge', 'overwrite'], 'Unknown strategy.'

        data = deepcopy(data_in)
        if '_uuid' not in data.keys() or '_creation_time' not in data.keys():
            # Add _uuid and _creation_time
            data['_uuid'] = self._get_uuid_from_item(data)
            data['_creation_time'] = datetime.now().timestamp()

        if data['_uuid'] in self._data.keys():
            if strategy == 'merge':
                base_data = self._data[data['_uuid']]
                data = self._merger.merge(base_data, data)

        self._data.update({data['_uuid']: data})

    def remove_data(self, data):
        """Remove data-record from DB.

        Args:
            data (dict): a dict containing the target data or '_uuid' in keys.

        """
        if '_uuid' in data.keys():
            uuid = data['_uuid']
        else:
            uuid = self._get_uuid_from_item(data)

        # Remove from in-memory data
        if uuid in self._data.keys():
            del self._data[uuid]

        # Remove from DB
        self._remove(uuid=uuid)

    def _df_from_dicts(self, dicts):
        """Create a DataFrame from a list of dicts.

        Args:
            dicts (list): list of dicts

        Returns:
            (pd.DataFrame): a data-frame

        """
        columns = self._config['columns'] if 'columns' in self._config.keys() else []
        df = pd.concat(
            [pd.Series(name=c['name'],
                       dtype=dtype_string_to_dtype_object(c['dtype']))
             for c in columns
             if c['name'] != '_uuid' and c['name'] != '_creation_time']  # noqa: E501
            + [pd.Series(name='_uuid', dtype=str),
               pd.Series(name='_creation_time', dtype=float)],
            axis=1
        )
        df.set_index('_uuid', inplace=True)
        df = pd.concat([df, pd.DataFrame.from_records(dicts)])
        return df

    @property
    def data(self):
        """Return data.

        Returns:
            (list): list of dicts

        """
        return list(self._data.values())

    @data.setter
    def data(self, data):
        """Setter for self.data.

        Args:
            data (list): new-data

        """
        assert isinstance(data, list)

        # Check if UUID exists in each value
        for value in data:
            if '_uuid' not in value.keys():
                raise ValueError('"_uuid" not found in data')

        self._data = {record['_uuid']: record for record in data}

    @property
    def columns(self):
        """Return columns of DF."""
        return self.df.columns.tolist()

    @property
    def df(self):
        """Return df."""
        df = self._df_from_dicts(self.data)
        return df

    @property
    def count_total(self):
        """Return total number of rows."""
        return self._count_total

    @property
    def config(self):
        """Return config."""
        return self._config


class ConfigDict(MutableMapping, dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    def __setitem__(self, key, value, force=False):
        if key.startswith('_') and not force:
            raise KeyError('key "{}" is not editable'.format(key))
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)

    def __iter__(self):
        return dict.__iter__(self)

    def __len__(self):
        return dict.__len__(self)

    def __contains__(self, x):
        return dict.__contains__(self, x)


register_handlers()
