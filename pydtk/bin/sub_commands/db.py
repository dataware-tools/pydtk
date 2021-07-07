#!/usr/bin/env python3

# Copyright Toolkit Authors

import json
import os
import sys

import pandas
from pydtk.db import DBHandler
from pydtk.models import MetaDataModel

pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)


def _get_db_handler(target: str, database_id: str = 'default', base_dir: str = '/'):
    if target in ['databases', 'database']:
        handler = DBHandler(
            db_class='database_id'
        )
        group_by = None
    elif target in ['records', 'record']:
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            base_dir_path=base_dir,
            orient='record_id'
        )
        group_by = 'record_id'
    elif target in ['files', 'file']:
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            base_dir_path=base_dir,
            orient='path'
        )
        group_by = None
    elif target in ['contents', 'content']:
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            base_dir_path=base_dir,
            orient='contents'
        )
        group_by = None
    else:
        raise ValueError('Unknown target')
    return handler, group_by


def _assert_target(target):
    assert target in [
        'database',
        'databases',
        'record',
        'records',
        'file',
        'files',
        'content',
        'contents'
    ], \
        "target must be one of 'database', 'record', 'file' or 'content"


def _add_data_from_stdin(handler):
    data = json.load(sys.stdin)
    if isinstance(data, dict):
        metadata = MetaDataModel(data=data)
        handler.add_data(metadata.data)
    elif isinstance(data, list):
        for element in data:
            metadata = MetaDataModel(data=element)
            handler.add_data(metadata.data)


class DB(object):
    """DB."""

    _handler = None

    def list(
        self,
        target: str,
        database_id: str = 'default',
        pql: str = None,
        offset: int = 0,
        limit: int = 20,
        order_by: str = None,
        base_dir: str = '/',
        **kwargs
    ):
        """List resources in DB.

        Args:
            target (str): 'databases', 'records', 'files' or 'contents
            *
            database_id (str): Database ID
            pql (str): Python-Query-Language for searching metadata
            offset (int): Cursor offset
            limit (int): Number of elements to show
            order_by (str): Sort key
            base_dir (str): Base directory

        Examples:
            List available databases using CLI:

            ```bash
            $ pydtk db list databases

            ```

            List records:
            ```bash
            $ pydtk db list records --database_id <Database ID>

            ```

            List records with PQL:
            ```bash
            $ pydtk db list records --database_id <Database ID> --pql 'record_id == regex("abc.*")'

            ```

            List records and display as a parsable string
            ```bash
            $ pydtk db list records --parsable

            ```

        """
        _assert_target(target)

        handler, group_by = _get_db_handler(target, database_id=database_id, base_dir=base_dir)

        # Prepare search query
        if order_by is not None:
            order_by = [(order_by, 1)]

        # Read
        handler.read(pql=pql, limit=limit, offset=offset, order_by=order_by, group_by=group_by)

        # Display
        _display(
            handler,
            columns=['Database ID', 'Record ID', 'Description', 'File path', 'Contents', 'Tags'],
            **kwargs
        )

        self._handler = handler

    def get(
        self,
        target: str,
        database_id: str = 'default',
        record_id: str = None,
        path: str = None,
        content: str = None,
        base_dir: str = '/',
        **kwargs
    ):
        """Get resources.

        Args:
            target (str): 'databases', 'records', 'files' or 'contents
            *
            database_id (str): Database ID
            record_id (str): Record ID
            path (str): File path
            content (str): Content
            base_dir (str): Base directory

        """
        _assert_target(target)

        handler, group_by = _get_db_handler(target, database_id=database_id, base_dir=base_dir)

        # Prepare query
        pql = ''
        if target in ['database', 'databases']:
            if database_id is not None:
                pql += ' and ' if pql != '' else ''
                pql += 'database_id == "{}"'.format(database_id)
        else:
            if record_id is not None:
                pql += ' and ' if pql != '' else ''
                pql += 'record_id == "{}"'.format(record_id)
            if path is not None:
                pql += ' and ' if pql != '' else ''
                pql += ' path == "{}"'.format(path)
            if content is not None:
                pql += ' and ' if pql != '' else ''
                pql += '"contents.{}" == exists(True)'.format(content)

        # Read
        handler.read(pql=pql, group_by=group_by)

        # Display
        _display(handler, **kwargs)

        self._handler = handler

    def add(
        self,
        target: str,
        content: str = None,
        database_id: str = 'default',
        base_dir: str = '/',
        **kwargs
    ):
        """Add resources.

        Args:
            target (str): 'databases', 'records', 'files' or 'contents
            *
            content (str): Content to add. This must be one of the followings:
                           1. Database ID (in case of adding database)
                           2. Path to a JSON file (in case of adding metadata)
                           3. Path to a directory containing JSON files (in case of adding metadata)
                           3. Empty (in case of adding metadata)
                           In the last case, PyDTK reads STDIN as JSON to add metadata.
            database_id (str): Database ID
            base_dir (str): Base directory

        """
        _assert_target(target)

        if target in ['database', 'databases']:
            if content is not None:
                database_id = content

        # Initialize Handler
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            base_dir_path=base_dir
        )

        if target not in ['database', 'databases']:
            if content is None:
                _add_data_from_stdin(handler)
            else:
                if os.path.isfile(content):
                    f = open(content, 'r')
                    data = json.load(f)
                    f.close()
                    metadata = MetaDataModel(data=data)
                    handler.add_data(metadata.data)
                elif os.path.isdir(content):
                    from pydtk.builder.meta_db import main as add_metadata_from_dir
                    add_metadata_from_dir(
                        target_dir=content,
                        database_id=database_id,
                        base_dir=base_dir
                    )
                else:
                    raise IOError('No such file or directory')

        # Save
        handler.save()

        self._handler = handler

    def delete(
        self,
        target: str,
        database_id: str = 'default',
        record_id: str = None,
        path: str = None,
        content: str = None,
        y: bool = False,
        base_dir: str = '/',
        **kwargs
    ):
        """Delete resources.

        Args:
            target (str): 'database' or 'metadata'
            *
            database_id (str): Database ID
            record_id (str): Record ID
            path (str): File path
            content (str): Content
            y (bool): Always answer yes
            base_dir (str): Base directory

        """
        _assert_target(target)

        print('The following data will be deleted:')

        # Get the corresponding resources
        if not all([
            record_id is None,
            path is None,
            content is None
        ]):
            self.get(
                target=target,
                database_id=database_id,
                record_id=record_id,
                path=path,
                content=content,
                columns=['Database ID', 'Record ID', 'Description', 'File path', 'Contents',
                         'Tags'],
                **kwargs
            )
            handler = self._handler

            if not y:
                judge = input("Proceed? [y/N]: ")
                if judge not in ['y', 'Y']:
                    print('Cancelled.')
                    sys.exit(1)

        else:
            handler = DBHandler(
                db_class='meta',
                database_id=database_id,
                base_dir_path=base_dir,
            )
            _add_data_from_stdin(handler)

        # Delete metadata
        data_all = [data for data in handler]
        for data in data_all:
            handler.remove_data(data)

        handler.save()
        print('Deleted.')


def _display(handler: DBHandler, columns: list = None, **kwargs):
    """Display data.

    Args:
        handler (DBHandler): Database handler
        *
        columns (list): List of columns to display

    """
    parsable = False
    if 'p' in kwargs and kwargs['p']:
        parsable = True
    if 'parsable' in kwargs and kwargs['parsable']:
        parsable = True

    if not parsable:
        available_columns = [column for column in handler.df.columns if not column.startswith('_')]
        if columns is not None:
            df = handler.df[[c for c in columns if c in available_columns]]
        else:
            df = handler.df[available_columns]
        print(f'Found: {handler.count_total} items.')
        if handler.count_total > len(handler):
            print(f'Only {len(handler)} items are displayed. '
                  'Please use `--limit` or `--offset` to see more.')
        print(df)

    else:
        print(json.dumps(handler.data, indent=4, default=_default_json_handler))


def _default_json_handler(o):
    return o.__str__()
