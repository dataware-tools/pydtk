#!/usr/bin/env python3

# Copyright Toolkit Authors
import json
import os
import sys

import pandas

from pydtk.db import DBHandler
from pydtk.models import MetaDataModel
from pydtk.utils.utils import fix_args_type

pandas.set_option("display.max_columns", None)
pandas.set_option("display.width", None)


class EmptySTDINError(Exception):
    """Exception for the case that STDIN is empty."""

    pass


def _get_db_handler(target: str, database_id: str = "default", base_dir: str = "/"):
    if not isinstance(database_id, str):
        raise ValueError(f"database_id must be str, not {type(database_id)}")

    if target in ["databases", "database"]:
        handler = DBHandler(db_class="database_id")
        group_by = None
    elif target in ["records", "record"]:
        handler = DBHandler(
            db_class="meta",
            database_id=database_id,
            base_dir_path=base_dir,
            orient="record_id",
        )
        group_by = "record_id"
    elif target in ["files", "file"]:
        handler = DBHandler(
            db_class="meta",
            database_id=database_id,
            base_dir_path=base_dir,
            orient="path",
        )
        group_by = None
    elif target in ["contents", "content"]:
        handler = DBHandler(
            db_class="meta",
            database_id=database_id,
            base_dir_path=base_dir,
            orient="contents",
        )
        group_by = None
    else:
        raise ValueError("Unknown target")
    return handler, group_by


def _assert_target(target):
    assert target in [
        "database",
        "databases",
        "record",
        "records",
        "file",
        "files",
        "content",
        "contents",
    ], "target must be one of 'database', 'record', 'file' or 'content"


def _add_data(handler, data, target="content"):
    """Add data depending on target."""
    if target in ["record", "records"]:
        handler.add_record(data)
    elif target in ["file", "files"]:
        handler.add_file(data)
    else:
        handler.add_data(data)


def _add_data_from_stdin(handler, target="content"):
    raw_data = sys.stdin.read()
    if raw_data == "":
        raise EmptySTDINError("STDIN is empty")
    data = json.loads(raw_data)
    if isinstance(data, dict):
        metadata = MetaDataModel(data=data)
        _add_data(handler, metadata.data, target)
    elif isinstance(data, list):
        for element in data:
            metadata = MetaDataModel(data=element)
            if target == "data":
                _target = metadata.data["_kind"]
                assert _target in [
                    "record",
                    "file",
                ], "The '_kind' field of each metadata must be 'record' or 'file'. "
                _add_data(handler, metadata.data, _target)
            else:
                _add_data(handler, metadata.data, target)


class DB(object):
    """DB."""

    _handler = None

    @fix_args_type
    def list(
        self,
        target: str,
        database_id: str = "default",
        pql: str = None,
        offset: int = 0,
        limit: int = 20,
        order_by: str = None,
        base_dir: str = "/",
        **kwargs,
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

        handler, group_by = _get_db_handler(
            target, database_id=database_id, base_dir=base_dir
        )

        # Prepare search query
        if order_by is not None:
            order_by = [(order_by, 1)]

        # Read
        handler.read(
            pql=pql, limit=limit, offset=offset, order_by=order_by, group_by=group_by
        )

        # Display
        _display(
            handler,
            columns=[
                "Database ID",
                "Record ID",
                "Description",
                "File path",
                "Contents",
                "Tags",
            ],
            **kwargs,
        )

        self._handler = handler

    @fix_args_type
    def get(
        self,
        target: str,
        database_id: str = "default",
        record_id: str = None,
        path: str = None,
        content: str = None,
        base_dir: str = "/",
        **kwargs,
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

        handler, group_by = _get_db_handler(
            target, database_id=database_id, base_dir=base_dir
        )

        # Prepare query
        pql = ""
        if target in ["database", "databases"]:
            if database_id is not None:
                pql += " and " if pql != "" else ""
                pql += 'database_id == "{}"'.format(database_id)
        else:
            if record_id is not None:
                pql += " and " if pql != "" else ""
                pql += 'record_id == "{}"'.format(record_id)
            if path is not None:
                relative_path = handler._solve_path(path, "relative")
                pql += " and " if pql != "" else ""
                pql += ' path == "{}"'.format(relative_path)
            if content is not None:
                pql += " and " if pql != "" else ""
                pql += '"contents.{}" == exists(True)'.format(content)

        # Read
        handler.read(pql=pql, group_by=group_by)

        # Display
        _display(handler, **kwargs)

        self._handler = handler

    @fix_args_type
    def add(
        self,
        target: str,
        content: str = None,
        database_id: str = "default",
        base_dir: str = "/",
        overwrite: bool = False,
        skip_checking_existence: bool = False,
        **kwargs,
    ):
        """Add resources.

        Args:
            target (str): 'databases', 'records', 'files' or 'contents'
            *
            content (str): Content to add. This must be one of the followings:
                           1. Database ID (in case of adding database)
                           2. Path to a JSON file (in case of adding metadata)
                           3. Path to a directory containing JSON files (in case of adding metadata)
                           4. Empty (in case of adding metadata)
                           In the last case, PyDTK reads STDIN as JSON to add metadata.
            database_id (str): Database ID
            base_dir (str): Base directory
            overwrite (bool): Overwrite the existing data on DB
            skip_checking_existence (bool): Skip checking the existence of the input data in DB

        """
        if not target == "data":
            _assert_target(target)

        num_added = 0
        num_updated = 0

        # Initialize Handler
        handler, _ = _get_db_handler(target, database_id=database_id, base_dir=base_dir)

        if target in ["database", "databases"]:
            database_id = database_id if content is None else content
            data = {"database_id": database_id, "name": database_id}
            if content is None:
                try:
                    _add_data_from_stdin(handler, target)
                except EmptySTDINError:
                    handler.add_data(data)
            else:
                handler.add_data(data)

        else:
            if content is None:
                _add_data_from_stdin(handler, target)
            else:
                if os.path.isfile(content):
                    f = open(content, "r")
                    data = json.load(f)
                    f.close()
                    metadata = MetaDataModel(data=data)
                    _add_data(handler, metadata.data, target)
                elif os.path.isdir(content):
                    from pydtk.builder.meta_db import _find_json as find_json

                    json_list = find_json(content)
                    for json_file in json_list:
                        metadata = MetaDataModel()
                        metadata.load(json_file)
                        _add_data(handler, metadata.data, target)
                else:
                    raise IOError("No such file or directory")

        if not skip_checking_existence:
            # Check if the UUIDs already exist on DB
            uuids = [data["_uuid"] for data in handler.data]
            self.list(
                target,
                database_id=database_id,
                pql=" or ".join(['_uuid == "{}"'.format(uuid) for uuid in uuids]),
                quiet=True,
                include_summary=False,
            )

            # Ask
            if not overwrite and len(self._handler) > 0:
                print("The following data on DB will be overwritten.")
                _display(self._handler, **kwargs)
                sys.stdin = open("/dev/tty")
                judge = input("Proceed? [y/N]: ")
                if judge not in ["y", "Y"]:
                    print("Cancelled.")
                    sys.exit(1)

            num_updated += len(self._handler)

        num_added += len(handler) - num_updated

        # Save
        handler.save()

        # Display
        if num_updated > 0:
            print("Updated: {} items.".format(num_updated))
        if num_added > 0:
            print("Added: {} items.".format(num_added))

        self._handler = handler

    @fix_args_type
    def delete(
        self,
        target: str,
        database_id: str = "default",
        record_id: str = None,
        path: str = None,
        content: str = None,
        base_dir: str = "/",
        **kwargs,
    ):
        """Delete resources.

        Args:
            target (str): 'database' or 'metadata'
            *
            database_id (str): Database ID
            record_id (str): Record ID
            path (str): File path
            content (str): Content
            base_dir (str): Base directory

        """
        _assert_target(target)
        yes = False
        quiet = False
        if "y" in kwargs and kwargs["y"]:
            yes = True
        if "yes" in kwargs and kwargs["yes"]:
            yes = True
        if "q" in kwargs and kwargs["q"]:
            quiet = True
        if "quiet" in kwargs and kwargs["quiet"]:
            quiet = True

        # Get the corresponding resources
        if not all([record_id is None, path is None, content is None]):
            self.get(
                target=target,
                database_id=database_id,
                record_id=record_id,
                path=path,
                content=content,
                columns=[
                    "Database ID",
                    "Record ID",
                    "Description",
                    "File path",
                    "Contents",
                    "Tags",
                ],
                **kwargs,
            )
            handler = self._handler

        else:
            handler = DBHandler(
                db_class="meta",
                database_id=database_id,
                base_dir_path=base_dir,
            )
            _add_data_from_stdin(handler)

        # Get the corresponding data from DB
        uuids = [data["_uuid"] for data in handler.data]
        self.list(
            target,
            database_id=database_id,
            pql=" or ".join(['_uuid == "{}"'.format(uuid) for uuid in uuids]),
            quiet=True,
            include_summary=False,
        )

        if not yes and not quiet and len(self._handler) > 0:
            print("The following data will be deleted:")
            _display(self._handler, **kwargs)
            sys.stdin = open("/dev/tty")
            judge = input("Proceed? [y/N]: ")
            if judge not in ["y", "Y"]:
                print("Cancelled.")
                sys.exit(1)

        if len(self._handler) == 0:
            if not quiet:
                print(f"No such {target}")
            sys.exit(1)

        # Delete metadata
        num_deleted = len(self._handler)
        for data in self._handler.data:
            handler.remove_data(data)
            self._handler.remove_data(data)

        # Save
        self._handler.save()

        # Display
        if not quiet:
            print("Deleted: {} items.".format(num_deleted))
            if len(handler) > 0:
                print("The following {} items were not found.".format(len(handler)))
                _display(handler, include_summary=False, **kwargs)


def _display(handler: DBHandler, columns: list = None, include_summary=True, **kwargs):
    """Display data.

    Args:
        handler (DBHandler): Database handler
        *
        columns (list): List of columns to display
        include_summary (bool): Display summary

    """
    parsable = False
    if "quiet" in kwargs and kwargs["quiet"]:
        return
    if "p" in kwargs and kwargs["p"]:
        parsable = True
    if "parsable" in kwargs and kwargs["parsable"]:
        parsable = True

    if not parsable:
        available_columns = [
            column for column in handler.df.columns if not column.startswith("_")
        ]
        if columns is not None:
            df = handler.df[[c for c in columns if c in available_columns]]
        else:
            df = handler.df[available_columns]
        if include_summary:
            print(f"Found: {handler.count_total} items.")
        if handler.count_total > len(handler):
            print(
                f"Only {len(handler)} items are displayed. "
                "Please use `--limit` or `--offset` to see more."
            )
        print(df)

    else:
        print(json.dumps(handler.data, indent=4, default=_default_json_handler))


def _default_json_handler(o):
    return o.__str__()
