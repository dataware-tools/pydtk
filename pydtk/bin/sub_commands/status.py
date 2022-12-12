#!/usr/bin/env python3

# Copyright Toolkit Authors

import os

from pydtk.db import DBHandler


class STATUS(object):
    """Status checker."""

    def environment(self):
        """Display environment variables."""
        env_vars = [
            "PYDTK_META_DB_ENGINE",
            "PYDTK_META_DB_USERNAME",
            "PYDTK_META_DB_PASSWORD",
            "PYDTK_META_DB_HOST",
            "PYDTK_META_DB_DATABASE",
        ]
        for env_var in env_vars:
            print(f"{env_var}:\t{os.environ.get(env_var, None)}")

    def env(self):
        """Display environment variables."""
        self.environment()

    def access(self):
        """Check access."""
        handler = DBHandler(db_class="database_id")
        try:
            handler.read()
        except Exception:
            print("Can't access database: database_id")
            return
        # Check reading
        for row in handler:
            database_id = row["database_id"]
            readable = self._read_check(database_id=database_id)
            print(f'{database_id}\tread: {"O.K." if readable else "N.G."}')

    def _read_check(self, database_id: str, base_dir: str = "/"):
        """Check access to DB."""
        handler = DBHandler(
            db_class="meta",
            database_id=database_id,
            base_dir_path=base_dir,
            orient="contents",
        )
        try:
            handler.read(limit=1)
            return True
        except Exception:
            return False
