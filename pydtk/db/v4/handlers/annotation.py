"""DB Handler."""

import hashlib
import os

from pydtk.utils.utils import load_config

from . import BaseDBHandler as _BaseDBHandler
from . import register_handler


@register_handler(
    db_classes=["annotation"], db_engines=["tinydb", "tinymongo", "mongodb", "montydb"]
)
class AnnotationDBHandler(_BaseDBHandler):
    """Handler for annotations."""

    __version__ = "v4"
    db_defaults = load_config(__version__).db.connection.annotations
    _df_class = "annotation_df"
    _database_id: str = ""

    def __init__(self, database_id: str = "default", **kwargs):
        """Initialize AnnotationDBHandler.

        Args:
            database_id (str): ID of the database.

        Returns:
            (DBHandler): Database Handler.

        """
        self._database_id = database_id
        super(AnnotationDBHandler, self).__init__(**kwargs)

    def _initialize_engine(
        self,
        db_engine: str = None,
        db_host: str = None,
        db_name: str = None,
        db_username: str = None,
        db_password: str = None,
    ):
        """Initialize DB engine."""
        # Load settings from environment variables
        engine = (
            db_engine
            if db_engine is not None
            else os.environ.get("PYDTK_ANNOTATION_DB_ENGINE", None)
        )
        username = (
            db_username
            if db_username is not None
            else os.environ.get("PYDTK_ANNOTATION_DB_USERNAME", None)
        )
        password = (
            db_password
            if db_password is not None
            else os.environ.get("PYDTK_ANNOTATION_DB_PASSWORD", None)
        )
        host = (
            db_host
            if db_host is not None
            else os.environ.get("PYDTK_ANNOTATION_DB_HOST", None)
        )
        database = (
            db_name
            if db_name is not None
            else os.environ.get("PYDTK_ANNOTATION_DB_DATABASE", None)
        )

        super()._initialize_engine(engine, host, database, username, password)

    def add_data(self, *args, **kwargs):
        """Add data."""
        super().add_data(*args, **kwargs)

        # Fix column aggregation to 'last' so that the latest annotation is returned
        # when grouping annotations by `annotation_id`
        for column in self.config["columns"]:
            column["aggregation"] = "last"

    @property
    def _df_name(self):
        """Return _df_name.

        Returns:
            (str): name

        """
        template = (
            self._config["_df_name"]
            if "_df_name" in self._config.keys()
            else "{database_id}"
        )
        digest_size = (
            self._config["_hash_digest_size"]
            if "_hash_digest_size" in self._config.keys()
            else 4
        )
        database_id_hashed = hashlib.blake2s(
            self._database_id.encode("utf-8"), digest_size=digest_size
        ).hexdigest()
        return template.format(**{"database_id": database_id_hashed})

    @_df_name.setter
    def _df_name(self, value):
        """Setter for self._df_name."""
        raise RuntimeError("Setting df_name is not supported in AnnotationDBHandler")
