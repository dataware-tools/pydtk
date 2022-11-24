"""Schemas."""
import glob
import importlib
import logging
import os
import pathlib
from importlib.util import spec_from_file_location

from pydantic import BaseModel, Field, constr

from pydtk.db.exceptions import SchemaNotFoundError

SCHEMAS_BY_VERSIONS = {}  # key: api_version, value: {kind: {"schema": schema}}
SCHEMAS_BY_FILES = {}  # key: relative file path, value: list of schemas}

import inspect


def register_schemas():
    """Register schemas."""
    global module_path
    files = glob.glob(f"{os.path.dirname(__file__)}/*/*/*.py", recursive=False)

    for file in files:
        filename = os.path.basename(file)
        if filename == "__init__.py":
            continue

        module_path = file
        try:
            schema = spec_from_file_location("schema", file)
            module = importlib.util.module_from_spec(schema)
            schema.loader.exec_module(module)
        except (ModuleNotFoundError, ImportError):
            logging.warning("Failed to load models in {}".format(filename))


def register_schema(schema):
    """Regist a schema."""
    rel_file = str(
        pathlib.Path(inspect.currentframe().f_back.f_code.co_filename).relative_to(
            os.path.dirname(__file__)
        )
    )

    def decorator():
        if schema._api_version.lower() not in SCHEMAS_BY_VERSIONS.keys():
            SCHEMAS_BY_VERSIONS.update({schema._api_version.lower(): {}})
        SCHEMAS_BY_VERSIONS[schema._api_version.lower()][schema._kind.lower()] = schema

        if rel_file not in SCHEMAS_BY_FILES.keys():
            SCHEMAS_BY_FILES.update({rel_file: []})
        SCHEMAS_BY_FILES[rel_file].append(schema)

        return schema

    return decorator()


class BaseSchema(BaseModel):
    """BaseSchema."""

    api_version: constr(min_length=1, strict=True) = Field(
        ...,
        description="Schema version information.",
        alias="_api_version",
    )
    kind: constr(min_length=1, strict=True) = Field(
        ...,
        description="Kind of information",
        alias="_kind",
    )
    # NOTE(kan-bayashi): `_uuid` and `_creation_time` will be created by pydtk
    #   and therefore we do not validate them.
    # uuid: constr(min_length=1, strict=True) = Field(
    #     ...,
    #     description="Universally unique ID",
    #     alias="_uuid",
    # )
    # creation_time: int = Field(
    #     ...,
    #     description="Creation time",
    #     alias="_creation_time",
    # )


def get_schema(api_version: str, kind: str):
    """Get schema based on the given `api_version` and `kind`.

    Args:
        api_version (str): Schema version information.
        kind (str): Kind of information.

    Returns:
        (BaseSchema): the corresponding schema.

    """
    try:
        schema = SCHEMAS_BY_VERSIONS[api_version.replace("/", os.sep).lower()][
            kind.lower()
        ]
    except KeyError:
        raise SchemaNotFoundError()
    return schema


register_schemas()
