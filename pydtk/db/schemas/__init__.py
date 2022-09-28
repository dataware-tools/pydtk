import importlib
import os
from importlib.util import spec_from_file_location

import pydtk
from pydantic import BaseModel, Field, constr

from pydtk.db.exceptions import SchemaNotFoundError


class BaseSchema(BaseModel):
    _api_version: constr(min_length=1, strict=True) = Field(..., description="Schema version information.")
    _kind: constr(min_length=1, strict=True) = Field(..., description="Kind of information")
    _uuid: constr(min_length=1, strict=True) = Field(..., description="Universally unique ID")


def get_schema(api_version: str, kind: str):
    """Get schema based on the given `api_version` and `kind`.

    Args:
        api_version (str): Schema version information.
        kind (str): Kind of information.

    Returns:
        (BaseSchema): the corresponding schema.

    """
    try:
        schema = spec_from_file_location("schema", os.path.join(os.path.dirname(pydtk.__file__), "db", "schemas", api_version.replace("/", os.sep).lower(), f"{kind.lower()}.py"))
        module  = importlib.util.module_from_spec(schema)
        schema.loader.exec_module(module)
    except FileNotFoundError:
        raise SchemaNotFoundError()

    res = getattr(module, kind)
    return res
