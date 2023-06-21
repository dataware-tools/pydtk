import os

from pydantic import Extra

from pydtk.db.schemas import register_schema
from pydtk.utils.imports import import_module_from_path

annotation = import_module_from_path(f"{os.path.dirname(__file__)}/annotation.py")


@register_schema
class ArbitraryAnnotation(annotation.Annotation):
    """Schema for an annotation with extra fields."""

    _api_version = "dataware-tools.com/v1alpha5"
    _kind = "ArbitraryAnnotation"

    class Config:
        """Configs for ArbitraryAnnotation."""

        extra = Extra.allow
