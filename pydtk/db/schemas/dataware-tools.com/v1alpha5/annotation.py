from typing import Union

from pydantic import Extra, Field, constr

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class Annotation(BaseSchema):
    """Schema for annotation."""

    _api_version = "dataware-tools.com/v1alpha5"
    _kind = "Annotation"
    annotation_id: constr(min_length=1) = Field(..., description="")
    generation: int
    record_id: constr(min_length=1) = Field(..., description="")
    timestamp_from: Union[float, None]
    timestamp_to: Union[float, None]
    created_at: Union[float, None]
    created_by: Union[str, None]


@register_schema
class ArbitraryAnnotation(Annotation):
    """Schema for an annotation with extra fields."""

    _api_version = "dataware-tools.com/v1alpha5"
    _kind = "ArbitraryAnnotation"

    class Config:
        """Configs for ArbitraryAnnotation."""
        extra = Extra.allow
