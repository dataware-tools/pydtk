from pydantic import Field, constr

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class Annotation(BaseSchema):
    """Schema for annotation."""

    _api_version = "dataware-tools.com/v1alpha1"
    _kind = "Annotation"
    annotation_id: constr(min_length=1) = Field(..., description="")
    generation: int
    record_id: constr(min_length=1) = Field(..., description="")
    timestamp_from: float
    timestamp_to: float
    # TODO(watanabe): String?
    created_at: str
