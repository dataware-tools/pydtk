from typing import Optional

from pydantic import Field, constr

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class Record(BaseSchema):
    """Schema for records."""

    _api_version = "dataware-tools.com/v1alpha1"
    _kind = "Record"
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")
