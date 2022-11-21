from typing import Optional

from pydantic import Field, constr

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class File(BaseSchema):
    """Schema for files."""

    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'File'
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")
    path: constr(min_length=1) = Field(..., description="")
    contents: Optional[dict] = Field(None, description="")
