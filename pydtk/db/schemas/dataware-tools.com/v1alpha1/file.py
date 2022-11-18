from typing import Optional

from pydantic import Field, constr

from pydtk.db.schemas import BaseSchema


class File(BaseSchema):
    """Schema for files."""

    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'Files-test'
    description: Optional[constr()] = Field(None, description="")
    record_id_test: constr(min_length=1) = Field(..., description="")
    path: constr(min_length=1) = Field(..., description="")
    contents: Optional[dict] = Field(None, description="")
