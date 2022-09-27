from typing import Optional
from pydtk.db.schemas import BaseSchema
from pydantic import constr, Field

class File(BaseSchema):
    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'File'
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")
    path: constr(min_length=1) = Field(..., description="")
    contents: Optional[dict] = Field(None, description="")
