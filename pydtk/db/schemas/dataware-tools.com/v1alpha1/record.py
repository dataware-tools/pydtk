from typing import Optional
from pydtk.db.schemas import BaseSchema
from pydantic import constr, Field


class Record(BaseSchema):
    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'Record'
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")