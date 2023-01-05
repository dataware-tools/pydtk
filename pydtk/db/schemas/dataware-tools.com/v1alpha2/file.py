from typing import Optional, Dict, Any, Type

from pydantic import Field, constr, Extra

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class File(BaseSchema):
    """Schema for files."""

    _api_version = "dataware-tools.com/v1alpha2"
    _kind = "File"
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")
    path: constr(min_length=1) = Field(..., description="")
    contents: Optional[dict] = Field(None, description="")

    # Allow additional properties
    class Config(BaseSchema.Config):
        """Config."""

        extra = Extra.allow

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['File']):
            """Extra schema."""
            BaseSchema.Config.schema_extra(schema, model)
            schema["additionalProperties"] = {}
