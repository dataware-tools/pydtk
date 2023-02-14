from typing import Optional, Dict, Any, Type

from pydantic import Field, constr, Extra

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class Record(BaseSchema):
    """Schema for records."""

    _api_version = "dataware-tools.com/v1alpha3"
    _kind = "Record"
    description: Optional[constr()] = Field(None, description="")
    record_id: constr(min_length=1) = Field(..., description="")

    # Allow additional properties
    class Config(BaseSchema.Config):
        """Config."""

        extra = Extra.allow

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['Record']):
            """Extra schema."""
            BaseSchema.Config.schema_extra(schema, model)
            schema["additionalProperties"] = {}
