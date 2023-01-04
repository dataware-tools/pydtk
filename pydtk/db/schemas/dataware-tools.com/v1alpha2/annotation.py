from pydantic import Field, constr
from typing import Union

from pydtk.db.schemas import BaseSchema, register_schema


@register_schema
class Annotation(BaseSchema):
    """Schema for annotation."""

    _api_version = "dataware-tools.com/v1alpha1"
    _kind = "Annotation"
    annotation_id: constr(min_length=1) = Field(..., description="")
    generation: int
    record_id: constr(min_length=1) = Field(..., description="")
    timestamp_from: Union[float, None]
    timestamp_to: Union[float, None]
    # TODO(watanabe): String?
    created_at: str

    # Note(WatanabeToshimitsu): workaround for make properties nullable
    #   See: https://github.com/pydantic/pydantic/issues/1270#issuecomment-734454493
    # This issue will be resolved in v2
    #   See: https://docs.pydantic.dev/blog/pydantic-v2/#required-vs-nullable-cleanup
    class Config:
        """Config."""

        @staticmethod
        def schema_extra(schema, model):
            """Extra schema."""
            for prop, value in schema.get("properties", {}).items():
                # retrieve right field from alias or name
                field = [x for x in model.__fields__.values() if x.alias == prop][0]
                if field.allow_none:
                    if "$ref" in value:
                        if issubclass(field.type_, BaseSchema):
                            # add 'title' in schema to have the exact same behaviour as the rest
                            value["title"] = (
                                field.type_.__config__.title or field.type_.__name__
                            )
                        value["anyOf"] = [{"$ref": value.pop("$ref")}]

                    value["nullable"] = True
