import os

from pydantic import BaseModel, Field, constr

from pydtk.db.schemas import register_schema
from pydtk.utils.imports import import_module_from_path

annotation = import_module_from_path(f"{os.path.dirname(__file__)}/annotation.py")


class Point(BaseModel):
    """Schema for point."""

    x: float
    y: float
    z: float


class CommentedPoint(BaseModel):
    """Schema for commented point."""

    text: constr(min_length=1) = Field(..., description="")
    frame_id: constr(min_length=1) = Field(..., description="Cordinate ID.")
    point: Point


@register_schema
class AnnotationCommentedPoint(annotation.Annotation):
    """Schema for commented point annotation."""

    _api_version = "dataware-tools.com/v1alpha4"
    _kind = "AnnotationCommentedPoint"
    commented_point: CommentedPoint
