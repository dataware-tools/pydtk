from typing import Dict
from pydantic import BaseModel, constr, Field

from pydtk.db.schemas import BaseSchema, register_schema

try:
    # NOTE(kan-bayashi): absolute and relative path import does not work
    from annotation import Annotation
except ImportError:
    import sys
    import os

    sys.path.append(os.path.dirname(__file__))
    from annotation import Annotation


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
class AnnotationCommentedPoint(Annotation):
    """Schema for commented point annotation."""

    _api_version = "dataware-tools.com/v1alpha1"
    _kind = "AnnotationCommentedPoint"
    commented_point: CommentedPoint
