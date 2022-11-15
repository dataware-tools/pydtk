from typing import Dict
from pydantic import BaseModel, constr, Field

from .annotation import Annotation


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


class AnnotationCommentedPoint(Annotation):
    """Schema for commented point annotation."""

    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'AnnotationCommentedPoint'
    commented_point: CommentedPoint
