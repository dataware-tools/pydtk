import os
from typing import Optional

from pydantic import BaseModel, Field, constr

from pydtk.db.schemas import register_schema
from pydtk.utils.imports import import_module_from_path

annotation = import_module_from_path(f"{os.path.dirname(__file__)}/annotation.py")


class ImageRectangularArea(BaseModel):
    """Schema for image rectangular area."""

    # TODO(kan-bayashi): Add cordinate figure in docstrings
    center_x: float
    center_y: float
    size_x: float
    size_y: float


class CommentedImageRectangularArea(BaseModel):
    """Schema for commented image rectangular area."""

    text: constr(min_length=1) = Field(..., description="")
    # NOTE(kan-bayashi): Sometimes frame_id in image message is empty.
    frame_id: constr(min_length=0) = Field(..., description="Cordinate ID.")
    target_topic: constr(min_length=1) = Field(..., description="Target topic to comment.")
    image_rectangular_area: ImageRectangularArea


@register_schema
class AnnotationCommentedImageRectangularArea(annotation.Annotation):
    """Schema for commented image rectangular area annotation."""

    _api_version: Optional[str] = "dataware-tools.com/v1alpha5"
    _kind: Optional[str] = "AnnotationCommentedImageRectangularArea"
    commented_image_rectangular_area: CommentedImageRectangularArea
