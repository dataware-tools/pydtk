from pydantic import BaseModel, Field, constr

from pydtk.db.schemas import register_schema

try:
    # NOTE(kan-bayashi): absolute and relative path import does not work
    from annotation import Annotation
except ImportError:
    import os
    import sys

    sys.path.append(os.path.dirname(__file__))
    from annotation import Annotation


class ImageRectangularArea(BaseModel):
    """Schema for image rectangular area."""

    # TODO(kan-bayashi): Check frontend side
    # TODO(kan-bayashi): Add cordinate figure in docstrings
    center_x: int
    center_y: int
    size_x: int
    size_y: int


class CommentedImageRectangularArea(BaseModel):
    """Schema for commented image rectangular area."""

    text: constr(min_length=1) = Field(..., description="")
    # NOTE(kan-bayashi): Sometimes frame_id in image message is empty.
    frame_id: constr(min_length=0) = Field(..., description="Cordinate ID.")
    target_topic: constr(min_length=1) = Field(
        ..., description="Target topic to comment."
    )
    image_rectangular_area: ImageRectangularArea


@register_schema
class AnnotationCommentedImageRectanglerArea(Annotation):
    """Schema for commented image rectangular area annotation."""

    _api_version = "dataware-tools.com/v1alpha2"
    _kind = "AnnotationCommentedImageRectanglerArea"
    commented_image_rectangular_area: CommentedImageRectangularArea
