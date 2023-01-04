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


class ImagePixel(BaseModel):
    """Schema for image pixel."""

    x: int
    y: int


class CommentedImagePixel(BaseModel):
    """Schema for commented image pixel."""

    text: constr(min_length=1) = Field(..., description="")
    # NOTE(kan-bayashi): Sometimes frame_id in image message is empty.
    frame_id: constr(min_length=0) = Field(..., description="Cordinate ID.")
    target_topic: constr(min_length=1) = Field(
        ..., description="Target topic to comment."
    )
    image_pixel: ImagePixel


@register_schema
class AnnotationCommentedImagePixel(Annotation):
    """Schema for commented image pixel annotation."""

    _api_version = "dataware-tools.com/v1alpha1"
    _kind = "AnnotationCommentedImagePixel"
    commented_image_pixel: CommentedImagePixel
