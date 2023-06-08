import os

from pydantic import BaseModel, Field, constr

from pydtk.db.schemas import register_schema
from pydtk.utils.imports import import_module_from_path

annotation = import_module_from_path(f"{os.path.dirname(__file__)}/annotation.py")


class ImagePixel(BaseModel):
    """Schema for image pixel."""

    x: int
    y: int


class CommentedImagePixel(BaseModel):
    """Schema for commented image pixel."""

    text: constr(min_length=1) = Field(..., description="")
    # NOTE(kan-bayashi): Sometimes frame_id in image message is empty.
    frame_id: constr(min_length=0) = Field(..., description="Cordinate ID.")
    target_topic: constr(min_length=1) = Field(..., description="Target topic to comment.")
    image_pixel: ImagePixel


@register_schema
class AnnotationCommentedImagePixel(annotation.Annotation):
    """Schema for commented image pixel annotation."""

    _api_version = "dataware-tools.com/v1alpha4"
    _kind = "AnnotationCommentedImagePixel"
    commented_image_pixel: CommentedImagePixel
