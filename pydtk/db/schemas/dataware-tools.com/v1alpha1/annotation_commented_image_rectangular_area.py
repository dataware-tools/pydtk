from pydantic import BaseModel, constr, Field

from .annotation import Annotation


class ImageRectangularArea(BaseModel):
    """Schema for image rectangular area."""

    # TODO(kan-bayashi): Check frontend side
    center_x: int
    center_y: int
    size_x: int
    size_y: int


class CommentedImageRectangularArea(BaseModel):
    """Schema for commented image rectangular area."""

    text: constr(min_length=1) = Field(..., description="")
    # NOTE(kan-bayashi): Sometimes frame_id in image message is empty.
    frame_id: constr(min_length=0) = Field(..., description="Cordinate ID.")
    target_topic: constr(min_length=1) = Field(..., description="Target topic to comment.")
    image_rectangular_area: ImageRectangularArea


class AnnotationCommentedImageRectanglerArea(Annotation):
    """Schema for commented image rectangular area annotation."""

    _api_version = 'dataware-tools.com/v1alpha1'
    _kind = 'AnnotationCommentedImagePixel'
    commented_image_pixel: CommentedImageRectangularArea
