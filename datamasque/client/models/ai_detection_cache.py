"""Typed response shape for clearing the AI detection cache."""

from pydantic import BaseModel, ConfigDict


class AiDetectionCacheClearResult(BaseModel):
    """Result of `clear_ai_detection_cache`."""

    model_config = ConfigDict(extra="allow")

    freed_bytes: int
    """The disk space the clear freed, in bytes."""

    size_bytes: int
    """The size of the cache on disk after the clear, in bytes."""
