"""Pagination envelope models matching the DataMasque admin-server and IFM list-endpoint response shapes."""

from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    """Admin-server paginated response envelope."""

    model_config = ConfigDict(extra="allow")

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: list[T]


class IfmPage(BaseModel, Generic[T]):
    """IFM paginated response envelope."""

    model_config = ConfigDict(extra="allow")

    items: list[T]
    total: int
    limit: int
    offset: int
