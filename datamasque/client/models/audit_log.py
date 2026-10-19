"""Typed response shape for the audit log endpoint."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict


class AuditLogAction(str, Enum):
    """What the user did in an audited event."""

    created = "created"
    modified = "modified"
    deleted = "deleted"
    logged_in = "logged_in"
    logged_out = "logged_out"
    started = "started"
    cancelled = "cancelled"


class AuditLog(BaseModel):
    """One audit log entry."""

    model_config = ConfigDict(extra="allow")

    id: int
    timestamp: datetime
    username: str
    category: str
    """The area the event belongs to, such as `auth`, `run` or `settings`, or the kind of object it changed."""

    action: AuditLogAction
    description: str
    project: Optional[str] = None
    """
    The project the event belongs to, as `<id>-<slug>`,
    or `None` for an event outside any project or from a server older than 3.26.20.
    """

    project_name: Optional[str] = None
    """
    The project's name when the event was logged,
    or `None` for an event outside any project or from a server older than 3.26.20.
    """
