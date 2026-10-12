"""Project models for the DataMasque API."""

from datetime import datetime
from typing import NewType

from pydantic import BaseModel, ConfigDict

ProjectId = NewType("ProjectId", int)


class Project(BaseModel):
    """A project: the top-level grouping that connections, rulesets and other resources belong to."""

    model_config = ConfigDict(extra="allow")

    id: ProjectId
    reference: str
    """The `<id>-<slug>` string other resources use to refer to this project."""
    name: str
    description: str = ""
    created: datetime
    updated: datetime


class ProjectCreateRequest(BaseModel):
    """
    Request body for `POST /api/projects/`.

    Names are unique across active and archived projects, ignoring case.
    The server trims leading and trailing whitespace from `name` and `description`,
    and rejects a name that contains "/", ".." or a control character.
    It may reject other names too,
    and its 400 response gives the reason.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
