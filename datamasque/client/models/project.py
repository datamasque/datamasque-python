"""Project models for the DataMasque API."""

from datetime import datetime
from enum import Enum
from typing import NewType, Optional, Union

from pydantic import BaseModel, ConfigDict

ProjectId = NewType("ProjectId", int)


def drop_namespace(cls: type, data: object) -> object:
    """
    Drops the deprecated `namespace` field that the server still returns on libraries.

    The models allow extra fields, so a kept `namespace` would be sent back on update,
    and the server lets `namespace` override `project`.
    """

    if isinstance(data, dict) and "namespace" in data:
        data = {key: value for key, value in data.items() if key != "namespace"}

    return data


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


def unwrap_project_id(project: Union[ProjectId, Project]) -> ProjectId:
    """Return the id of `project`, which a caller may give as a `Project` or as its `ProjectId`."""
    return project.id if isinstance(project, Project) else project


class ProjectCreateRequest(BaseModel):
    """
    Request body for `POST /api/projects/`.

    A name must not contain "/", ".." or a control character,
    and must differ, ignoring case, from the name of every other project, including deleted ones.
    The server trims leading and trailing whitespace from `name` and `description`.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""


class ProjectDeleteConflictReason(str, Enum):
    """Why a project's resources cannot move to the destination project when the project is deleted."""

    name_in_use = "name_in_use"
    """The destination project already contains a resource of the same type and name."""
    file_exists = "file_exists"
    """The destination project already has a file or directory where the resource's files would move to."""
    unexpected_file = "unexpected_file"
    """The project's directory contains a file or directory that belongs to none of its resources."""


class ProjectDeleteConflict(BaseModel):
    """A conflict that prevents a project's resources moving to the destination project when the project is deleted."""

    model_config = ConfigDict(extra="allow")

    reason: ProjectDeleteConflictReason
    detail: str
    """A message that describes the conflict."""
    type: Optional[str] = None
    """
    For `name_in_use` and `file_exists`: the type of the resource that would move, such as `connections` or `rulesets`.
    """
    id: Optional[str] = None
    """For `name_in_use` and `file_exists`: the ID of the resource that would move."""
    name: Optional[str] = None
    """For `name_in_use` and `file_exists`: the name of the resource that would move."""
    path: Optional[str] = None
    """For `file_exists` and `unexpected_file`: the path of the file or directory."""
