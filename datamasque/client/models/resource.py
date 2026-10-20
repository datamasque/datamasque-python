"""Resource listing models for the DataMasque API."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict

from datamasque.client.models.discovery_config import DiscoveryConfigType
from datamasque.client.models.resource_type import ResourceTypeName
from datamasque.client.models.runs import MaskType


class Resource(BaseModel):
    """
    One resource in the resources listing: the summary the listing gives, not the resource's full model.

    Connections, rulesets and discovery configs are listed as the subclasses that add the field each carries.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    name: str
    project: Optional[str] = None
    """Its project's `<id>-<slug>` reference, or `None` for a resource in no project, such as a built-in seed."""
    created: Optional[datetime] = None
    """`None` for a type that does not record it, currently connections."""
    modified: Optional[datetime] = None
    """`None` for a type that does not record it, currently connections."""


class ConnectionResource(Resource):
    """A connection in the resources listing."""

    mask_type: MaskType


class RulesetResource(Resource):
    """A ruleset in the resources listing."""

    mask_type: MaskType


class DiscoveryConfigResource(Resource):
    """A discovery config in the resources listing."""

    config_type: DiscoveryConfigType


# The model each type's rows are listed as; a type not named here is listed as `Resource`.
RESOURCE_MODEL_BY_TYPE_NAME: dict[str, type[Resource]] = {
    ResourceTypeName.connections.value: ConnectionResource,
    ResourceTypeName.rulesets.value: RulesetResource,
    ResourceTypeName.discovery_configs.value: DiscoveryConfigResource,
}

# Resources by type, each given as a listed `Resource` or its ID, in the shape `list_resources` returns.
ResourcesByType = Mapping[ResourceTypeName, Sequence[Union[Resource, str]]]


class ResourceMoveConflictReason(str, Enum):
    """Why a resource cannot move to the destination project."""

    already_in_destination = "already_in_destination"
    """The resource is already in the destination project."""
    name_in_use = "name_in_use"
    """The destination project already contains a resource of the same type and name."""
    name_in_selection = "name_in_selection"
    """Another resource of the same type and name is moving too, from another project."""
    file_exists = "file_exists"
    """The destination project already has a file or directory where the resource's files would move to."""


class ResourceMoveConflict(BaseModel):
    """A conflict that keeps a resource from moving to the destination project."""

    model_config = ConfigDict(extra="allow")

    reason: ResourceMoveConflictReason
    detail: str
    """A message that describes the conflict."""
    type: ResourceTypeName
    id: str
    name: str
    path: Optional[str] = None
    """For `file_exists`: the path of the file or directory, relative to the DataMasque files directory."""


class ResourceMoveResult(BaseModel):
    """The outcome of `move_resources`: every resource asked for either moved or is listed in `conflicts`."""

    model_config = ConfigDict(extra="allow")

    moved: int
    """The number of resources that moved."""
    conflicts: list[ResourceMoveConflict]
    """The resources that did not move, and why."""
