"""Resource listing models for the DataMasque API."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, field_validator

from datamasque.client.models.discovery_config import DiscoveryConfigType
from datamasque.client.models.resource_type import ResourceTypeName
from datamasque.client.models.runs import MaskType


class Resource(BaseModel):
    """
    A resource as the resources listing summarizes it, not the resource's full model.

    Connections, rulesets and discovery configs are summarized as the subclasses that add the field each carries.
    A resource in another's `uses` or `used_by` has this shape; `list_resources` returns a `ListedResource`.
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


# The model each type's resources are summarized as; a type not named here is summarized as `Resource`.
RESOURCE_MODEL_BY_TYPE_NAME: dict[ResourceTypeName, type[Resource]] = {
    ResourceTypeName.connections: ConnectionResource,
    ResourceTypeName.rulesets: RulesetResource,
    ResourceTypeName.discovery_configs: DiscoveryConfigResource,
}

# Resources grouped by type, as `uses` and `used_by` give them.
LinkedResources = dict[ResourceTypeName, list[Resource]]

ResourceT = TypeVar("ResourceT", bound=Resource)


def parse_resource_groups(
    groups: Mapping[str, Sequence[Mapping[str, object]]],
    model_by_type_name: Mapping[ResourceTypeName, type[ResourceT]],
    default_model: type[ResourceT],
) -> dict[ResourceTypeName, list[ResourceT]]:
    """
    Parse resources grouped by type name, each as the model `model_by_type_name` gives its type, else `default_model`.

    Raises:
        ValueError: a group's type name is not a `ResourceTypeName`.
    """
    parsed: dict[ResourceTypeName, list[ResourceT]] = {}
    for type_name, rows in groups.items():
        resource_type_name = ResourceTypeName(type_name)
        model = model_by_type_name.get(resource_type_name, default_model)
        parsed[resource_type_name] = [model.model_validate(row) for row in rows]

    return parsed


class ListedResource(Resource):
    """
    A resource that `list_resources` returns, with the resources it links to and the resources that link to it.

    The links are a connection's credentials, a table reference's connection, and the libraries a resource imports.
    Seed files and table references that a ruleset names in its YAML are not links.
    """

    uses: LinkedResources
    """The resources this one links to, by type. A type with no linked resources has no key."""
    used_by: LinkedResources
    """The resources that link to this one, by type. A type with no linked resources has no key."""

    @field_validator("uses", "used_by", mode="before")
    @classmethod
    def _parse_linked_resources(cls, groups: Mapping[str, Sequence[Mapping[str, object]]]) -> LinkedResources:
        return parse_resource_groups(groups, RESOURCE_MODEL_BY_TYPE_NAME, Resource)


class ListedConnectionResource(ConnectionResource, ListedResource):
    """A connection that `list_resources` returns."""


class ListedRulesetResource(RulesetResource, ListedResource):
    """A ruleset that `list_resources` returns."""


class ListedDiscoveryConfigResource(DiscoveryConfigResource, ListedResource):
    """A discovery config that `list_resources` returns."""


# The model `list_resources` returns for each type's resources; a type not named here is a `ListedResource`.
LISTED_RESOURCE_MODEL_BY_TYPE_NAME: dict[ResourceTypeName, type[ListedResource]] = {
    ResourceTypeName.connections: ListedConnectionResource,
    ResourceTypeName.rulesets: ListedRulesetResource,
    ResourceTypeName.discovery_configs: ListedDiscoveryConfigResource,
}

# Resources grouped by type, as `list_resources` returns them.
ListedResources = dict[ResourceTypeName, list[ListedResource]]

# Resources by type, each given as a `Resource` or its ID, such as `list_resources` returns or a resource's `uses`.
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
