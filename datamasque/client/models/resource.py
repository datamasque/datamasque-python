"""Resource listing models for the DataMasque API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict

from datamasque.client.models.discovery_config import DiscoveryConfigType
from datamasque.client.models.runs import MaskType


class ResourceTypeName(str, Enum):
    """A type of resource that belongs to a project, named as the resources listing groups it."""

    connections = "connections"
    connection_filesets = "connection_filesets"
    oracle_wallets = "oracle_wallets"
    snowflake_keys = "snowflake_keys"
    table_references = "table_references"
    rulesets = "rulesets"
    ruleset_libraries = "ruleset_libraries"
    discovery_configs = "discovery_configs"
    discovery_config_libraries = "discovery_config_libraries"
    ruleset_generation_configs = "ruleset_generation_configs"
    seeds = "seeds"


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
