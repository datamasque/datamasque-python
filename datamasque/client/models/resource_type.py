"""The types of resource that belong to a project."""

from enum import Enum


class ResourceTypeName(str, Enum):
    """A type of resource that belongs to a project, named as the resources listing groups it."""

    connections = "connections"
    connection_filesets = "connection_filesets"
    oracle_wallets = "oracle_wallets"
    snowflake_keys = "snowflake_keys"
    certificates = "certificates"
    table_references = "table_references"
    rulesets = "rulesets"
    ruleset_libraries = "ruleset_libraries"
    discovery_configs = "discovery_configs"
    discovery_config_libraries = "discovery_config_libraries"
    ruleset_generation_configs = "ruleset_generation_configs"
    seeds = "seeds"


RESOURCE_TYPE_NAME_VALUES = frozenset(type_name.value for type_name in ResourceTypeName)
"""Every `ResourceTypeName` value, to tell a type this client knows from one a newer server reports."""
