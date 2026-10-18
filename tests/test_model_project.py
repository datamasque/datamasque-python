"""Tests for the `project` field on resource and run models."""

from collections.abc import Callable
from typing import Union

import pytest

from datamasque.client.models.connection import ConnectionConfig, validate_connection
from datamasque.client.models.discovery_config import DiscoveryConfig
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary
from datamasque.client.models.rg_config import RGConfig
from datamasque.client.models.ruleset import Ruleset
from datamasque.client.models.ruleset_library import RulesetLibrary
from datamasque.client.models.runs import RunInfo, UnfinishedRun
from datamasque.client.models.table_reference import TableReference
from tests.helpers import PROJECT

ResourceModel = Union[
    ConnectionConfig, Ruleset, RulesetLibrary, DiscoveryConfig, DiscoveryConfigLibrary, RGConfig, TableReference
]
Payload = dict[str, object]
ParseResource = Callable[[Payload], ResourceModel]

_CONNECTION_PAYLOAD: Payload = {
    "id": "db-1",
    "name": "pg",
    "mask_type": "database",
    "db_type": "postgres",
    "host": "h",
    "port": 5432,
    "database": "d",
    "user": "u",
    "schema": "public",
}

_RESOURCE_CASES = [
    pytest.param(validate_connection, _CONNECTION_PAYLOAD, id="connection"),
    pytest.param(
        Ruleset.model_validate, {"id": "r-1", "name": "r", "config_yaml": "", "mask_type": "database"}, id="ruleset"
    ),
    pytest.param(RulesetLibrary.model_validate, {"id": "rl-1", "name": "rl"}, id="ruleset_library"),
    pytest.param(
        DiscoveryConfig.model_validate, {"id": "dc-1", "name": "dc", "config_type": "database"}, id="discovery_config"
    ),
    pytest.param(DiscoveryConfigLibrary.model_validate, {"id": "dcl-1", "name": "dcl"}, id="discovery_config_library"),
    pytest.param(RGConfig.model_validate, {"id": "rg-1", "name": "rg"}, id="rg_config"),
    pytest.param(
        TableReference.model_validate,
        {"id": "tr-1", "name": "tr", "connection": "db-1", "source": "public.people"},
        id="table_reference",
    ),
]


@pytest.mark.parametrize(("parse", "payload"), _RESOURCE_CASES)
def test_resource_parses_project_and_sends_it_when_serializing(parse: ParseResource, payload: Payload) -> None:
    model = parse({**payload, "project": PROJECT})

    assert model.project == PROJECT
    assert "project" not in (model.model_extra or {})
    assert model.model_dump(by_alias=True, mode="json")["project"] == PROJECT


@pytest.mark.parametrize(("parse", "payload"), _RESOURCE_CASES)
def test_resource_project_is_optional_and_not_sent_when_unset(parse: ParseResource, payload: Payload) -> None:
    model = parse(payload)

    assert model.project is None
    assert "project" not in model.model_dump(by_alias=True, exclude_none=True, mode="json")


@pytest.mark.parametrize("library_class", [RulesetLibrary, DiscoveryConfigLibrary])
def test_library_drops_namespace(library_class: type[Union[RulesetLibrary, DiscoveryConfigLibrary]]) -> None:
    """The server still returns the deprecated `namespace`, which would override `project` if it were sent back."""
    library = library_class.model_validate({"id": "l-1", "name": "l", "project": PROJECT, "namespace": "org"})

    assert "namespace" not in (library.model_extra or {})
    assert "namespace" not in library.model_dump(by_alias=True, mode="json")


_RUN_PAYLOAD: Payload = {
    "id": 21,
    "status": "running",
    "mask_type": "database",
    "source_connection": "db-1",
    "source_connection_name": "pg",
    "ruleset_name": "r",
}


@pytest.mark.parametrize("run_class", [RunInfo, UnfinishedRun])
def test_run_parses_project(run_class: type[Union[RunInfo, UnfinishedRun]]) -> None:
    run = run_class.model_validate({**_RUN_PAYLOAD, "project": PROJECT})

    assert run.project == PROJECT
    assert "project" not in (run.model_extra or {})
