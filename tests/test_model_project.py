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
def test_resource_parses_project_and_excludes_it_when_serializing(parse: ParseResource, payload: Payload) -> None:
    model = parse({**payload, "project": "1-default"})

    assert model.project == "1-default"
    assert "project" not in (model.model_extra or {})
    assert "project" not in model.model_dump(by_alias=True, mode="json")


@pytest.mark.parametrize(("parse", "payload"), _RESOURCE_CASES)
def test_resource_project_defaults_to_none_when_absent(parse: ParseResource, payload: Payload) -> None:
    assert parse(payload).project is None


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
    run = run_class.model_validate({**_RUN_PAYLOAD, "project": "1-default"})

    assert run.project == "1-default"
    assert "project" not in (run.model_extra or {})
