import pytest

from datamasque.client import DataMasqueClient
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from datamasque.client.models.ruleset import Ruleset, RulesetType
from tests.helpers import database_connection_config, file_connection_config


@pytest.fixture
def config():
    return DataMasqueInstanceConfig(
        base_url="http://test-server",
        username="test_user",
        password="test_password",
    )


@pytest.fixture
def client(config):
    return DataMasqueClient(config)


@pytest.fixture
def connection_config(request):
    try:
        if request.param == "file":
            return file_connection_config()
    except AttributeError:
        pass

    return database_connection_config()


@pytest.fixture
def existing_connection_json():
    return {
        "id": "1",
        "name": "an_existing_connection",
        "mask_type": "database",
        "db_type": "mysql",
        "host": "my-host",
        "port": 1433,
        "database": "mydatabase",
        "user": "mysql-user",
    }


@pytest.fixture
def existing_rulesets_json():
    return [
        {
            "id": "1",
            "name": "db_masking_ruleset",
            "mask_type": "database",
            "config_yaml": "version: '1.0'",
            "is_valid": "valid",
        },
        {
            "id": "2",
            "name": "file_masking_ruleset",
            "mask_type": "file",
            "config_yaml": "version: '1.0'",
            "is_valid": "invalid",
        },
    ]


@pytest.fixture
def ruleset():
    return Ruleset(
        name="test_ruleset",
        yaml="version: '1.0'\ntasks: []",
        ruleset_type=RulesetType.database,
    )
