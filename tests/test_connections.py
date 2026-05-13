"""Tests for `ConnectionClient` (CRUD + Snowflake-specific behaviour)."""

import pytest
import requests_mock

from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
from datamasque.client.models.connection import (
    AzureConnectionConfig,
    ConnectionId,
    DatabaseConnectionConfig,
    DatabaseType,
    DatabricksDeltaS3ConnectionConfig,
    DynamoConnectionConfig,
    MongoConnectionConfig,
    MountedShareConnectionConfig,
    MssqlLinkedServerConnectionConfig,
    S3ConnectionConfig,
    SnowflakeConnectionConfig,
    SnowflakeStageLocation,
    SseConfig,
    SseSelection,
    validate_connection,
)
from tests.helpers import (
    sample_mounted_share_connection_json,
    snowflake_connection_config_azure,
    snowflake_connection_config_local,
    snowflake_connection_config_s3,
)


@pytest.mark.parametrize("connection_config", ["database", "file"], indirect=True)
def test_create_or_update_connection(client, connection_config):
    """Create a new connection, the test is parameterized to run with both a db and file connection."""
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/connections/", json=[], status_code=200)  # no existing connections
        m.post("http://test-server/api/connections/", json={"id": "2"}, status_code=201)

        result = client.create_or_update_connection(connection_config)
        assert result.id == ConnectionId("2")


@pytest.mark.parametrize("connection_config", ["database"], indirect=True)
@pytest.mark.parametrize("engine_options", [None, {}, {"pool_size": 5}])
def test_create_or_update_connection_engine_options(client, connection_config, engine_options):
    """Create a new connection with engine options. They should only be passed on the API if truthy."""
    connection_config.engine_options = engine_options
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/connections/", json=[], status_code=200)  # no existing connections
        m.post("http://test-server/api/connections/", json={"id": "2"}, status_code=201)

        result = client.create_or_update_connection(connection_config)
        assert result.id == ConnectionId("2")

        request_body = m.last_request.json()
        if engine_options:
            assert request_body["engine_options"] == engine_options
        else:
            assert "engine_options" not in request_body


def test_create_or_update_connection_update(client, connection_config):
    """Update a connection."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[
                connection_config.model_dump(exclude_none=True, by_alias=True, mode="json")
                | {"id": "1", "mask_type": "database"}
            ],
            status_code=200,
        )
        m.put("http://test-server/api/connections/1/", json={"id": "1"}, status_code=200)
        result = client.create_or_update_connection(connection_config)
        assert result.id == ConnectionId("1")


def test_create_or_update_connection_create_fail(client, connection_config, existing_connection_json):
    """Fail to create a connection."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[existing_connection_json],
            status_code=200,
        )
        m.post("http://test-server/api/connections/", status_code=400)

        with pytest.raises(DataMasqueApiError):
            client.create_or_update_connection(connection_config)


def test_list_connnections(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[
                # Realistic API data, hence why it's so verbose
                {
                    "name": "s3",
                    "bucket": "my-s3-bucket",
                    "base_directory": "",
                    "type": "s3_connection",
                    "mask_type": "file",
                    "version": "1.0",
                    "id": "88dabb63-aca5-4cc4-8f76-f78736a42f39",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "is_file_mask_source": True,
                    "is_file_mask_destination": False,
                },
                {
                    "name": "azure",
                    "type": "azure_blob_connection",
                    "base_directory": "",
                    "mask_type": "file",
                    "container": "mycontainer",
                    "version": "1.0",
                    "connection_string_encrypted": "some_base64_here",
                    "id": "490502e5-5bf6-4abb-b67b-c6091d40ecf0",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "is_file_mask_source": True,
                    "is_file_mask_destination": True,
                },
                {
                    "name": "mounted_share_dest",
                    "type": "mounted_share_connection",
                    "base_directory": "dest",
                    "mask_type": "file",
                    "version": "1.0",
                    "id": "7ba07e3d-f917-4bee-bfc0-c42b9b01a06e",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "is_file_mask_source": False,
                    "is_file_mask_destination": True,
                },
                {
                    "version": "1.0",
                    "host": "my-mysql-host",
                    "port": 3306,
                    "user": "me",
                    "db_type": "mysql",
                    "database": "mydatabase",
                    "name": "mysql",
                    "schema": "",
                    "is_read_only": False,
                    "password_encrypted": "some_base64_here",
                    "id": "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "mask_type": "database",
                    "is_file_mask_source": False,
                    "is_file_mask_destination": False,
                },
                {
                    "version": "1.0",
                    "mask_type": "database",
                    "name": "db_dynamo",
                    "s3_bucket_name": "my-dynamo-staging-bucket",
                    "dynamo_append_datetime": False,
                    "dynamo_append_suffix": "-masked",
                    "dynamo_replace_tables": True,
                    "dynamo_default_region": None,
                    "dynamo_default_sse": {
                        "selection": "account_managed",
                        "kms_key_id": "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
                    },
                    "db_type": "dynamo_db",
                    "host": "",
                    "port": None,
                    "user": "",
                    "password": "",
                    "database": "",
                    "schema": "",
                    "id": "d7257552-0485-4806-b0fb-d72b4d268073",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "is_file_mask_source": False,
                    "is_file_mask_destination": False,
                },
                {
                    "version": "1.0",
                    "host": "mssql-linked-host",
                    "port": 3306,
                    "user": "mine",
                    "db_type": "mssql_linked",
                    "database": "database_name",
                    "name": "mssql-linked",
                    "schema": "",
                    "is_read_only": False,
                    "password_encrypted": "some_base64_here",
                    "id": "48a7af45-f63f-4e05-bf9f-7b1cc3a0e89d",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "mask_type": "database",
                    "linked_server": "name.database.schema",
                    "is_file_mask_source": False,
                    "is_file_mask_destination": False,
                },
                {
                    "version": "1.0",
                    "mask_type": "database",
                    "name": "db_dynamo_2",
                    "s3_bucket_name": "my-dynamo-staging-bucket-2",
                    "dynamo_append_datetime": False,
                    "dynamo_append_suffix": "-masked",
                    "dynamo_replace_tables": True,
                    "dynamo_default_region": None,
                    "db_type": "dynamo_db",
                    "host": "",
                    "port": None,
                    "user": "",
                    "password": "",
                    "database": "",
                    "schema": "",
                    "id": "d7257552-0485-4806-b0fb-d72b4d123456",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "is_file_mask_source": False,
                    "is_file_mask_destination": False,
                },
                {
                    "version": "1.0",
                    "user": "snowman",
                    "db_type": "snowflake",
                    "database": "icicle",
                    "name": "snowflake",
                    "schema": "snowball",
                    "snowflake_role": "snowballs do indeed roll",
                    "snowflake_account_id": "ABCDEF-123456",
                    "snowflake_warehouse": "warehouse1",
                    "snowflake_storage_integration_name": "mysi",
                    "host": "snowflake.com",
                    "port": 443,
                    "s3_bucket_name": "ice-bucket",
                    "iam_role_arn": "swiss roll",
                    "is_read_only": False,
                    "password_encrypted": "some_base64_here",
                    "id": "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7",
                    "oracle_wallet": None,
                    "connection_fileset": None,
                    "mask_type": "database",
                    "is_file_mask_source": False,
                    "is_file_mask_destination": False,
                },
                {
                    "version": "1.0",
                    "user": "frosty",
                    "db_type": "snowflake",
                    "database": "igloo",
                    "name": "snowflake_minimal_with_key",
                    "snowflake_account_id": "ACCOUNT-1234",
                    "snowflake_warehouse": "clothing_store",
                    "snowflake_storage_integration_name": "kennards",
                    "s3_bucket_name": "champagne-bucket",
                    "snowflake_private_key": "2831289a-4398-abcd-4112-fe09a1239f89",
                    "snowflake_private_key_passphrase_encrypted": "some base64 here",
                    "id": "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7",
                    "mask_type": "database",
                },
            ],
            status_code=200,
        )
        connections = client.list_connections()
        assert len(connections) == 9

        s3_connection = connections[0]
        assert isinstance(s3_connection, S3ConnectionConfig)
        assert s3_connection.id == "88dabb63-aca5-4cc4-8f76-f78736a42f39"
        assert s3_connection.bucket == "my-s3-bucket"
        assert s3_connection.base_directory == ""

        azure_connection = connections[1]
        assert isinstance(azure_connection, AzureConnectionConfig)
        assert azure_connection.container == "mycontainer"
        assert azure_connection.connection_string is None
        assert azure_connection.is_file_mask_source is True
        assert azure_connection.is_file_mask_destination is True

        mounted_share_connection = connections[2]
        assert isinstance(mounted_share_connection, MountedShareConnectionConfig)
        assert mounted_share_connection.is_file_mask_source is False
        assert mounted_share_connection.is_file_mask_destination is True
        assert mounted_share_connection.base_directory == "dest"

        database_connection = connections[3]
        assert isinstance(database_connection, DatabaseConnectionConfig)
        assert database_connection.database_type is DatabaseType.mysql
        assert database_connection.database == "mydatabase"
        assert database_connection.user == "me"
        assert database_connection.password is None
        assert database_connection.db_schema is None

        dynamo_connection = connections[4]
        assert isinstance(dynamo_connection, DynamoConnectionConfig)
        assert dynamo_connection.s3_bucket_name == "my-dynamo-staging-bucket"
        assert dynamo_connection.dynamo_append_datetime is False
        assert dynamo_connection.dynamo_replace_tables is True
        assert dynamo_connection.dynamo_append_suffix == "-masked"
        assert dynamo_connection.dynamo_default_sse == SseConfig(
            selection=SseSelection.account_managed,
            kms_key_id="arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
        )

        mssql_linked_connection = connections[5]
        assert isinstance(mssql_linked_connection, MssqlLinkedServerConnectionConfig)
        assert mssql_linked_connection.database_type is DatabaseType.mssql_linked
        assert mssql_linked_connection.database == "database_name"
        assert mssql_linked_connection.user == "mine"
        assert mssql_linked_connection.password is None
        assert mssql_linked_connection.db_schema == ""
        assert mssql_linked_connection.linked_server == "name.database.schema"

        dynamo_connection = connections[6]
        assert isinstance(dynamo_connection, DynamoConnectionConfig)
        assert dynamo_connection.s3_bucket_name == "my-dynamo-staging-bucket-2"
        assert dynamo_connection.dynamo_append_datetime is False
        assert dynamo_connection.dynamo_replace_tables is True
        assert dynamo_connection.dynamo_append_suffix == "-masked"
        assert dynamo_connection.dynamo_default_sse == SseConfig(
            selection=SseSelection.dynamodb_owned,
            kms_key_id=None,
        )

        snowflake_connection = connections[7]
        assert isinstance(snowflake_connection, SnowflakeConnectionConfig)
        assert snowflake_connection.database_type is DatabaseType.snowflake
        assert snowflake_connection.database == "icicle"
        assert snowflake_connection.db_schema == "snowball"
        assert snowflake_connection.host == "snowflake.com"
        assert snowflake_connection.port == 443
        assert snowflake_connection.user == "snowman"
        assert snowflake_connection.snowflake_role == "snowballs do indeed roll"
        assert snowflake_connection.snowflake_account_id == "ABCDEF-123456"
        assert snowflake_connection.snowflake_warehouse == "warehouse1"
        assert snowflake_connection.snowflake_storage_integration_name == "mysi"
        assert snowflake_connection.s3_bucket_name == "ice-bucket"
        assert snowflake_connection.iam_role_arn == "swiss roll"
        assert snowflake_connection.is_read_only is False

        minimal_snowflake_connection = connections[8]
        assert isinstance(minimal_snowflake_connection, SnowflakeConnectionConfig)
        assert minimal_snowflake_connection.database_type is DatabaseType.snowflake
        assert minimal_snowflake_connection.database == "igloo"
        assert minimal_snowflake_connection.db_schema is None
        assert minimal_snowflake_connection.host == ""
        assert minimal_snowflake_connection.port is None
        assert minimal_snowflake_connection.user == "frosty"
        assert minimal_snowflake_connection.snowflake_role == ""
        assert minimal_snowflake_connection.snowflake_account_id == "ACCOUNT-1234"
        assert minimal_snowflake_connection.snowflake_warehouse == "clothing_store"
        assert minimal_snowflake_connection.snowflake_storage_integration_name == "kennards"
        assert minimal_snowflake_connection.s3_bucket_name == "champagne-bucket"
        assert minimal_snowflake_connection.iam_role_arn is None
        assert minimal_snowflake_connection.is_read_only is False
        assert minimal_snowflake_connection.snowflake_private_key == "2831289a-4398-abcd-4112-fe09a1239f89"


def test_delete_connection_by_id(client):
    connection_id = ConnectionId("f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7")
    with requests_mock.Mocker() as m:
        m.delete(f"http://test-server/api/connections/{connection_id}/", status_code=200)
        client.delete_connection_by_id_if_exists(connection_id)


def test_delete_connection_by_name(client):
    connection_name = "my-connection"
    connection_id = "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7"
    connection_with_same_name_id = "abcd1234-1234-5678-90ab-cdefcdefcdef"
    other_connection_id = "bcbcbcbc-5454-6565-7676-123412341234"
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[
                sample_mounted_share_connection_json(name=connection_name, id=connection_id),
                sample_mounted_share_connection_json(name="some_other_connection", id=other_connection_id),
                # There shouldn't ever be two connections with the same name, but we check both are deleted
                sample_mounted_share_connection_json(name=connection_name, id=connection_with_same_name_id),
            ],
            status_code=200,
        )
        m.delete(f"http://test-server/api/connections/{connection_id}/", status_code=200)
        m.delete(
            f"http://test-server/api/connections/{connection_with_same_name_id}/",
            status_code=200,
        )

        client.delete_connection_by_name_if_exists(connection_name)

    assert m.call_count == 3
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"
    assert m.request_history[2].method == "DELETE"


def test_delete_connection_that_does_not_exist(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[
                sample_mounted_share_connection_json(
                    name="not_this_connection",
                    id="abcd1234-1234-5678-90ab-cdefcdefcdef",
                ),
                sample_mounted_share_connection_json(
                    name="not_this_connection_either",
                    id="bcbcbcbc-5454-6565-7676-123412341234",
                ),
            ],
            status_code=200,
        )
        client.delete_connection_by_name_if_exists("my_connection")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


@pytest.mark.parametrize(
    "config_func,expected_stage_location,expected_fields,unexpected_fields",
    [
        (
            snowflake_connection_config_s3,
            "aws_s3",
            ["s3_bucket_name", "iam_role_arn"],
            ["snowflake_azure_container_name", "snowflake_azure_connection_string"],
        ),
        (
            snowflake_connection_config_azure,
            "azure_blob_storage",
            ["snowflake_azure_container_name", "snowflake_azure_connection_string"],
            ["s3_bucket_name", "iam_role_arn"],
        ),
        (
            snowflake_connection_config_local,
            "local",
            [],
            [
                "s3_bucket_name",
                "iam_role_arn",
                "snowflake_azure_container_name",
                "snowflake_azure_connection_string",
            ],
        ),
    ],
)
def test_create_snowflake_connection_with_staging_platform(
    client, config_func, expected_stage_location, expected_fields, unexpected_fields
):
    """Test creating Snowflake connections with different staging platforms."""
    config = config_func()

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/connections/", json=[], status_code=200)
        m.post("http://test-server/api/connections/", json={"id": "2"}, status_code=201)

        result = client.create_or_update_connection(config)
        assert result.id == ConnectionId("2")

        # Verify the correct data was sent
        request_data = m.last_request.json()
        assert request_data["snowflake_stage_location"] == expected_stage_location

        # Check expected fields are present
        for field in expected_fields:
            assert field in request_data

        # Check unexpected fields are not present
        for field in unexpected_fields:
            assert field not in request_data


@pytest.mark.parametrize(
    "config_func,expected_stage_location,expected_fields,unexpected_fields",
    [
        (
            snowflake_connection_config_s3,
            SnowflakeStageLocation.aws_s3,
            {
                "s3_bucket_name": "test-bucket",
                "iam_role_arn": "arn:aws:iam::123456789012:role/test-role",
            },
            ["snowflake_azure_container_name", "snowflake_azure_connection_string"],
        ),
        (
            snowflake_connection_config_azure,
            SnowflakeStageLocation.azure_blob_storage,
            {
                "snowflake_azure_container_name": "test-container",
                "snowflake_azure_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test",
            },
            ["s3_bucket_name", "iam_role_arn"],
        ),
        (
            snowflake_connection_config_local,
            SnowflakeStageLocation.local,
            {},
            [
                "s3_bucket_name",
                "iam_role_arn",
                "snowflake_azure_container_name",
                "snowflake_azure_connection_string",
            ],
        ),
    ],
)
def test_snowflake_connection_model_dump(config_func, expected_stage_location, expected_fields, unexpected_fields):
    """Test that Snowflake connections serialize correctly for each staging platform."""
    config = config_func()
    api_dict = config.model_dump(exclude_none=True, by_alias=True, mode="json")

    assert api_dict["snowflake_stage_location"] == expected_stage_location

    # Check expected fields and their values
    for field, value in expected_fields.items():
        assert api_dict[field] == value

    # Check unexpected fields are not present
    for field in unexpected_fields:
        assert field not in api_dict


def test_list_snowflake_connections_with_different_platforms(client):
    """Test listing Snowflake connections returns correct staging platform information."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/connections/",
            json=[
                {
                    "version": "1.0",
                    "user": "s3_user",
                    "db_type": "snowflake",
                    "database": "s3_db",
                    "name": "snowflake_s3",
                    "snowflake_account_id": "S3-ACCOUNT",
                    "snowflake_warehouse": "s3_warehouse",
                    "snowflake_storage_integration_name": "s3_integration",
                    "s3_bucket_name": "s3-bucket",
                    "iam_role_arn": "arn:aws:iam::123456789012:role/s3-role",
                    "snowflake_stage_location": "aws_s3",
                    "password_encrypted": "encrypted",
                    "id": "s3-connection-id",
                    "mask_type": "database",
                },
                {
                    "version": "1.0",
                    "user": "azure_user",
                    "db_type": "snowflake",
                    "database": "azure_db",
                    "name": "snowflake_azure",
                    "snowflake_account_id": "AZURE-ACCOUNT",
                    "snowflake_warehouse": "azure_warehouse",
                    "snowflake_storage_integration_name": "azure_integration",
                    "snowflake_azure_container_name": "azure-container",
                    "snowflake_azure_connection_string_encrypted": "encrypted_azure_string",
                    "snowflake_stage_location": "azure_blob_storage",
                    "password_encrypted": "encrypted",
                    "id": "azure-connection-id",
                    "mask_type": "database",
                },
                {
                    "version": "1.0",
                    "user": "local_user",
                    "db_type": "snowflake",
                    "database": "local_db",
                    "name": "snowflake_local",
                    "snowflake_account_id": "LOCAL-ACCOUNT",
                    "snowflake_warehouse": "local_warehouse",
                    "snowflake_storage_integration_name": "local_integration",
                    "snowflake_stage_location": "local",
                    "password_encrypted": "encrypted",
                    "id": "local-connection-id",
                    "mask_type": "database",
                },
            ],
            status_code=200,
        )

        connections = client.list_connections()
        snowflake_connections = [c for c in connections if isinstance(c, SnowflakeConnectionConfig)]
        assert len(snowflake_connections) == 3

        # Check S3 connection
        s3_conn = next(c for c in snowflake_connections if c.name == "snowflake_s3")
        assert s3_conn.snowflake_stage_location is SnowflakeStageLocation.aws_s3
        assert s3_conn.s3_bucket_name == "s3-bucket"
        assert s3_conn.iam_role_arn == "arn:aws:iam::123456789012:role/s3-role"
        assert s3_conn.snowflake_azure_container_name is None
        assert s3_conn.snowflake_azure_connection_string is None

        # Check Azure connection
        azure_conn = next(c for c in snowflake_connections if c.name == "snowflake_azure")
        assert azure_conn.snowflake_stage_location is SnowflakeStageLocation.azure_blob_storage
        assert azure_conn.snowflake_azure_container_name == "azure-container"
        assert azure_conn.snowflake_azure_connection_string is None  # Encrypted, so empty
        assert azure_conn.s3_bucket_name is None
        assert azure_conn.iam_role_arn is None

        # Check local connection
        local_conn = next(c for c in snowflake_connections if c.name == "snowflake_local")
        assert local_conn.snowflake_stage_location is SnowflakeStageLocation.local
        assert local_conn.s3_bucket_name is None
        assert local_conn.iam_role_arn is None
        assert local_conn.snowflake_azure_container_name is None
        assert local_conn.snowflake_azure_connection_string is None


@pytest.mark.parametrize(
    "stage_location,missing_fields,error_message",
    [
        (
            SnowflakeStageLocation.azure_blob_storage,
            {
                "snowflake_azure_container_name": None,
                "snowflake_azure_connection_string": None,
            },
            "Missing Azure fields",
        ),
        (
            SnowflakeStageLocation.aws_s3,
            {
                "s3_bucket_name": None,
                "iam_role_arn": None,  # IAM role is optional, so only s3_bucket_name is truly required
            },
            "Missing S3 bucket",
        ),
    ],
)
def test_create_snowflake_connection_missing_required_fields(client, stage_location, missing_fields, error_message):
    """Test that creating a Snowflake connection with missing required fields fails appropriately."""
    config_dict = {
        "name": f"invalid_{stage_location.value}",
        "database": "test_db",
        "user": "snowflake_user",
        "snowflake_account_id": "ACCOUNT-123",
        "snowflake_warehouse": "test_warehouse",
        "snowflake_storage_integration_name": "test_integration",
        "password": "test_password",
        "snowflake_stage_location": stage_location,
    }

    # Add the missing fields
    config_dict.update(missing_fields)

    config = SnowflakeConnectionConfig(**config_dict)

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/connections/", json=[], status_code=200)
        m.post(
            "http://test-server/api/connections/",
            json={"error": error_message},
            status_code=400,
        )

        with pytest.raises(DataMasqueApiError):
            client.create_or_update_connection(config)


def test_s3_connection_model_validate():
    payload = {
        "id": "88dabb63-aca5-4cc4-8f76-f78736a42f39",
        "name": "s3",
        "mask_type": "file",
        "type": "s3_connection",
        "base_directory": "data/",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
        "bucket": "my-bucket",
        "iam_role_arn": "arn:aws:iam::111122223333:role/s3-role",
    }

    conn = S3ConnectionConfig.model_validate(payload)

    assert isinstance(conn, S3ConnectionConfig)
    assert conn.id == "88dabb63-aca5-4cc4-8f76-f78736a42f39"
    assert conn.name == "s3"
    assert conn.bucket == "my-bucket"
    assert conn.base_directory == "data/"
    assert conn.is_file_mask_source is True
    assert conn.is_file_mask_destination is False
    assert conn.iam_role_arn == "arn:aws:iam::111122223333:role/s3-role"


def test_s3_connection_model_validate_no_iam_role():
    payload = {
        "id": "id-1",
        "name": "s3",
        "mask_type": "file",
        "type": "s3_connection",
        "base_directory": "",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
        "bucket": "my-bucket",
    }

    conn = S3ConnectionConfig.model_validate(payload)
    assert conn.iam_role_arn is None


def test_databricks_delta_s3_connection_model_validate():
    payload = {
        "id": "11223344-5566-7788-99aa-bbccddeeff00",
        "name": "delta_s3",
        "mask_type": "file",
        "type": "databricks_delta_s3_connection",
        "base_directory": "delta/",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
        "bucket": "my-delta-bucket",
        "iam_role_arn": "arn:aws:iam::111122223333:role/delta-role",
    }

    conn = DatabricksDeltaS3ConnectionConfig.model_validate(payload)

    assert isinstance(conn, DatabricksDeltaS3ConnectionConfig)
    assert conn.id == "11223344-5566-7788-99aa-bbccddeeff00"
    assert conn.name == "delta_s3"
    assert conn.bucket == "my-delta-bucket"
    assert conn.base_directory == "delta/"
    assert conn.is_file_mask_source is True
    assert conn.is_file_mask_destination is False
    assert conn.iam_role_arn == "arn:aws:iam::111122223333:role/delta-role"


def test_databricks_delta_s3_connection_model_validate_no_iam_role():
    payload = {
        "id": "id-delta",
        "name": "delta_s3",
        "mask_type": "file",
        "type": "databricks_delta_s3_connection",
        "base_directory": "",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
        "bucket": "my-delta-bucket",
    }

    conn = DatabricksDeltaS3ConnectionConfig.model_validate(payload)
    assert conn.iam_role_arn is None


def test_validate_connection_dispatches_databricks_delta_s3():
    payload = {
        "id": "aabb-ccdd",
        "name": "delta",
        "mask_type": "file",
        "type": "databricks_delta_s3_connection",
        "base_directory": "",
        "is_file_mask_source": False,
        "is_file_mask_destination": True,
        "bucket": "delta-bucket",
    }

    conn = validate_connection(payload)

    assert isinstance(conn, DatabricksDeltaS3ConnectionConfig)
    assert conn.bucket == "delta-bucket"


def test_azure_connection_model_validate_blanks_encrypted_connection_string():
    payload = {
        "id": "490502e5-5bf6-4abb-b67b-c6091d40ecf0",
        "name": "azure",
        "mask_type": "file",
        "type": "azure_blob_connection",
        "base_directory": "",
        "container": "mycontainer",
        "is_file_mask_source": True,
        "is_file_mask_destination": True,
        # The API only returns the encrypted form; the plaintext is never sent back.
        "connection_string_encrypted": "some_base64_here",
    }

    conn = AzureConnectionConfig.model_validate(payload)

    assert isinstance(conn, AzureConnectionConfig)
    assert conn.container == "mycontainer"
    assert conn.connection_string is None
    assert conn.id == "490502e5-5bf6-4abb-b67b-c6091d40ecf0"


def test_mounted_share_connection_model_validate():
    payload = sample_mounted_share_connection_json(id="7ba07e3d-f917-4bee-bfc0-c42b9b01a06e", name="mount")

    conn = MountedShareConnectionConfig.model_validate(payload)

    assert isinstance(conn, MountedShareConnectionConfig)
    assert conn.name == "mount"
    assert conn.base_directory == ""
    assert conn.id == "7ba07e3d-f917-4bee-bfc0-c42b9b01a06e"


def test_database_connection_model_validate_drops_schema_for_mysql():
    payload = {
        "id": "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7",
        "name": "mysql",
        "mask_type": "database",
        "db_type": "mysql",
        "host": "my-mysql-host",
        "port": 3306,
        "database": "mydatabase",
        "user": "me",
        "schema": "should_be_dropped",  # MySQL has no schemas — must be discarded.
        "is_read_only": False,
    }

    conn = DatabaseConnectionConfig.model_validate(payload)

    assert isinstance(conn, DatabaseConnectionConfig)
    assert conn.database_type is DatabaseType.mysql
    assert conn.db_schema is None
    assert conn.password is None


def test_database_connection_model_validate_keeps_schema_for_postgres():
    payload = {
        "id": "abc",
        "name": "pg",
        "mask_type": "database",
        "db_type": "postgres",
        "host": "pg-host",
        "port": 5432,
        "database": "pgdb",
        "user": "pg",
        "schema": "public",
        "is_read_only": False,
    }

    conn = DatabaseConnectionConfig.model_validate(payload)
    assert conn.db_schema == "public"


def test_database_connection_model_validate_databricks_lakebase():
    payload = {
        "id": "abc-lakebase",
        "name": "lakebase",
        "mask_type": "database",
        "db_type": "databricks_lakebase",
        "host": "lakebase-host",
        "port": 5432,
        "database": "lakebasedb",
        "user": "lakebase_user",
        "schema": "public",
        "is_read_only": False,
    }

    conn = DatabaseConnectionConfig.model_validate(payload)

    assert isinstance(conn, DatabaseConnectionConfig)
    assert conn.database_type is DatabaseType.databricks_lakebase
    assert conn.db_schema == "public"


def test_mssql_linked_connection_model_validate_includes_linked_server():
    payload = {
        "id": "48a7af45-f63f-4e05-bf9f-7b1cc3a0e89d",
        "name": "mssql-linked",
        "mask_type": "database",
        "db_type": "mssql_linked",
        "host": "mssql-linked-host",
        "port": 3306,
        "database": "database_name",
        "user": "mine",
        "schema": "",
        "is_read_only": False,
        "linked_server": "name.database.schema",
    }

    conn = MssqlLinkedServerConnectionConfig.model_validate(payload)

    assert isinstance(conn, MssqlLinkedServerConnectionConfig)
    assert conn.database_type is DatabaseType.mssql_linked
    assert conn.linked_server == "name.database.schema"


def test_dynamo_connection_model_validate_with_sse():
    payload = {
        "id": "d7257552-0485-4806-b0fb-d72b4d268073",
        "name": "db_dynamo",
        "mask_type": "database",
        "db_type": "dynamo_db",
        "s3_bucket_name": "my-dynamo-staging-bucket",
        "dynamo_append_datetime": False,
        "dynamo_append_suffix": "-masked",
        "dynamo_replace_tables": True,
        "dynamo_default_region": None,
        "dynamo_default_sse": {
            "selection": "account_managed",
            "kms_key_id": "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
        },
    }

    conn = DynamoConnectionConfig.model_validate(payload)

    assert isinstance(conn, DynamoConnectionConfig)
    assert conn.s3_bucket_name == "my-dynamo-staging-bucket"
    assert conn.dynamo_default_sse == SseConfig(
        selection=SseSelection.account_managed,
        kms_key_id="arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
    )
    assert conn.id == "d7257552-0485-4806-b0fb-d72b4d268073"


def test_dynamo_connection_model_validate_without_sse_uses_default():
    payload = {
        "id": "id-2",
        "name": "db_dynamo",
        "mask_type": "database",
        "db_type": "dynamo_db",
        "s3_bucket_name": "bucket",
        "dynamo_append_datetime": False,
        "dynamo_append_suffix": "-masked",
        "dynamo_replace_tables": True,
        "dynamo_default_region": "us-east-1",
    }

    conn = DynamoConnectionConfig.model_validate(payload)

    # Falls back to the dataclass default when the API omits the field.
    assert conn.dynamo_default_sse == SseConfig(selection=SseSelection.dynamodb_owned, kms_key_id=None)


def test_snowflake_connection_model_validate_with_stage_location():
    payload = {
        "id": "f0557fb3-1c9a-4cb1-bcf4-9699cf496bf7",
        "name": "snowflake",
        "mask_type": "database",
        "db_type": "snowflake",
        "user": "snowman",
        "database": "icicle",
        "schema": "snowball",
        "snowflake_role": "snowballs do indeed roll",
        "snowflake_account_id": "ABCDEF-123456",
        "snowflake_warehouse": "warehouse1",
        "snowflake_storage_integration_name": "mysi",
        "host": "snowflake.com",
        "port": 443,
        "s3_bucket_name": "ice-bucket",
        "iam_role_arn": "swiss roll",
        "snowflake_stage_location": "aws_s3",
        "is_read_only": False,
    }

    conn = SnowflakeConnectionConfig.model_validate(payload)

    assert isinstance(conn, SnowflakeConnectionConfig)
    assert conn.snowflake_stage_location is SnowflakeStageLocation.aws_s3
    assert conn.iam_role_arn == "swiss roll"
    assert conn.password is None


def test_snowflake_connection_model_validate_without_stage_location():
    payload = {
        "id": "id-3",
        "name": "snowflake",
        "mask_type": "database",
        "db_type": "snowflake",
        "user": "frosty",
        "database": "igloo",
        "snowflake_account_id": "ACCOUNT-1234",
        "snowflake_warehouse": "clothing_store",
        "snowflake_storage_integration_name": "kennards",
    }

    conn = SnowflakeConnectionConfig.model_validate(payload)

    assert conn.snowflake_stage_location is None
    assert conn.host == ""
    assert conn.port is None
    assert conn.db_schema is None


def test_connection_config_dispatch_picks_subclass():
    """`ConnectionConfig.model_validate` dispatches by `mask_type` and `type`/`db_type`."""
    s3_payload = {
        "id": "id-s3",
        "name": "s3",
        "mask_type": "file",
        "type": "s3_connection",
        "base_directory": "",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
        "bucket": "b",
    }
    db_payload = {
        "id": "id-pg",
        "name": "pg",
        "mask_type": "database",
        "db_type": "postgres",
        "host": "h",
        "port": 5432,
        "database": "d",
        "user": "u",
        "schema": "public",
        "is_read_only": False,
    }

    assert isinstance(validate_connection(s3_payload), S3ConnectionConfig)
    assert isinstance(validate_connection(db_payload), DatabaseConnectionConfig)


def test_connection_config_dispatch_unknown_mask_type_raises():
    with pytest.raises(DataMasqueException, match="Unexpected connection mask_type"):
        validate_connection({"mask_type": "unknown", "id": "x", "name": "x"})


def test_connection_config_dispatch_unknown_file_type_raises():
    with pytest.raises(DataMasqueException, match="Unexpected file connection type"):
        validate_connection({"mask_type": "file", "type": "totally_made_up", "id": "x", "name": "x"})


def test_dynamo_connection_round_trip_with_iam_role_and_prefix():
    """`iam_role_arn` and `export_s3_prefix` survive a full from-API → to-API round trip."""
    payload = {
        "id": "id-dynamo-1",
        "name": "db_dynamo",
        "mask_type": "database",
        "db_type": "dynamo_db",
        "s3_bucket_name": "staging-bucket",
        "dynamo_append_datetime": False,
        "dynamo_append_suffix": "-masked",
        "dynamo_replace_tables": True,
        "dynamo_default_region": "us-east-1",
        "iam_role_arn": "arn:aws:iam::111122223333:role/dynamo-role",
        "export_s3_prefix": "team/dynamo/",
    }

    conn = DynamoConnectionConfig.model_validate(payload)
    assert conn.iam_role_arn == "arn:aws:iam::111122223333:role/dynamo-role"
    assert conn.export_s3_prefix == "team/dynamo/"

    api_dict = conn.model_dump(exclude_none=True, by_alias=True, mode="json")
    assert api_dict["iam_role_arn"] == "arn:aws:iam::111122223333:role/dynamo-role"
    assert api_dict["export_s3_prefix"] == "team/dynamo/"


def test_dynamo_connection_model_dump_omits_unset_iam_role_and_prefix():
    """When the new optional fields are unset, `model_dump` must omit them entirely (not send `null`)."""
    conn = DynamoConnectionConfig(
        name="db_dynamo",
        s3_bucket_name="bucket",
        dynamo_append_datetime=False,
        dynamo_append_suffix="-masked",
        dynamo_replace_tables=True,
        dynamo_default_region="us-east-1",
    )

    api_dict = conn.model_dump(exclude_none=True, by_alias=True, mode="json")
    assert "iam_role_arn" not in api_dict
    assert "export_s3_prefix" not in api_dict


def test_dynamo_model_validate_defaults_to_none_when_fields_absent():
    """An older server that doesn't return the new fields still deserializes cleanly."""
    payload = {
        "id": "id-dynamo-2",
        "name": "db_dynamo",
        "mask_type": "database",
        "db_type": "dynamo_db",
        "s3_bucket_name": "bucket",
        "dynamo_append_datetime": False,
        "dynamo_append_suffix": "-masked",
        "dynamo_replace_tables": True,
        "dynamo_default_region": "us-east-1",
    }

    conn = DynamoConnectionConfig.model_validate(payload)
    assert conn.iam_role_arn is None
    assert conn.export_s3_prefix is None


def test_mongo_connection_model_dump_minimal():
    """An unauthenticated, plain TCP connection sends only the required keys plus the defaulted booleans."""
    conn = MongoConnectionConfig(
        name="mongo",
        host="mongo.example",
        database="people",
    )

    d = conn.model_dump(exclude_none=True, by_alias=True, mode="json")
    assert d["name"] == "mongo"
    assert d["db_type"] == "mongodb"
    assert d["mask_type"] == "database"
    assert d["host"] == "mongo.example"
    assert d["port"] == 27017
    assert d["database"] == "people"
    assert d["auth_source"] == "admin"
    assert d["is_read_only"] is False


def test_mongo_connection_model_dump_full():
    conn = MongoConnectionConfig(
        name="mongo",
        host="mongo.example",
        port=27018,
        database="people",
        user="alice",
        password="hunter2",
        auth_source="other-db",
        tls=True,
        direct_connection=True,
        replica_set="rs0",
        is_read_only=True,
    )

    d = conn.model_dump(exclude_none=True, by_alias=True, mode="json")
    assert d["name"] == "mongo"
    assert d["db_type"] == "mongodb"
    assert d["mask_type"] == "database"
    assert d["host"] == "mongo.example"
    assert d["port"] == 27018
    assert d["database"] == "people"
    assert d["auth_source"] == "other-db"
    assert d["is_read_only"] is True
    assert d["dbpassword"] == "hunter2"
    assert d["tls"] is True
    assert d["direct_connection"] is True
    assert d["replica_set"] == "rs0"


def test_mongo_connection_model_dump_omits_falsy_optional_flags():
    """`tls`, `direct_connection`, `user`, `password`, and `replica_set` are only sent when truthy."""
    conn = MongoConnectionConfig(
        name="mongo",
        host="mongo.example",
        database="people",
        user="",
        password="",
        tls=False,
        direct_connection=False,
        replica_set="",
    )

    api_dict = conn.model_dump(exclude_none=True, by_alias=True, mode="json")
    for absent in ("user", "dbpassword", "tls", "direct_connection", "replica_set"):
        assert absent not in api_dict


def test_mongo_connection_model_validate_blanks_encrypted_password():
    payload = {
        "id": "mongo-id-1",
        "name": "mongo",
        "mask_type": "database",
        "db_type": "mongodb",
        "host": "mongo.example",
        "port": 27017,
        "database": "people",
        "user": "alice",
        # The API only ever returns the encrypted form; the plaintext is never echoed back.
        "password_encrypted": "some_base64_here",
        "auth_source": "admin",
        "tls": True,
        "direct_connection": False,
        "replica_set": "rs0",
        "is_read_only": False,
    }

    conn = MongoConnectionConfig.model_validate(payload)

    assert isinstance(conn, MongoConnectionConfig)
    assert conn.id == "mongo-id-1"
    assert conn.host == "mongo.example"
    assert conn.user == "alice"
    assert conn.password is None
    assert conn.tls is True
    assert conn.replica_set == "rs0"
    assert conn.database_type is DatabaseType.mongodb


def test_mongo_connection_model_validate_defaults_when_optional_fields_missing():
    payload = {
        "id": "mongo-id-2",
        "name": "mongo-min",
        "mask_type": "database",
        "db_type": "mongodb",
        "host": "mongo.example",
        "database": "people",
    }

    conn = MongoConnectionConfig.model_validate(payload)
    assert conn.port == 27017
    assert conn.user == ""
    assert conn.auth_source == "admin"
    assert conn.tls is False
    assert conn.direct_connection is False
    assert conn.replica_set == ""
    assert conn.is_read_only is False


def test_connection_config_dispatch_picks_mongo_subclass():
    payload = {
        "id": "mongo-id-3",
        "name": "mongo",
        "mask_type": "database",
        "db_type": "mongodb",
        "host": "mongo.example",
        "database": "people",
    }
    assert isinstance(validate_connection(payload), MongoConnectionConfig)


def test_database_connection_config_rejects_mongodb_database_type():
    """`DatabaseConnectionConfig` is for SQL engines; MongoDB users must use `MongoConnectionConfig`."""
    with pytest.raises(ValueError, match="For MongoDB"):
        DatabaseConnectionConfig(
            name="mongo",
            host="mongo.example",
            port=27017,
            database="people",
            user="alice",
            password="hunter2",
            database_type=DatabaseType.mongodb,
        )


def test_create_or_update_mongo_connection(client):
    """End-to-end: a Mongo connection round-trips through `create_or_update_connection`."""
    conn = MongoConnectionConfig(
        name="mongo",
        host="mongo.example",
        database="people",
        user="alice",
        password="hunter2",
        replica_set="rs0",
    )

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/connections/", json=[], status_code=200)
        m.post(
            "http://test-server/api/connections/",
            json={"id": "mongo-id-9"},
            status_code=201,
        )
        result = client.create_or_update_connection(conn)

    assert result.id == ConnectionId("mongo-id-9")
    sent = m.last_request.json()
    assert sent["mask_type"] == "database"
    assert sent["db_type"] == "mongodb"
    assert sent["dbpassword"] == "hunter2"
    assert sent["replica_set"] == "rs0"
