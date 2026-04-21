"""Shared test helpers used across the per-feature test modules."""

from faker import Faker
from requests import Response

from datamasque.client.models.connection import (
    DatabaseConnectionConfig,
    DatabaseType,
    S3ConnectionConfig,
    SnowflakeConnectionConfig,
    SnowflakeStageLocation,
)

fake = Faker()


def parse_multipart_form(request) -> dict:  # noqa: C901
    """
    Parse a multipart form request body into a dictionary.

    Returns a dict where:
    - Regular fields have string values
    - File fields have dict values with 'filename', 'content_type', and 'content' keys.
    """
    content_type = request.headers.get("Content-Type", "")
    if "boundary=" not in content_type:
        raise ValueError("No boundary found in Content-Type header")

    boundary = content_type.split("boundary=")[1].encode()
    parts = request.body.split(b"--" + boundary)

    result = {}
    for part in parts:
        if not part or part == b"--\r\n" or part.strip() == b"--":
            continue

        if b"\r\n\r\n" not in part:
            continue
        headers_section, content = part.split(b"\r\n\r\n", 1)

        if content.endswith(b"\r\n"):
            content = content[:-2]

        headers_text = headers_section.decode("utf-8", errors="replace")
        name = None
        filename = None
        field_content_type = None

        for line in headers_text.split("\r\n"):
            if line.lower().startswith("content-disposition:"):
                if 'name="' in line:
                    name = line.split('name="')[1].split('"')[0]
                if 'filename="' in line:
                    filename = line.split('filename="')[1].split('"')[0]
            elif line.lower().startswith("content-type:"):
                field_content_type = line.split(":", 1)[1].strip()

        if name:
            if filename is not None:
                result[name] = {
                    "filename": filename,
                    "content_type": field_content_type,
                    "content": content,
                }
            else:
                result[name] = content.decode("utf-8", errors="replace")

    return result


def database_connection_config():
    return DatabaseConnectionConfig(
        name=fake.word(),
        user=fake.user_name(),
        password=fake.password(),
        host="localhost",
        port=fake.port_number(),
        database=f"{fake.word()}_db",
        schema="test_schema",
        database_type=DatabaseType.postgres,
    )


def file_connection_config():
    return S3ConnectionConfig(
        name=fake.word(),
        base_directory=fake.uri_path(),
        bucket=fake.uri_page(),
        is_file_mask_source=True,
        is_file_mask_destination=False,
    )


def sample_mounted_share_connection_json(*, id, name):
    return {
        "name": name,
        "id": id,
        "mask_type": "file",
        "type": "mounted_share_connection",
        "base_directory": "",
        "is_file_mask_source": True,
        "is_file_mask_destination": False,
    }


def make_ok_response() -> Response:
    """
    Build a minimal 2xx `Response` for tests that patch `requests.request`.

    Returned value has `status_code = 200` and an empty JSON body,
    which is enough to pass through `make_request`'s status check
    without the test having to construct a full HTTP round-trip.
    """
    response = Response()
    response.status_code = 200
    response._content = b"{}"
    return response


def snowflake_connection_config_s3():
    return SnowflakeConnectionConfig(
        name="snowflake_s3",
        database="test_db",
        user="snowflake_user",
        snowflake_account_id="ACCOUNT-123",
        snowflake_warehouse="test_warehouse",
        snowflake_storage_integration_name="test_integration",
        password="test_password",
        snowflake_stage_location=SnowflakeStageLocation.aws_s3,
        s3_bucket_name="test-bucket",
        iam_role_arn="arn:aws:iam::123456789012:role/test-role",
    )


def snowflake_connection_config_azure():
    return SnowflakeConnectionConfig(
        name="snowflake_azure",
        database="test_db",
        user="snowflake_user",
        snowflake_account_id="ACCOUNT-456",
        snowflake_warehouse="test_warehouse",
        snowflake_storage_integration_name="test_integration",
        password="test_password",
        snowflake_stage_location=SnowflakeStageLocation.azure_blob_storage,
        snowflake_azure_container_name="test-container",
        snowflake_azure_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test",
    )


def snowflake_connection_config_local():
    return SnowflakeConnectionConfig(
        name="snowflake_local",
        database="test_db",
        user="snowflake_user",
        snowflake_account_id="ACCOUNT-789",
        snowflake_warehouse="test_warehouse",
        snowflake_storage_integration_name="test_integration",
        password="test_password",
        snowflake_stage_location=SnowflakeStageLocation.local,
    )
