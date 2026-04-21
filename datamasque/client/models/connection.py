"""Connection configuration models for the DataMasque API."""

from enum import Enum
from typing import Any, Callable, Literal, NewType, Optional

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator

from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.files import FileId

ConnectionId = NewType("ConnectionId", str)


def unwrap_connection_id(value: Any) -> Any:
    """
    Coerce a `ConnectionConfig` to its `id`; pass other values through unchanged.

    Used by request-model validators that accept either a `ConnectionId`
    or a full `ConnectionConfig` for user convenience.
    Raises `ValueError` if the config has no `id`
    (i.e. the caller hasn't yet created it on the server).
    """

    if isinstance(value, ConnectionConfig):
        if value.id is None:
            raise ValueError("Connection has not been created yet (id is None)")
        return value.id

    return value


class DatabaseType(Enum):
    """Supported database engines for `DatabaseConnectionConfig`."""

    postgres = "postgres"
    mysql = "mysql"
    oracle = "oracle"
    mariadb = "mariadb"
    sql_server = "mssql"
    redshift = "redshift"
    dynamodb = "dynamo_db"
    db2_luw = "db2_luw"
    db2i = "db2i"
    mssql_linked = "mssql_linked"
    snowflake = "snowflake"
    mongodb = "mongodb"


class SnowflakeStageLocation(str, Enum):
    """Storage backend for a Snowflake connection's external stage."""

    local = "local"  # Not supported for production use
    aws_s3 = "aws_s3"
    azure_blob_storage = "azure_blob_storage"


class SseSelection(Enum):
    """Mirrors the available options in the AWS console for DynamoDB Server-Side Encryption."""

    dynamodb_owned = "dynamodb_owned"
    aws_managed = "aws_managed"
    account_managed = "account_managed"
    use_source = "use_source"


class SseConfig(BaseModel):
    """
    Server-side encryption configuration for a DynamoDB connection.

    `kms_key_id` is required when `selection` is `SseSelection.account_managed`
    and must be `None` for every other selection.
    """

    model_config = ConfigDict(extra="forbid")

    selection: SseSelection
    kms_key_id: Optional[str] = None  # Required when `selection` is `account_managed`; must be None otherwise

    @model_validator(mode="after")
    def _validate_kms_key(self) -> "SseConfig":
        if self.selection is SseSelection.account_managed:
            if self.kms_key_id is None:
                raise ValueError(
                    "A KMS key ID must be specified when the SSE key is stored in your account, and owned "
                    "and managed by you."
                )
        elif self.kms_key_id is not None:
            raise ValueError(
                "A KMS key ID can only be specified when the SSE key is stored in your account, and "
                "owned and managed by you."
            )
        return self


class ConnectionConfig(BaseModel):
    """
    Base class for all connection configurations.

    Use `validate_connection(payload)` to deserialize an API response
    into the appropriate concrete subclass.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    id: Optional[ConnectionId] = None


class DynamoConnectionConfig(ConnectionConfig):
    """Connection configuration for a DynamoDB table."""

    s3_bucket_name: Optional[str] = None
    dynamo_append_datetime: bool = False
    dynamo_append_suffix: str = "-MASKED"
    dynamo_replace_tables: bool = True
    dynamo_default_region: Optional[str] = None
    dynamo_default_sse: SseConfig = SseConfig(selection=SseSelection.dynamodb_owned, kms_key_id=None)
    iam_role_arn: Optional[str] = None
    export_s3_prefix: Optional[str] = None

    mask_type: Literal["database"] = "database"
    db_type: Literal["dynamo_db"] = "dynamo_db"

    @property
    def database_type(self) -> DatabaseType:
        return DatabaseType.dynamodb

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Callable) -> dict:
        d = handler(self)
        # The admin server requires these placeholder fields for Dynamo connections.
        d.setdefault("host", "")
        d.setdefault("port", None)
        d.setdefault("user", "")
        d.setdefault("password", "")
        d.setdefault("database", "")
        d.setdefault("schema", "")
        return d

    @model_validator(mode="before")
    @classmethod
    def _strip_server_only_fields(cls, data: dict) -> dict:
        """Drop fields that come back from the server but aren't part of this model."""
        if isinstance(data, dict):
            for key in ("password_encrypted", "dbpassword"):
                data.pop(key, None)
        return data


class MongoConnectionConfig(ConnectionConfig):
    """Connection configuration for a MongoDB instance."""

    host: str = ""
    port: int = 27017
    database: str = ""
    user: str = ""
    password: Optional[str] = None
    auth_source: str = "admin"
    tls: bool = False
    direct_connection: bool = False
    replica_set: str = ""
    is_read_only: bool = False

    mask_type: Literal["database"] = "database"
    db_type: Literal["mongodb"] = "mongodb"

    @property
    def database_type(self) -> DatabaseType:
        return DatabaseType.mongodb

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Callable) -> dict:
        d = handler(self)
        # The server expects the password under the `dbpassword` key.
        password = d.pop("password", None)
        if password:
            d["dbpassword"] = password
        if not d.get("tls"):
            d.pop("tls", None)
        if not d.get("direct_connection"):
            d.pop("direct_connection", None)
        if not d.get("replica_set"):
            d.pop("replica_set", None)
        if not d.get("user"):
            d.pop("user", None)
        return d

    @model_validator(mode="before")
    @classmethod
    def _strip_encrypted_password(cls, data: dict) -> dict:
        if isinstance(data, dict):
            for key in ("password_encrypted", "dbpassword"):
                data.pop(key, None)
        return data


class SnowflakeConnectionConfig(ConnectionConfig):
    """
    Connection configuration for a Snowflake database.

    Supports password authentication (`password`)
    and key-pair authentication (`snowflake_private_key` + optional `snowflake_private_key_passphrase`).
    """

    database: str
    user: str
    snowflake_account_id: str
    snowflake_warehouse: str
    snowflake_storage_integration_name: str
    host: str = ""
    port: Optional[int] = None
    db_schema: Optional[str] = Field(default=None, alias="schema")
    snowflake_role: str = ""
    is_read_only: bool = False
    password: Optional[str] = None
    snowflake_private_key: Optional[FileId] = None
    snowflake_private_key_passphrase: Optional[str] = None
    snowflake_stage_location: Optional[SnowflakeStageLocation] = None
    s3_bucket_name: Optional[str] = None
    iam_role_arn: Optional[str] = None
    snowflake_azure_container_name: Optional[str] = None
    snowflake_azure_connection_string: Optional[str] = None
    snowflake_azure_connection_string_encrypted: Optional[str] = None

    mask_type: Literal["database"] = "database"
    db_type: Literal["snowflake"] = "snowflake"

    @property
    def database_type(self) -> DatabaseType:
        return DatabaseType.snowflake

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Callable) -> dict:
        d = handler(self)
        # The server expects the password under the `dbpassword` key.
        password = d.pop("password", None)
        if password is not None:
            d["dbpassword"] = password
        # Snowflake requires `schema` even when the user hasn't set one.
        if d.get("schema") is None:
            d["schema"] = ""
        return d

    @model_validator(mode="before")
    @classmethod
    def _strip_encrypted_password(cls, data: dict) -> dict:
        if isinstance(data, dict):
            for key in ("password_encrypted", "dbpassword"):
                data.pop(key, None)
        return data


class DatabaseConnectionConfig(ConnectionConfig):
    """
    Connection configuration for a SQL database.

    Use `DynamoConnectionConfig` for DynamoDB, `SnowflakeConnectionConfig` for Snowflake,
    and `MongoConnectionConfig` for MongoDB.
    """

    host: str
    port: int
    database: str
    user: str
    password: Optional[str] = None
    database_type: DatabaseType
    engine_options: Optional[dict] = None
    db_schema: Optional[str] = Field(default=None, alias="schema")
    data_encoding: Optional[str] = None
    is_read_only: bool = False
    s3_bucket_name: Optional[str] = None
    s3_redshift_iam_role: Optional[str] = None

    @model_validator(mode="after")
    def _reject_special_engines(self) -> "DatabaseConnectionConfig":
        if self.database_type is DatabaseType.dynamodb:
            raise ValueError("For DynamoDB, use the DynamoConnectionConfig class instead")
        if self.database_type is DatabaseType.snowflake:
            raise ValueError("For Snowflake, use the SnowflakeConnectionConfig class instead")
        if self.database_type is DatabaseType.mongodb:
            raise ValueError("For MongoDB, use the MongoConnectionConfig class instead")
        return self

    mask_type: Literal["database"] = "database"

    @property
    def db_type(self) -> str:
        return self.database_type.value

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Callable) -> dict:
        d = handler(self)
        # The server expects the password under the `dbpassword` key.
        password = d.pop("password", None)
        if password is not None:
            d["dbpassword"] = password
        d.pop("database_type", None)
        d["db_type"] = self.db_type

        # The server requires certain fields to be present or absent
        # depending on the engine type.
        db_type = self.database_type
        if db_type in {DatabaseType.mysql, DatabaseType.mariadb} or d.get("schema") is None:
            d["schema"] = ""
        if db_type not in {DatabaseType.mysql, DatabaseType.mariadb, DatabaseType.oracle, DatabaseType.postgres}:
            d.pop("data_encoding", None)
        if db_type is not DatabaseType.redshift:
            d.pop("s3_bucket_name", None)
            d.pop("s3_redshift_iam_role", None)
        if not d.get("engine_options"):
            d.pop("engine_options", None)
        return d

    @model_validator(mode="before")
    @classmethod
    def _normalize_incoming(cls, data: dict) -> dict:
        if isinstance(data, dict):
            for key in ("password_encrypted", "dbpassword"):
                data.pop(key, None)

            # Determine the engine type from whichever key is present.
            engine = data.get("database_type") or data.get("db_type", "")
            if isinstance(engine, DatabaseType):
                engine = engine.value

            # The API returns a `schema` value for engines that don't have schemas (MySQL/MariaDB).
            # Drop it so the model accurately reflects "not applicable".
            if engine in {DatabaseType.mysql.value, DatabaseType.mariadb.value}:
                data.pop("schema", None)

            # Map `db_type` → `database_type` for incoming payloads.
            if "db_type" in data and "database_type" not in data:
                data["database_type"] = data.pop("db_type")
        return data


class MssqlLinkedServerConnectionConfig(DatabaseConnectionConfig):
    """Connection configuration for a Microsoft SQL Server linked-server setup."""

    linked_server: str = ""


class FileConnectionConfig(ConnectionConfig):
    """
    Abstract base for file-based connections.

    `is_file_mask_source` and `is_file_mask_destination`
    control whether the connection can be used as the source, destination, or both of a masking run.
    """

    base_directory: str = ""
    is_file_mask_source: bool = False
    is_file_mask_destination: bool = False

    mask_type: Literal["file"] = "file"


class S3ConnectionConfig(FileConnectionConfig):
    """Connection configuration for an S3 bucket."""

    type: Literal["s3_connection"] = "s3_connection"
    bucket: str = ""
    iam_role_arn: Optional[str] = None


class AzureConnectionConfig(FileConnectionConfig):
    """
    Connection configuration for an Azure Blob Storage container.

    `connection_string` comes back encrypted from `list_connections`
    and is write-only in practice.
    """

    type: Literal["azure_blob_connection"] = "azure_blob_connection"
    container: str = ""
    connection_string: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _strip_encrypted_connection_string(cls, data: dict) -> dict:
        if isinstance(data, dict):
            # The API returns the encrypted form; drop it so `connection_string` stays None.
            data.pop("connection_string_encrypted", None)
        return data


class MountedShareConnectionConfig(FileConnectionConfig):
    """Connection configuration for a mounted file share."""

    type: Literal["mounted_share_connection"] = "mounted_share_connection"


FILE_TYPE_MAP: dict[str, type[FileConnectionConfig]] = {
    "s3_connection": S3ConnectionConfig,
    "azure_blob_connection": AzureConnectionConfig,
    "mounted_share_connection": MountedShareConnectionConfig,
}

DB_TYPE_MAP: dict[str, type[ConnectionConfig]] = {
    DatabaseType.dynamodb.value: DynamoConnectionConfig,
    DatabaseType.mongodb.value: MongoConnectionConfig,
    DatabaseType.snowflake.value: SnowflakeConnectionConfig,
    DatabaseType.mssql_linked.value: MssqlLinkedServerConnectionConfig,
    # others use the default `DatabaseConnectionConfig`
}


def validate_connection(payload: dict) -> ConnectionConfig:
    """
    Validate an API response payload into the appropriate concrete `ConnectionConfig` subclass.

    Dispatches on `mask_type`, then on `type` (file) or `db_type` (database).
    """

    mask_type = payload.get("mask_type")

    if mask_type == "file":
        file_type = payload.get("type", "")
        klass = FILE_TYPE_MAP.get(file_type)
        if klass is None:
            raise DataMasqueException(f"Unexpected file connection type: {file_type}")
        return klass.model_validate(payload)

    if mask_type == "database":
        db_type = payload.get("db_type", "")
        db_klass = DB_TYPE_MAP.get(db_type, DatabaseConnectionConfig)
        return db_klass.model_validate(payload)

    raise DataMasqueException(f"Unexpected connection mask_type: {mask_type}")
