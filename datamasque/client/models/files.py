from abc import abstractmethod
from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, model_validator

FileId = NewType("FileId", str)


class DataMasqueFile(BaseModel):
    """Base class for the concrete file types (`SeedFile`, `OracleWalletFile`, `SslZipFile`, `SnowflakeKeyFile`)."""

    model_config = ConfigDict(extra="allow")

    name: str
    created_date: datetime
    modified_date: Optional[datetime] = None
    id: Optional[FileId] = None

    @model_validator(mode="before")
    @classmethod
    def _promote_filename(cls, data: dict) -> dict:
        """The API sometimes returns `filename` instead of `name`."""
        if isinstance(data, dict):
            if "filename" in data and "name" not in data:
                data["name"] = data["filename"]
        return data

    @classmethod
    @abstractmethod
    def get_url(cls) -> str:
        """Returns the API URL path for files of this type."""

        raise NotImplementedError  # pragma: no cover

    @classmethod
    @abstractmethod
    def get_content_param_name(cls) -> str:
        """Returns the multipart form field name used when uploading files of this type."""

        raise NotImplementedError  # pragma: no cover


class SeedFile(DataMasqueFile):
    """Represents a seed file (CSV file)."""

    @classmethod
    def get_url(cls) -> str:
        return "api/seeds/"

    @classmethod
    def get_content_param_name(cls) -> str:
        return "seed_file"


class OracleWalletFile(DataMasqueFile):
    """Represents an Oracle wallet file (ZIP file)."""

    @classmethod
    def get_url(cls) -> str:
        return "api/oracle-wallets/"

    @classmethod
    def get_content_param_name(cls) -> str:
        return "zip_archive"


class SslZipFile(DataMasqueFile):
    """Represents a ZIP file of SSL certificates used to establish secure database connections."""

    @classmethod
    def get_url(cls) -> str:
        return "api/connection-filesets/"

    @classmethod
    def get_content_param_name(cls) -> str:
        return "zip_archive"


class SnowflakeKeyFile(DataMasqueFile):
    """Represents a private SSH key file for Snowflake connections."""

    @classmethod
    def get_url(cls) -> str:
        return "api/files/snowflake-keys/"

    @classmethod
    def get_content_param_name(cls) -> str:
        return "key_file"
