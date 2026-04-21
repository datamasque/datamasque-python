from datamasque.client.base import FileOrContent, UploadFile
from datamasque.client.connections import ConnectionClient
from datamasque.client.discovery import DiscoveryClient
from datamasque.client.files import FileClient
from datamasque.client.license import LicenseClient
from datamasque.client.ruleset_libraries import RulesetLibraryClient
from datamasque.client.rulesets import RulesetClient
from datamasque.client.runs import RunClient
from datamasque.client.settings import SettingsClient
from datamasque.client.users import UserClient

__all__ = ["DataMasqueClient", "FileOrContent", "UploadFile"]


class DataMasqueClient(
    LicenseClient,
    ConnectionClient,
    RulesetClient,
    RulesetLibraryClient,
    FileClient,
    RunClient,
    DiscoveryClient,
    UserClient,
    SettingsClient,
):
    """
    Client for a DataMasque server instance.

    Example usage:

    .. code-block:: python

        from datamasque.client import DataMasqueClient
        from datamasque.client.models.dm_instance import DataMasqueInstanceConfig

        config = DataMasqueInstanceConfig(
            base_url="https://datamasque.example.com",
            username="api_user",
            password="api_password",
        )
        client = DataMasqueClient(config)
        client.authenticate()

        for connection in client.list_connections():
            print(connection.name)

    Authentication is performed on the first request if `authenticate()` is not called explicitly,
    and is automatically retried once on a 401 response.
    """
