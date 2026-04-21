import logging

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.connection import ConnectionConfig, ConnectionId, validate_connection

logger = logging.getLogger(__name__)


class ConnectionClient(BaseClient):
    """Connection-related API methods. Mixed into `DataMasqueClient`."""

    def list_connections(self) -> list[ConnectionConfig]:
        """
        Lists all configured connections.

        Note that database passwords and connection strings are returned encrypted over the API
        and so are `None` on the returned `ConnectionConfig` objects.
        """

        response = self.make_request("GET", "/api/connections/")
        return [validate_connection(payload) for payload in response.json()]

    def create_or_update_connection(self, connection_config: ConnectionConfig) -> ConnectionConfig:
        """Creates or updates the connection in DM, and sets the `id` field on the given `connection_config`."""

        connection_id = connection_config.id

        all_connections = self.list_connections()
        connections_matching_name = [
            connection for connection in all_connections if connection.name == connection_config.name
        ]
        if connections_matching_name:
            connection_id = connections_matching_name[0].id

        data = {
            "version": "1.0",
        } | connection_config.model_dump(exclude_none=True, by_alias=True, mode="json")
        if connection_id is None:
            response = self.make_request("POST", "/api/connections/", data=data)
        else:
            response = self.make_request("PUT", f"/api/connections/{connection_id}/", data=data)

        connection_data = response.json()
        server_connection_id = ConnectionId(connection_data["id"])
        logger.debug("%s creation successful", type(connection_config).__name__)
        connection_config.id = server_connection_id
        return connection_config

    def delete_connection_by_id_if_exists(self, connection_id: ConnectionId) -> None:
        """Deletes the connection with the given ID. No-op if the connection does not exist."""

        self._delete_if_exists(f"/api/connections/{connection_id}/")

    def delete_connection_by_name_if_exists(self, connection_name: str) -> None:
        """Deletes the connection with the given name. No-op if the connection does not exist."""

        all_connections = self.list_connections()
        connections_matching_name = [connection for connection in all_connections if connection.name == connection_name]
        for connection in connections_matching_name:
            if connection.id is None:
                raise DataMasqueException(f'Server returned a connection named "{connection.name}" without an `id`.')

            self.delete_connection_by_id_if_exists(connection.id)
