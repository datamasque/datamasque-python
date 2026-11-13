import logging
from typing import Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.connection import ConnectionConfig, ConnectionId, validate_connection
from datamasque.client.models.project import ProjectId, ProjectIdentifier, unwrap_project_id

logger = logging.getLogger(__name__)


class ConnectionClient(BaseClient):
    """Connection-related API methods. Mixed into `DataMasqueClient`."""

    def list_connections(self, project: Optional[ProjectIdentifier] = None) -> list[ConnectionConfig]:
        """
        Lists all configured connections, or only those in `project` when it is given.

        `project` is a `Project`, its `ProjectId` or its `<id>-<slug>` form.
        Note that database passwords and connection strings are returned encrypted over the API
        and so are `None` on the returned `ConnectionConfig` objects.
        """

        return self._list_connections(unwrap_project_id(project) if project is not None else None)

    def _list_connections(self, project: Optional[Union[ProjectId, str]]) -> list[ConnectionConfig]:
        """Lists the connections in `project`, given as an ID or `<id>-<slug>` reference, or in every project."""

        params = {"project": project} if project is not None else None
        response = self.make_request("GET", "/api/connections/", params=params)
        return [validate_connection(payload) for payload in response.json()]

    def create_or_update_connection(self, connection_config: ConnectionConfig) -> ConnectionConfig:
        """
        Creates or updates the connection in DM, and sets the `id` field on the given `connection_config`.

        An existing connection with the same name in the same project is updated.
        With no `project` set, the client's `default_project` is used, else the server's only active project,
        and `DataMasqueException` is raised when it has no active project or more than one.
        """

        connection_id = connection_config.id

        connections_matching_name = [
            connection
            for connection in self._list_connections(self._resolve_project(connection_config.project))
            if connection.name == connection_config.name
        ]
        if connections_matching_name:
            connection_id = connections_matching_name[0].id

        data = {
            "version": "1.0",
        } | connection_config.model_dump(exclude_none=True, by_alias=True, mode="json")
        if connection_id is None:
            data |= self._default_project_field(connection_config.project)
            response = self.make_request("POST", "/api/connections/", data=data)
        else:
            response = self.make_request("PUT", f"/api/connections/{connection_id}/", data=data)

        connection_data = response.json()
        server_connection_id = ConnectionId(connection_data["id"])
        logger.debug("%s creation successful", type(connection_config).__name__)
        connection_config.id = server_connection_id
        connection_config.project = connection_data.get("project")
        return connection_config

    def delete_connection_by_id_if_exists(self, connection_id: ConnectionId) -> None:
        """Deletes the connection with the given ID. No-op if the connection does not exist."""

        self._delete_if_exists(f"/api/connections/{connection_id}/")

    def delete_connection_by_name_if_exists(self, connection_name: str, project: ProjectIdentifier) -> None:
        """
        Deletes the connection with the given name in the given project.

        `project` is a `Project`, its `ProjectId` or its `<id>-<slug>` form.
        No-op if the connection does not exist.
        """

        connections_matching_name = [
            connection for connection in self.list_connections(project) if connection.name == connection_name
        ]
        for connection in connections_matching_name:
            if connection.id is None:
                raise DataMasqueException(f'Server returned a connection named "{connection.name}" without an `id`.')

            self.delete_connection_by_id_if_exists(connection.id)
