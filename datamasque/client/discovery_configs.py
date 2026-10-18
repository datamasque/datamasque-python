import logging
from typing import Iterator, Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.discovery_config import DiscoveryConfig, DiscoveryConfigId, DiscoveryConfigType
from datamasque.client.models.pagination import Page
from datamasque.client.models.project import Project, ProjectId, unwrap_project_id

logger = logging.getLogger(__name__)


class DiscoveryConfigClient(BaseClient):
    """Discovery config CRUD API methods. Mixed into `DataMasqueClient`."""

    def iter_discovery_configs(self) -> Iterator[DiscoveryConfig]:
        """Lazily iterate all discovery configs via the paginated endpoint."""

        return self._iter_paginated("/api/discovery/configs/", model=DiscoveryConfig)

    def list_discovery_configs(self) -> list[DiscoveryConfig]:
        """
        Lists all discovery configs.

        Note: the YAML content is not included in the list response for performance.
        Use `get_discovery_config` to retrieve the full config with its YAML body.
        """

        return list(self.iter_discovery_configs())

    def get_discovery_config(self, config_id: DiscoveryConfigId) -> DiscoveryConfig:
        """Retrieves a single discovery config by ID."""

        response = self.make_request("GET", f"/api/discovery/configs/{config_id}/")
        return DiscoveryConfig.model_validate(response.json())

    def _get_discovery_config_id_by_name(
        self, name: str, config_type: DiscoveryConfigType, project: Union[ProjectId, str]
    ) -> Optional[DiscoveryConfigId]:
        """Return the id of the config matching name, type and project via a single list request, or `None`."""

        response = self.make_request(
            "GET",
            "/api/discovery/configs/",
            params={"name_exact": name, "config_type": config_type.value, "project": project, "limit": 1},
        )
        page = Page[DiscoveryConfig].model_validate(response.json())
        if not page.results:
            return None

        config_id = page.results[0].id
        if config_id is None:
            raise DataMasqueApiError(
                "Server returned a discovery config list entry without an `id`.",
                response=response,
            )

        return config_id

    def get_discovery_config_by_name(
        self, name: str, config_type: DiscoveryConfigType, project: Union[ProjectId, Project]
    ) -> Optional[DiscoveryConfig]:
        """
        Looks for a discovery config matching the given name (case-sensitive, exact match), type and project.

        Config names are unique per type and project, so both are required to identify a single config.
        `project` is a `Project` or its `ProjectId`.
        Returns it if found, otherwise `None`.
        """

        config_id = self._get_discovery_config_id_by_name(name, config_type, unwrap_project_id(project))
        if config_id is None:
            return None

        return self.get_discovery_config(config_id)

    def create_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Creates a new discovery config on the server.

        Sets the config's server-assigned fields
        (`id`, `is_valid`, `validation_error`, `validation_error_details`, `created`, `modified`)
        and returns the config.
        """

        if not config.yaml:
            raise ValueError("Cannot create a discovery config without YAML content (yaml is empty)")

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/discovery/configs/", data=data)
        created = DiscoveryConfig.model_validate(response.json())
        config.id = created.id
        config.project = created.project
        config.is_valid = created.is_valid
        config.validation_error = created.validation_error
        config.validation_error_details = created.validation_error_details
        config.created = created.created
        config.modified = created.modified
        logger.info('Creation of discovery config "%s" successful', config.name)
        return config

    def update_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Performs a full update of the discovery config.

        The config must have its `id` set
        and its `yaml` content present.
        """

        if config.id is None:
            raise ValueError("Cannot update a discovery config that has not been created yet (id is None)")

        if not config.yaml:
            raise ValueError(
                "Cannot update a discovery config without YAML content (yaml is empty or unset); "
                "list results omit YAML, so fetch the full config with `get_discovery_config` first"
            )

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/discovery/configs/{config.id}/", data=data)
        updated = DiscoveryConfig.model_validate(response.json())
        config.is_valid = updated.is_valid
        config.validation_error = updated.validation_error
        config.validation_error_details = updated.validation_error_details
        config.modified = updated.modified
        logger.debug('Update of discovery config "%s" successful', config.name)
        return config

    def create_or_update_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Creates the config, or updates the existing one with the same name and type in the same project.

        With no `project` set, the server's only active project is used,
        and `DataMasqueException` is raised when it has no active project or more than one.
        Sets the config's `id` property.
        """

        existing_id = self._get_discovery_config_id_by_name(
            config.name, config.config_type, self._resolve_project(config.project)
        )
        if existing_id is not None:
            config.id = existing_id
            return self.update_discovery_config(config)

        return self.create_discovery_config(config)

    def delete_discovery_config_by_id_if_exists(self, config_id: DiscoveryConfigId) -> None:
        """
        Deletes the discovery config with the given ID.

        No-op if the config does not exist.
        """

        self._delete_if_exists(f"/api/discovery/configs/{config_id}/")

    def delete_discovery_config_by_name_if_exists(
        self, name: str, config_type: DiscoveryConfigType, project: Union[ProjectId, Project]
    ) -> None:
        """
        Deletes the discovery config with the given name and type in the given project.

        Config names are unique per type and project, so both are required to identify a single config.
        `project` is a `Project` or its `ProjectId`.
        No-op if no such config exists.
        """

        config_id = self._get_discovery_config_id_by_name(name, config_type, unwrap_project_id(project))
        if config_id is not None:
            self.delete_discovery_config_by_id_if_exists(config_id)

    def get_default_discovery_config_yaml(self) -> str:
        """Returns the server's built-in default discovery configuration as a YAML string."""

        response = self.make_request("GET", "/api/discovery/configs/defaults/")
        return response.content.decode("utf-8")
