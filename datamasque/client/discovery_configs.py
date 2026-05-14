import logging
from typing import Iterator, Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
from datamasque.client.models.discovery_config import DiscoveryConfig, DiscoveryConfigId
from datamasque.client.models.pagination import Page

logger = logging.getLogger(__name__)


class DiscoveryConfigClient(BaseClient):
    """Discovery config CRUD API methods. Mixed into `DataMasqueClient`."""

    def iter_discovery_configs(self) -> Iterator[DiscoveryConfig]:
        """Lazily iterate all discovery configs via the paginated endpoint."""

        return self._iter_paginated("/api/discovery/configs/", model=DiscoveryConfig)

    def list_discovery_configs(self) -> list[DiscoveryConfig]:
        """
        Lists all (non-archived) discovery configs.

        Note: the YAML content is not included in the list response for performance.
        Use `get_discovery_config` to retrieve the full config with its YAML body.
        """

        return list(self.iter_discovery_configs())

    def get_discovery_config(self, config_id: DiscoveryConfigId) -> DiscoveryConfig:
        """Retrieves a single discovery config by ID."""

        response = self.make_request("GET", f"/api/discovery/configs/{config_id}/")
        return DiscoveryConfig.model_validate(response.json())

    def get_discovery_config_by_name(self, name: str) -> Optional[DiscoveryConfig]:
        """
        Looks for a discovery config matching the given name (case-sensitive, exact match).

        Returns it if found, otherwise `None`.
        """

        response = self.make_request(
            "GET",
            "/api/discovery/configs/",
            params={"name_exact": name, "limit": 1},
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

        return self.get_discovery_config(config_id)

    def create_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Creates a new discovery config on the server.

        Sets the config's server-assigned fields (`id`, `created`, `modified`) and returns the config.
        """

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/discovery/configs/", data=data)
        created = DiscoveryConfig.model_validate(response.json())
        config.id = created.id
        config.created = created.created
        config.modified = created.modified
        logger.info('Creation of discovery config "%s" successful', config.name)
        return config

    def update_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Performs a full update of the discovery config.

        The config must have its `id` set
        (i.e., it must have been previously created or retrieved from the server).
        """

        if config.id is None:
            raise ValueError("Cannot update a discovery config that has not been created yet (id is None)")

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/discovery/configs/{config.id}/", data=data)
        updated = DiscoveryConfig.model_validate(response.json())
        config.modified = updated.modified
        logger.debug('Update of discovery config "%s" successful', config.name)
        return config

    def create_or_update_discovery_config(self, config: DiscoveryConfig) -> DiscoveryConfig:
        """
        Creates the config if it doesn't exist, or updates it if one with the same name already exists.

        Sets the config's `id` property.
        """

        existing = self.get_discovery_config_by_name(config.name)
        if existing is not None:
            config.id = existing.id
            return self.update_discovery_config(config)

        return self.create_discovery_config(config)

    def delete_discovery_config_by_id_if_exists(self, config_id: DiscoveryConfigId) -> None:
        """
        Archives the discovery config with the given ID.

        No-op if the config does not exist.
        """

        self._delete_if_exists(f"/api/discovery/configs/{config_id}/")

    def delete_discovery_config_by_name_if_exists(self, name: str) -> None:
        """
        Archives the discovery config with the given name.

        No-op if the config does not exist.
        """

        all_configs = self.list_discovery_configs()
        matching = [config for config in all_configs if config.name == name]
        for config in matching:
            if config.id is None:
                raise DataMasqueException(f'Server returned a discovery config named "{config.name}" without an `id`.')

            self.delete_discovery_config_by_id_if_exists(config.id)

    def get_default_discovery_config_yaml(self) -> str:
        """Returns the server's built-in default discovery configuration as a YAML string."""

        response = self.make_request("GET", "/api/discovery/configs/defaults/")
        return response.content.decode("utf-8")
