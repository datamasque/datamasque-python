import logging
from typing import Iterator, Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.pagination import Page
from datamasque.client.models.project import ProjectId, ProjectIdentifier, unwrap_project_id
from datamasque.client.models.rg_config import RGConfig, RGConfigId

logger = logging.getLogger(__name__)


class RGConfigClient(BaseClient):
    """Ruleset-generation (RG) config CRUD API methods. Mixed into `DataMasqueClient`."""

    def iter_rg_configs(self) -> Iterator[RGConfig]:
        """Lazily iterate all RG configs via the paginated endpoint."""

        return self._iter_paginated("/api/ruleset-generation-configs/", model=RGConfig)

    def list_rg_configs(self) -> list[RGConfig]:
        """
        Lists all RG configs.

        Note: the YAML content is not included in the list response for performance.
        Use `get_rg_config` to retrieve the full config with its YAML body.
        """

        return list(self.iter_rg_configs())

    def get_rg_config(self, config_id: RGConfigId) -> RGConfig:
        """Retrieves a single RG config by ID."""

        response = self.make_request("GET", f"/api/ruleset-generation-configs/{config_id}/")
        return RGConfig.model_validate(response.json())

    def _get_rg_config_id_by_name(self, name: str, project: Union[ProjectId, str]) -> Optional[RGConfigId]:
        """Return the id of the config matching the name and project via a single list request, or `None`."""

        response = self.make_request(
            "GET",
            "/api/ruleset-generation-configs/",
            params={"name_exact": name, "project": project, "limit": 1},
        )
        page = Page[RGConfig].model_validate(response.json())
        if not page.results:
            return None

        config_id = page.results[0].id
        if config_id is None:
            raise DataMasqueApiError(
                "Server returned an RG config list entry without an `id`.",
                response=response,
            )

        return config_id

    def get_rg_config_by_name(self, name: str, project: ProjectIdentifier) -> Optional[RGConfig]:
        """
        Looks for an RG config matching the given name (case-sensitive, exact match) in the given project.

        RG configs are untyped and their names are unique per project.
        `project` is a `Project`, its `ProjectId` or its `<id>-<slug>` form.
        Returns it if found, otherwise `None`.
        """

        config_id = self._get_rg_config_id_by_name(name, unwrap_project_id(project))
        if config_id is None:
            return None

        return self.get_rg_config(config_id)

    def create_rg_config(self, config: RGConfig) -> RGConfig:
        """
        Creates a new RG config on the server.

        Sets the config's server-assigned fields
        (`id`, `is_valid`, `validation_errors`, `created`, `modified`) and returns the config.
        """

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/ruleset-generation-configs/", data=data)
        created = RGConfig.model_validate(response.json())
        config.id = created.id
        config.project = created.project
        config.is_valid = created.is_valid
        config.validation_errors = created.validation_errors
        config.created = created.created
        config.modified = created.modified
        logger.info('Creation of RG config "%s" successful', config.name)
        return config

    def update_rg_config(self, config: RGConfig) -> RGConfig:
        """
        Performs a full update of the RG config.

        The config must have its `id` set
        (i.e., it must have been previously created or retrieved from the server)
        and its `yaml` content present.
        """

        if config.id is None:
            raise ValueError("Cannot update an RG config that has not been created yet (id is None)")

        if config.yaml is None:
            raise ValueError(
                "Cannot update an RG config without YAML content (yaml is None); "
                "list results omit YAML, so fetch the full config with `get_rg_config` first"
            )

        data = config.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/ruleset-generation-configs/{config.id}/", data=data)
        updated = RGConfig.model_validate(response.json())
        config.is_valid = updated.is_valid
        config.validation_errors = updated.validation_errors
        config.modified = updated.modified
        logger.debug('Update of RG config "%s" successful', config.name)
        return config

    def create_or_update_rg_config(self, config: RGConfig) -> RGConfig:
        """
        Creates the config, or updates the existing one with the same name in the same project.

        With no `project` set, the server's only active project is used,
        and `DataMasqueException` is raised when it has no active project or more than one.
        Sets the config's `id` property.
        """

        existing_id = self._get_rg_config_id_by_name(config.name, self._resolve_project(config.project))
        if existing_id is not None:
            config.id = existing_id
            return self.update_rg_config(config)

        return self.create_rg_config(config)

    def delete_rg_config_by_id_if_exists(self, config_id: RGConfigId) -> None:
        """
        Deletes the RG config with the given ID.

        No-op if the config does not exist.
        """

        self._delete_if_exists(f"/api/ruleset-generation-configs/{config_id}/")

    def delete_rg_config_by_name_if_exists(self, name: str, project: ProjectIdentifier) -> None:
        """
        Deletes the RG config with the given name in the given project.

        `project` is a `Project`, its `ProjectId` or its `<id>-<slug>` form.
        No-op if no such config exists.
        """

        config_id = self._get_rg_config_id_by_name(name, unwrap_project_id(project))
        if config_id is not None:
            self.delete_rg_config_by_id_if_exists(config_id)

    def get_default_rg_config_yaml(self) -> str:
        """Returns the server's built-in default RG configuration as a YAML string."""

        response = self.make_request("GET", "/api/ruleset-generation-configs/defaults/")
        return response.content.decode("utf-8")
