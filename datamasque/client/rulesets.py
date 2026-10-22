import logging
from typing import Union

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.project import Project, ProjectId, unwrap_project_id
from datamasque.client.models.ruleset import Ruleset, RulesetId, RulesetType

logger = logging.getLogger(__name__)


class RulesetClient(BaseClient):
    """Ruleset CRUD API methods. Mixed into `DataMasqueClient`."""

    def list_rulesets(self) -> list[Ruleset]:
        """Returns all rulesets configured on the server."""

        response = self.make_request("GET", "/api/v2/rulesets/")
        return [Ruleset.model_validate(payload) for payload in response.json()]

    def create_or_update_ruleset(self, ruleset: Ruleset) -> Ruleset:
        """
        Creates the ruleset, or updates the existing one with the same name and type in the same project.

        With no `project` set, the client's `default_project` is used, else the server's only active project,
        and `DataMasqueException` is raised when it has no active project or more than one.
        Populates the given ruleset's `id`, `is_valid`, `validation_errors`, `unlicensed_feature_warnings`,
        and `git` fields from the server response, and returns the same ruleset instance for convenience.
        """

        # The server finds the existing ruleset itself, so this only checks that an unset project resolves.
        self._resolve_project(ruleset.project)
        data = ruleset.model_dump(exclude_none=True, by_alias=True, mode="json") | self._default_project_field(
            ruleset.project
        )
        response = self.make_request("POST", "/api/rulesets/", data=data, params={"upsert": "true"})
        created = Ruleset.model_validate(response.json())
        ruleset.id = created.id
        ruleset.project = created.project
        ruleset.is_valid = created.is_valid
        ruleset.validation_errors = created.validation_errors
        ruleset.unlicensed_feature_warnings = created.unlicensed_feature_warnings
        ruleset.git = created.git

        if response.status_code == 201:
            logger.info('Creation of ruleset "%s" successful', ruleset.name)
        elif response.status_code == 200:
            logger.debug('Update of ruleset "%s" successful', ruleset.name)

        return ruleset

    def delete_ruleset_by_id_if_exists(self, ruleset_id: RulesetId) -> None:
        """Deletes the ruleset with the given ID. No-op if the ruleset does not exist."""

        self._delete_if_exists(f"/api/rulesets/{ruleset_id}/")

    def delete_ruleset_by_name_if_exists(
        self, ruleset_name: str, ruleset_type: RulesetType, project: Union[ProjectId, Project]
    ) -> None:
        """
        Deletes the ruleset with the given name and type in the given project.

        Ruleset names are unique per type and project, so both are required to identify a single ruleset.
        `project` is a `Project` or its `ProjectId`.
        No-op if no such ruleset exists.
        """

        response = self.make_request(
            "GET",
            "/api/v2/rulesets/",
            params={"name_exact": ruleset_name, "mask_type": ruleset_type.value, "project": unwrap_project_id(project)},
        )
        for ruleset in (Ruleset.model_validate(payload) for payload in response.json()):
            if ruleset.id is None:
                raise DataMasqueException(f'Server returned a ruleset named "{ruleset.name}" without an `id`.')

            self.delete_ruleset_by_id_if_exists(ruleset.id)
