import logging

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
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
        Creates or updates a ruleset.

        Populates the given ruleset's `id`, `is_valid`, `validation_error`, `validation_error_type`,
        and `git` fields from the server response, and returns the same ruleset instance for convenience.
        """

        data = ruleset.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/rulesets/", data=data, params={"upsert": "true"})
        created = Ruleset.model_validate(response.json())
        ruleset.id = created.id
        ruleset.is_valid = created.is_valid
        ruleset.validation_error = created.validation_error
        ruleset.validation_error_type = created.validation_error_type
        ruleset.git = created.git

        if response.status_code == 201:
            logger.info('Creation of ruleset "%s" successful', ruleset.name)
        elif response.status_code == 200:
            logger.debug('Update of ruleset "%s" successful', ruleset.name)

        return ruleset

    def delete_ruleset_by_id_if_exists(self, ruleset_id: RulesetId) -> None:
        """Deletes the ruleset with the given ID. No-op if the ruleset does not exist."""

        self._delete_if_exists(f"/api/rulesets/{ruleset_id}/")

    def delete_ruleset_by_name_if_exists(self, ruleset_name: str, ruleset_type: RulesetType) -> None:
        """
        Deletes the ruleset with the given name and type.

        Ruleset names are unique per type, so a type is required to identify a single ruleset.
        No-op if no such ruleset exists.
        """

        matching = [
            ruleset
            for ruleset in self.list_rulesets()
            if ruleset.name == ruleset_name and ruleset.ruleset_type is ruleset_type
        ]
        for ruleset in matching:
            if ruleset.id is None:
                raise DataMasqueException(f'Server returned a ruleset named "{ruleset.name}" without an `id`.')

            self.delete_ruleset_by_id_if_exists(ruleset.id)
