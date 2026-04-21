import logging

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.ruleset import Ruleset, RulesetId
from datamasque.client.models.status import ValidationStatus

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

        Populates the given ruleset's `id` and `is_valid` fields from the server response,
        and returns the same ruleset instance for convenience.
        """

        data = ruleset.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/rulesets/", data=data, params={"upsert": "true"})
        response_data = response.json()
        ruleset.id = RulesetId(response_data["id"])
        is_valid = response_data.get("is_valid")
        if is_valid is not None:
            ruleset.is_valid = ValidationStatus(is_valid)

        if response.status_code == 201:
            logger.info('Creation of ruleset "%s" successful', ruleset.name)
        elif response.status_code == 200:
            logger.debug('Update of ruleset "%s" successful', ruleset.name)

        return ruleset

    def delete_ruleset_by_id_if_exists(self, ruleset_id: RulesetId) -> None:
        """Deletes the ruleset with the given ID. No-op if the ruleset does not exist."""

        self._delete_if_exists(f"/api/rulesets/{ruleset_id}/")

    def delete_ruleset_by_name_if_exists(self, ruleset_name: str) -> None:
        """Deletes the ruleset with the given name. No-op if the ruleset does not exist."""

        all_rulesets = self.list_rulesets()
        rulesets_matching_name = [ruleset for ruleset in all_rulesets if ruleset.name == ruleset_name]
        for ruleset in rulesets_matching_name:
            if ruleset.id is None:
                raise DataMasqueException(f'Server returned a ruleset named "{ruleset.name}" without an `id`.')

            self.delete_ruleset_by_id_if_exists(ruleset.id)
