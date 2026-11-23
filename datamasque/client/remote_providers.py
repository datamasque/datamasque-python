import logging
from collections.abc import Mapping
from typing import Optional

from datamasque.client.base import BaseClient
from datamasque.client.models.remote_provider import (
    ParameterValue,
    RemoteProvider,
    RemoteProviderId,
    RemoteProviderTestResult,
)

logger = logging.getLogger(__name__)


def _request_body(remote_provider: RemoteProvider) -> dict:
    """Return the provider as a request body, leaving out `secret` when it is `None` so the saved one is kept."""

    data = remote_provider.model_dump(by_alias=True, mode="json")
    if data.get("secret") is None:
        data.pop("secret", None)
    return data


class RemoteProviderClient(BaseClient):
    """
    Remote provider API methods. Mixed into `DataMasqueClient`.

    Every method needs an administrator: the server answers other users with 403.
    """

    def list_remote_providers(self) -> list[RemoteProvider]:
        """
        Lists all remote providers, by name.

        The endpoint is not paginated; the server returns every provider in a single response.
        """

        response = self.make_request("GET", "/api/remote-providers/")
        return [RemoteProvider.model_validate(item) for item in response.json()]

    def get_remote_provider(self, remote_provider_id: RemoteProviderId) -> RemoteProvider:
        """Retrieves a single remote provider by ID."""

        response = self.make_request("GET", f"/api/remote-providers/{remote_provider_id}/")
        return RemoteProvider.model_validate(response.json())

    def get_remote_provider_by_name(self, name: str) -> Optional[RemoteProvider]:
        """
        Looks for the remote provider with the given name (case-sensitive, exact match).

        Names are unique, so at most one can match.
        Returns it if found, otherwise `None`.
        """

        for remote_provider in self.list_remote_providers():
            if remote_provider.name == name:
                return remote_provider

        return None

    def create_remote_provider(self, remote_provider: RemoteProvider) -> RemoteProvider:
        """
        Creates a new remote provider on the server, and returns it as the server stored it.

        Sets the `id` of `remote_provider`.
        The returned provider has no `secret`; its `has_secret` says whether one is saved.
        """

        response = self.make_request("POST", "/api/remote-providers/", data=_request_body(remote_provider))
        created = RemoteProvider.model_validate(response.json())
        remote_provider.id = created.id
        logger.info('Creation of remote provider "%s" successful', created.name)
        return created

    def update_remote_provider(self, remote_provider: RemoteProvider) -> RemoteProvider:
        """
        Performs a full update of the remote provider, and returns it as the server stored it.

        The provider must have its `id` set
        (i.e., it must have been previously created or retrieved from the server).
        With `secret` left as `None` the server keeps the saved secret,
        except that it refuses a change of the base URL or the token URL,
        or of certificate verification to `none`, without a new secret.
        The name cannot change.
        """

        if remote_provider.id is None:
            raise ValueError("Cannot update a remote provider that has not been created yet (id is None)")

        response = self.make_request(
            "PUT", f"/api/remote-providers/{remote_provider.id}/", data=_request_body(remote_provider)
        )
        updated = RemoteProvider.model_validate(response.json())
        logger.debug('Update of remote provider "%s" successful', updated.name)
        return updated

    def create_or_update_remote_provider(self, remote_provider: RemoteProvider) -> RemoteProvider:
        """
        Creates the remote provider, or updates the existing one with the same name.

        Sets the `id` of `remote_provider`, and returns the provider as the server stored it.
        """

        existing = self.get_remote_provider_by_name(remote_provider.name)
        if existing is None:
            return self.create_remote_provider(remote_provider)

        remote_provider.id = existing.id
        return self.update_remote_provider(remote_provider)

    def delete_remote_provider_by_id_if_exists(self, remote_provider_id: RemoteProviderId) -> None:
        """
        Deletes the remote provider with the given ID. No-op if the provider does not exist.

        Rulesets that use the provider fail to validate until a provider with its name is created again.
        """

        self._delete_if_exists(f"/api/remote-providers/{remote_provider_id}/")

    def delete_remote_provider_by_name_if_exists(self, name: str) -> None:
        """Deletes the remote provider with the given name. No-op if no such provider exists."""

        remote_provider = self.get_remote_provider_by_name(name)
        if remote_provider is not None and remote_provider.id is not None:
            self.delete_remote_provider_by_id_if_exists(remote_provider.id)

    def test_remote_provider(
        self,
        remote_provider_id: RemoteProviderId,
        value: str,
        params: Optional[Mapping[str, ParameterValue]] = None,
    ) -> RemoteProviderTestResult:
        """
        Sends one value, with the provider's parameters, to the remote provider.

        Returns the provider's output, or the error that stopped it, such as a TLS failure,
        an HTTP status or a timeout.
        The server does not save or log the value.
        A missing required parameter, or one the provider does not declare, raises `DataMasqueApiError` (400).
        """

        response = self.make_request(
            "POST",
            f"/api/remote-providers/{remote_provider_id}/test/",
            data={"value": value, "params": dict(params or {})},
        )
        return RemoteProviderTestResult.model_validate(response.json())
