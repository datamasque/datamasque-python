from typing import Optional

from requests import Response

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import InstanceSecretLinkError, InstanceSecretLinkErrorCode
from datamasque.client.models.instance_secret import (
    InFlightSyncResult,
    InstanceSecretLinkHistory,
    InstanceSecretLinkRequest,
    InstanceSecretLinkRequestVerification,
    InstanceSecretLinkResponse,
    InstanceSecretLinkResponseVerification,
)


class InstanceSecretClient(BaseClient):
    """
    Instance secret and instance secret linking API methods, mixed into `DataMasqueClient`.

    Linking gives two DataMasque instances the same instance secret, so that they produce the same hashed output.
    The joining instance creates a request code, the sharing instance accepts it and returns a response code,
    and the joining instance accepts that response code and adopts the instance secret in it.

    Compare the fingerprints from both DataMasque instances before each accept step.
    That comparison protects against a code being swapped on its way between the instances,
    so when the codes travel through another person or system, compare the fingerprints over a separate channel.

    Example usage, with one client for each DataMasque instance:

    .. code-block:: python

        # On the joining instance: create a request code.
        link_request = joining_client.create_instance_secret_link_request()

        # On the sharing instance: check the request fingerprint, then share the instance secret.
        request_verification = sharing_client.verify_instance_secret_link_request(link_request.request_code)
        assert request_verification.fingerprint == link_request.fingerprint
        link_response = sharing_client.accept_instance_secret_link_request(link_request.request_code)

        # On the joining instance: check the response fingerprint, then adopt the instance secret.
        response_verification = joining_client.verify_instance_secret_link_response(link_response.response_code)
        assert response_verification.response_fingerprint == link_response.response_fingerprint
        joining_client.accept_instance_secret_link_response(
            link_response.response_code,
            response_verification.verification_token,
        )
    """

    def get_instance_secret_fingerprint(self) -> str:
        """
        Returns the fingerprint of the instance secret of this DataMasque instance.

        Two DataMasque instances have the same fingerprint only when they have the same instance secret.
        """

        response = self.make_request("GET", "/api/instance-secret/")
        fingerprint: str = response.json()["fingerprint"]
        return fingerprint

    def get_instance_secret_link_history(self) -> InstanceSecretLinkHistory:
        """Returns every event of the instance secret of this DataMasque instance, and the links that are current."""

        response = self.make_request("GET", "/api/instance-secret/link/history/")
        return InstanceSecretLinkHistory.model_validate(response.json())

    def create_instance_secret_link_request(self) -> InstanceSecretLinkRequest:
        """
        Creates a request code for joining the instance secret of another DataMasque instance.

        Pass the request code to `accept_instance_secret_link_request` on the sharing DataMasque instance.
        The new request code replaces the pending one, which can then no longer be used.
        """

        response = self.make_request("POST", "/api/instance-secret/link/request/")
        return InstanceSecretLinkRequest.model_validate(response.json())

    def get_pending_instance_secret_link_request(self) -> Optional[InstanceSecretLinkRequest]:
        """
        Returns the pending request code of this DataMasque instance.

        Returns `None` when there is no request, or it has expired, been cancelled or been used.
        """

        response = self.make_request("GET", "/api/instance-secret/link/request/", require_status_check=False)
        if response.status_code == 404:
            return None

        self._raise_for_status(response)
        return InstanceSecretLinkRequest.model_validate(response.json())

    def cancel_instance_secret_link_request(self) -> None:
        """Cancels the pending request code, so that it can no longer be used, or does nothing if there is none."""

        self.make_request("DELETE", "/api/instance-secret/link/request/")

    def verify_instance_secret_link_request(self, request_code: str) -> InstanceSecretLinkRequestVerification:
        """
        Verifies a request code from the joining DataMasque instance, without changing anything.

        Call this on the sharing DataMasque instance.

        Raises:
            ValueError: `request_code` is empty.
            InstanceSecretLinkError: the request code is not valid.
        """

        response = self._post_link_step("/api/instance-secret/link/request/verify/", {"request_code": request_code})
        return InstanceSecretLinkRequestVerification.model_validate(response.json())

    def accept_instance_secret_link_request(self, request_code: str) -> InstanceSecretLinkResponse:
        """
        Shares the instance secret of this DataMasque instance with the instance that created the request code.

        Call this on the sharing DataMasque instance,
        and pass the response code to `verify_instance_secret_link_response` on the joining instance.

        Raises:
            ValueError: `request_code` is empty.
            InstanceSecretLinkError: the request code is not valid.
        """

        response = self._post_link_step("/api/instance-secret/link/request/accept/", {"request_code": request_code})
        return InstanceSecretLinkResponse.model_validate(response.json())

    def verify_instance_secret_link_response(self, response_code: str) -> InstanceSecretLinkResponseVerification:
        """
        Verifies a response code from the sharing DataMasque instance, without changing anything.

        Call this on the joining DataMasque instance,
        and pass the returned `verification_token` to `accept_instance_secret_link_response`.

        Raises:
            ValueError: `response_code` is empty.
            InstanceSecretLinkError: the response code is not valid.
        """

        response = self._post_link_step(
            "/api/instance-secret/link/response/verify/",
            {"response_code": response_code},
        )
        return InstanceSecretLinkResponseVerification.model_validate(response.json())

    def accept_instance_secret_link_response(self, response_code: str, verification_token: str) -> InFlightSyncResult:
        """
        Accepts the response code and adopts its instance secret as the instance secret of this DataMasque instance.

        Call this on the joining DataMasque instance,
        with the `verification_token` from `verify_instance_secret_link_response` for the same response code.
        Once this succeeds, masking runs use the new instance secret.
        Returns an `InFlightSyncResult` stating whether the IFM service successfully received the new instance secret.
        If DataMasque fails to notify the IFM service,
        a DataMasque restart is required for in-flight masking to use the new instance secret.

        Raises:
            ValueError: `response_code` or `verification_token` is empty.
            InstanceSecretLinkError: the response code is not valid, runs are in progress,
              or the instance secret or its history changed after the response code was verified.
        """

        response = self._post_link_step(
            "/api/instance-secret/link/response/accept/",
            {"response_code": response_code, "verification_token": verification_token},
        )
        return InFlightSyncResult(response.json()["in_flight_sync"])

    def _post_link_step(self, path: str, data: dict[str, str]) -> Response:
        for name, value in data.items():
            if not value:
                raise ValueError(f"`{name}` must not be empty.")

        response = self.make_request("POST", path, data=data, require_status_check=False)
        if response.status_code == 400:
            # A 400 that is not a link error, or has a code that this version of the client does not know,
            # is raised as a plain API error.
            try:
                error = response.json()
                code = InstanceSecretLinkErrorCode(error["code"])
                message = error["message"]
            except (ValueError, KeyError, TypeError):
                pass
            else:
                raise InstanceSecretLinkError(message, code=code, response=response)

        self._raise_for_status(response, request_data=data)
        return response
