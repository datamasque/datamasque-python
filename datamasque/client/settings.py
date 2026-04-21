from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueUserError


class SettingsClient(BaseClient):
    """Server-wide settings, log retrieval, and admin-install bootstrap. Mixed into `DataMasqueClient`."""

    def retrieve_application_logs(self, output_path: Path) -> None:
        """Downloads the DataMasque application logs archive to `output_path`."""

        response = self.make_request("GET", path="/api/logs/download/", params={"log_service": "application"})

        with open(output_path, "wb") as application_logs_output:
            for chunk in response.iter_content(chunk_size=4096):
                application_logs_output.write(chunk)

    def set_locality(self, locality: str) -> None:
        """Sets the server-wide locality used for ruleset generation and Jinja2 interpolation of ruleset YAML."""

        self.make_request("PATCH", path="api/settings/", data={"locality": locality})

    def admin_install(
        self,
        email: str,
        username: str = "admin",
        password: Optional[str] = None,
        allowed_hosts: Optional[list[str]] = None,
    ) -> None:
        """
        Performs the first-time admin-install bootstrap on a fresh DataMasque server.

        Creates the initial admin account and configures the server's allowed-hosts list.
        This endpoint is unauthenticated and can only be called once per server;
        subsequent calls will fail.

        If `password` is not given, the client's configured password is used.
        If `allowed_hosts` is not given, it defaults to the following list:

          - `localhost`
          - `127.0.0.1`
          - the client's configured hostname (from `base_url`).
        """

        if password is None:
            password = self.password
            if password is None:
                # Clients constructed with `token_source` instead of a password
                # have no fallback to use here; require an explicit `password` argument.
                raise DataMasqueUserError(
                    "`admin_install` requires a `password` argument when the client was constructed without one."
                )

        if allowed_hosts is None:
            allowed_hosts = ["localhost", "127.0.0.1"]
            dm_hostname = urlparse(self.base_url).hostname
            if dm_hostname and dm_hostname not in allowed_hosts:
                allowed_hosts.append(dm_hostname)

        data = {
            "email": email,
            "username": username,
            "password": password,
            "re_password": password,
            "allowed_hosts": allowed_hosts,
        }

        self.make_request(
            "POST",
            "/api/users/admin-install/",
            data=data,
            requires_authorization=False,
        )
