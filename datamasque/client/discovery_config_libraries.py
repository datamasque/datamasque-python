import logging
from typing import Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.discovery_config import DiscoveryConfigType
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary, DiscoveryConfigLibraryId

logger = logging.getLogger(__name__)


class DiscoveryConfigLibraryClient(BaseClient):
    """Discovery config library CRUD API methods. Mixed into `DataMasqueClient`."""

    def list_discovery_config_libraries(self) -> list[DiscoveryConfigLibrary]:
        """
        Lists all discovery config libraries.

        Unlike most list endpoints, this one is not paginated;
        the server returns every library in a single response.
        Note: the YAML content is not included in the list response for performance.
        Use `get_discovery_config_library` to retrieve the full library with its YAML body.
        """

        response = self.make_request("GET", "/api/discovery/config-libraries/")
        return [DiscoveryConfigLibrary.model_validate(item) for item in response.json()]

    def get_discovery_config_library(self, library_id: DiscoveryConfigLibraryId) -> DiscoveryConfigLibrary:
        """Retrieves a single discovery config library by ID, including its YAML content."""

        response = self.make_request("GET", f"/api/discovery/config-libraries/{library_id}/")
        return DiscoveryConfigLibrary.model_validate(response.json())

    def _get_discovery_config_library_id_by_name(
        self, name: str, config_type: DiscoveryConfigType, namespace: str
    ) -> Optional[DiscoveryConfigLibraryId]:
        """
        Returns the ID of the library with the given name, type, and namespace, or `None` if there is none.

        The listing is filtered by name to keep the response small;
        type and namespace are matched client-side,
        since the API's namespace filter cannot select the default (empty) namespace.
        """

        response = self.make_request(
            "GET",
            "/api/discovery/config-libraries/",
            params={"name_exact": name},
        )
        entries = [DiscoveryConfigLibrary.model_validate(item) for item in response.json()]
        matches = [
            entry
            for entry in entries
            if entry.name == name and entry.namespace == namespace and entry.config_type is config_type
        ]
        if not matches:
            return None

        if matches[0].id is None:
            raise DataMasqueApiError(
                "Server returned a discovery config library list entry without an `id`.",
                response=response,
            )

        return matches[0].id

    def get_discovery_config_library_by_name(
        self, name: str, config_type: DiscoveryConfigType, namespace: str = ""
    ) -> Optional[DiscoveryConfigLibrary]:
        """
        Looks for a discovery config library matching the given name, type, and namespace (case-sensitive, exact match).

        Library names are unique per type within a namespace,
        so a type is required to identify a single library.
        Returns it (with full YAML content) if found, otherwise `None`.
        """

        library_id = self._get_discovery_config_library_id_by_name(name, config_type, namespace)
        if library_id is None:
            return None

        return self.get_discovery_config_library(library_id)

    def create_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Creates a new discovery config library on the server.

        Sets the library's server-assigned fields
        (`id`, `is_valid`, `validation_error`, `created`, `modified`) and returns the library.
        """

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/discovery/config-libraries/", data=data)
        created = DiscoveryConfigLibrary.model_validate(response.json())
        library.id = created.id
        library.is_valid = created.is_valid
        library.validation_error = created.validation_error
        library.created = created.created
        library.modified = created.modified
        logger.info('Creation of discovery config library "%s" successful', library.name)
        return library

    def update_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Performs a full update of the discovery config library.

        The library must have its `id` set (i.e., it must have been previously created or retrieved from the server)
        and its `yaml` content present.
        A library's `config_type` is fixed at creation and cannot be changed by an update.
        """

        if library.id is None:
            raise ValueError("Cannot update a discovery config library that has not been created yet (id is None)")

        if library.yaml is None:
            raise ValueError(
                "Cannot update a discovery config library without YAML content (yaml is None); "
                "list results omit YAML, so fetch the full library with `get_discovery_config_library` first"
            )

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/discovery/config-libraries/{library.id}/", data=data)
        updated = DiscoveryConfigLibrary.model_validate(response.json())
        library.is_valid = updated.is_valid
        library.validation_error = updated.validation_error
        library.modified = updated.modified
        logger.debug('Update of discovery config library "%s" successful', library.name)
        return library

    def create_or_update_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Creates the library, or updates the existing one with the same name, namespace, and config type.

        Sets the library's `id` property.
        """

        library_id = self._get_discovery_config_library_id_by_name(library.name, library.config_type, library.namespace)
        if library_id is not None:
            library.id = library_id
            return self.update_discovery_config_library(library)

        return self.create_discovery_config_library(library)

    def delete_discovery_config_library_by_id_if_exists(
        self, library_id: DiscoveryConfigLibraryId, *, force: bool = False
    ) -> None:
        """
        Deletes the discovery config library with the given ID.

        No-op if the library does not exist.

        If the library is imported by any discovery configs,
        the server will return 409 Conflict unless `force=True` is passed.
        """

        params = {"force": "true"} if force else None
        self._delete_if_exists(f"/api/discovery/config-libraries/{library_id}/", params=params)

    def delete_discovery_config_library_by_name_if_exists(
        self, name: str, config_type: DiscoveryConfigType, namespace: str = "", *, force: bool = False
    ) -> None:
        """
        Deletes the discovery config library with the given name, type, and namespace.

        Library names are unique per type within a namespace,
        so a type is required to identify a single library.
        No-op if no such library exists.
        """

        library_id = self._get_discovery_config_library_id_by_name(name, config_type, namespace)
        if library_id is not None:
            self.delete_discovery_config_library_by_id_if_exists(library_id, force=force)
