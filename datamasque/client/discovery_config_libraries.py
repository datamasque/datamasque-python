import logging
from typing import Iterator, Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary, DiscoveryConfigLibraryId
from datamasque.client.models.pagination import Page

logger = logging.getLogger(__name__)


class DiscoveryConfigLibraryClient(BaseClient):
    """Discovery config library CRUD API methods. Mixed into `DataMasqueClient`."""

    def iter_discovery_config_libraries(self) -> Iterator[DiscoveryConfigLibrary]:
        """Lazily iterate all discovery config libraries via the paginated endpoint."""

        return self._iter_paginated("/api/discovery/config-libraries/", model=DiscoveryConfigLibrary)

    def list_discovery_config_libraries(self) -> list[DiscoveryConfigLibrary]:
        """
        Lists all (non-archived) discovery config libraries.

        Note: the YAML content is not included in the list response for performance.
        Use `get_discovery_config_library` to retrieve the full library with its YAML body.
        """

        return list(self.iter_discovery_config_libraries())

    def get_discovery_config_library(self, library_id: DiscoveryConfigLibraryId) -> DiscoveryConfigLibrary:
        """Retrieves a single discovery config library by ID, including its YAML content."""

        response = self.make_request("GET", f"/api/discovery/config-libraries/{library_id}/")
        return DiscoveryConfigLibrary.model_validate(response.json())

    def get_discovery_config_library_by_name(self, name: str, namespace: str = "") -> Optional[DiscoveryConfigLibrary]:
        """
        Looks for a discovery config library matching the given name and namespace (case-sensitive, exact match).

        Returns it (with full YAML content) if found, otherwise `None`.
        """

        response = self.make_request(
            "GET",
            "/api/discovery/config-libraries/",
            params={"name_exact": name, "namespace_exact": namespace, "limit": 1},
        )
        page = Page[DiscoveryConfigLibrary].model_validate(response.json())
        if not page.results:
            return None

        library_id = page.results[0].id
        if library_id is None:
            raise DataMasqueApiError(
                "Server returned a discovery config library list entry without an `id`.",
                response=response,
            )

        return self.get_discovery_config_library(library_id)

    def create_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Creates a new discovery config library on the server.

        Sets the library's server-assigned fields (`id`, `is_valid`, `created`, `modified`) and returns the library.
        """

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/discovery/config-libraries/", data=data)
        created = DiscoveryConfigLibrary.model_validate(response.json())
        library.id = created.id
        library.is_valid = created.is_valid
        library.created = created.created
        library.modified = created.modified
        logger.info('Creation of discovery config library "%s" successful', library.name)
        return library

    def update_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Performs a full update of the discovery config library.

        The library must have its `id` set (i.e., it must have been previously created or retrieved from the server).
        """

        if library.id is None:
            raise ValueError("Cannot update a discovery config library that has not been created yet (id is None)")

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/discovery/config-libraries/{library.id}/", data=data)
        updated = DiscoveryConfigLibrary.model_validate(response.json())
        library.is_valid = updated.is_valid
        library.modified = updated.modified
        logger.debug('Update of discovery config library "%s" successful', library.name)
        return library

    def create_or_update_discovery_config_library(self, library: DiscoveryConfigLibrary) -> DiscoveryConfigLibrary:
        """
        Creates the library if it doesn't exist, or updates it if one with the same name and namespace already exists.

        Sets the library's `id` property.
        """

        existing = self.get_discovery_config_library_by_name(library.name, library.namespace)
        if existing is not None:
            library.id = existing.id
            return self.update_discovery_config_library(library)

        return self.create_discovery_config_library(library)

    def delete_discovery_config_library_by_id_if_exists(
        self, library_id: DiscoveryConfigLibraryId, *, force: bool = False
    ) -> None:
        """
        Deletes (archives) the discovery config library with the given ID.

        No-op if the library does not exist.

        If the library is imported by any discovery configs,
        the server will return 409 Conflict unless `force=True` is passed.
        """

        params = {"force": "true"} if force else None
        self._delete_if_exists(f"/api/discovery/config-libraries/{library_id}/", params=params)

    def delete_discovery_config_library_by_name_if_exists(
        self, name: str, namespace: str = "", *, force: bool = False
    ) -> None:
        """
        Deletes the discovery config library with the given name and namespace.

        No-op if the library does not exist.
        """

        all_libraries = self.list_discovery_config_libraries()
        matching = [lib for lib in all_libraries if lib.name == name and lib.namespace == namespace]
        for lib in matching:
            if lib.id is None:
                raise DataMasqueException(
                    f'Server returned a discovery config library named "{lib.name}" without an `id`.'
                )

            self.delete_discovery_config_library_by_id_if_exists(lib.id, force=force)
