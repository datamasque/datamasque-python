import logging
from typing import Iterator, Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueException
from datamasque.client.models.pagination import Page
from datamasque.client.models.ruleset import Ruleset
from datamasque.client.models.ruleset_library import RulesetLibrary, RulesetLibraryId

logger = logging.getLogger(__name__)


class RulesetLibraryClient(BaseClient):
    """Ruleset library CRUD API methods. Mixed into `DataMasqueClient`."""

    def iter_ruleset_libraries(self) -> Iterator[RulesetLibrary]:
        """Lazily iterate all ruleset libraries via paginated endpoint."""

        return self._iter_paginated("/api/ruleset-libraries/", model=RulesetLibrary)

    def list_ruleset_libraries(self) -> list[RulesetLibrary]:
        """
        Lists all ruleset libraries.

        Note: The YAML content is not included in the list response for performance.
        Use `get_ruleset_library` to retrieve the full library with YAML content.
        """

        return list(self.iter_ruleset_libraries())

    def get_ruleset_library(self, library_id: RulesetLibraryId) -> RulesetLibrary:
        """Retrieves a single ruleset library by ID, including its YAML content."""

        response = self.make_request("GET", f"/api/ruleset-libraries/{library_id}/")
        return RulesetLibrary.model_validate(response.json())

    def get_ruleset_library_by_name(self, name: str, namespace: str = "") -> Optional[RulesetLibrary]:
        """
        Looks for a ruleset library matching the given name and namespace (case-sensitive, exact match).

        Returns it (with full YAML content) if found, otherwise None.
        """

        response = self.make_request(
            "GET",
            "/api/ruleset-libraries/",
            params={"name_exact": name, "namespace_exact": namespace, "limit": 1},
        )
        page = Page[RulesetLibrary].model_validate(response.json())
        if not page.results:
            return None

        library_id = page.results[0].id
        if library_id is None:
            raise DataMasqueApiError(
                "Server returned a ruleset library list entry without an `id`.",
                response=response,
            )

        return self.get_ruleset_library(library_id)

    def create_ruleset_library(self, library: RulesetLibrary) -> RulesetLibrary:
        """
        Creates a new ruleset library on the server.

        Sets the library's server-assigned fields (`id`, `is_valid`, `created`, `modified`) and returns the library.
        """

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/ruleset-libraries/", data=data)
        created_library = RulesetLibrary.model_validate(response.json())
        library.id = created_library.id
        library.is_valid = created_library.is_valid
        library.created = created_library.created
        library.modified = created_library.modified
        logger.info('Creation of ruleset library "%s" successful', library.name)
        return library

    def update_ruleset_library(self, library: RulesetLibrary) -> RulesetLibrary:
        """
        Performs a full update of the ruleset library.

        The library must have its `id` set (i.e., it must have been previously created or retrieved from the server).
        """

        if library.id is None:
            raise ValueError("Cannot update a library that has not been created yet (id is None)")

        data = library.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/ruleset-libraries/{library.id}/", data=data)
        updated_library = RulesetLibrary.model_validate(response.json())
        library.is_valid = updated_library.is_valid
        library.modified = updated_library.modified
        logger.debug('Update of ruleset library "%s" successful', library.name)
        return library

    def create_or_update_ruleset_library(self, library: RulesetLibrary) -> RulesetLibrary:
        """
        Creates the library if it doesn't exist, or updates it if a library with the same name already exists.

        Sets the library's `id` property.
        """

        existing = self.get_ruleset_library_by_name(library.name, library.namespace)
        if existing is not None:
            library.id = existing.id
            return self.update_ruleset_library(library)

        return self.create_ruleset_library(library)

    def delete_ruleset_library_by_id_if_exists(self, library_id: RulesetLibraryId, *, force: bool = False) -> None:
        """
        Deletes (archives) the ruleset library with the given ID.

        No-op if the library does not exist.

        If the library is imported by any rulesets,
        the server will return 409 Conflict unless `force=True` is passed.
        """

        params = {"force": "true"} if force else None
        self._delete_if_exists(f"/api/ruleset-libraries/{library_id}/", params=params)

    def delete_ruleset_library_by_name_if_exists(
        self, library_name: str, namespace: str = "", *, force: bool = False
    ) -> None:
        """
        Deletes the ruleset library with the given name and namespace.

        No-op if the library does not exist.
        """

        all_libraries = self.list_ruleset_libraries()
        matching = [lib for lib in all_libraries if lib.name == library_name and lib.namespace == namespace]
        for lib in matching:
            if lib.id is None:
                raise DataMasqueException(f'Server returned a ruleset library named "{lib.name}" without an `id`.')

            self.delete_ruleset_library_by_id_if_exists(lib.id, force=force)

    def iter_rulesets_using_library(self, library_id: RulesetLibraryId) -> Iterator[Ruleset]:
        """Lazily iterate non-archived rulesets that import the given library."""

        return self._iter_paginated(f"/api/ruleset-libraries/{library_id}/rulesets/", model=Ruleset)

    def list_rulesets_using_library(self, library_id: RulesetLibraryId) -> list[Ruleset]:
        """
        Lists non-archived rulesets that import the given library.

        Note: The YAML content is not included in the response for performance.
        Each returned Ruleset will have an empty string for `yaml`.
        """

        return list(self.iter_rulesets_using_library(library_id))

    def validate_ruleset_library(self, library_id: RulesetLibraryId) -> RulesetLibrary:
        """
        Triggers re-validation of the ruleset library by performing a no-op update.

        Returns the updated library with the new validation status.
        """

        response = self.make_request("PATCH", f"/api/ruleset-libraries/{library_id}/", data={})
        return RulesetLibrary.model_validate(response.json())
