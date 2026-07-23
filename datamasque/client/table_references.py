import logging
from typing import Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.table_reference import TableReference, TableReferenceId

logger = logging.getLogger(__name__)


class TableReferenceClient(BaseClient):
    """Table reference CRUD API methods. Mixed into `DataMasqueClient`."""

    def list_table_references(self) -> list[TableReference]:
        """
        Lists all table references.

        Unlike most list endpoints, this one is not paginated;
        the server returns every table reference in a single response.
        Deleted table references are archived rather than removed, and are not listed.
        """

        response = self.make_request("GET", "/api/table-references/")
        return [TableReference.model_validate(item) for item in response.json()]

    def get_table_reference(self, table_reference_id: TableReferenceId) -> TableReference:
        """Retrieves a single table reference by ID."""

        response = self.make_request("GET", f"/api/table-references/{table_reference_id}/")
        return TableReference.model_validate(response.json())

    def get_table_reference_by_name(self, name: str) -> Optional[TableReference]:
        """
        Looks for a table reference with the given name (case-sensitive, exact match).

        Names are unique across live table references, so at most one can match.
        Returns it if found, otherwise `None`.

        The endpoint takes no name filter, so the match is made client-side over the full listing.
        """

        matches = [reference for reference in self.list_table_references() if reference.name == name]
        if not matches:
            return None

        return matches[0]

    def _get_table_reference_id_by_name(self, name: str) -> Optional[TableReferenceId]:
        """Return the ID of the table reference with the given name, or `None` if there is none."""

        existing = self.get_table_reference_by_name(name)
        if existing is None:
            return None

        if existing.id is None:
            raise DataMasqueException(f'Server returned a table reference named "{name}" without an `id`.')

        return existing.id

    def create_table_reference(self, table_reference: TableReference) -> TableReference:
        """
        Creates a new table reference on the server.

        Sets the table reference's server-assigned fields (`id`, `created`, `modified`)
        and its `options` as stored by the server (with any omitted option filled in with its default),
        then returns the table reference.
        """

        data = table_reference.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("POST", "/api/table-references/", data=data)
        created = TableReference.model_validate(response.json())
        table_reference.id = created.id
        table_reference.options = created.options
        table_reference.created = created.created
        table_reference.modified = created.modified
        logger.info('Creation of table reference "%s" successful', table_reference.name)
        return table_reference

    def update_table_reference(self, table_reference: TableReference) -> TableReference:
        """
        Performs a full update of the table reference.

        The table reference must have its `id` set
        (i.e., it must have been previously created or retrieved from the server).

        `options` are replaced wholesale when set, so an option left at its model default
        is sent as that default rather than keeping whatever the server had stored.
        Leaving `options` as `None` sends none at all, which leaves the stored options untouched.
        """

        if table_reference.id is None:
            raise ValueError("Cannot update a table reference that has not been created yet (id is None)")

        data = table_reference.model_dump(exclude_none=True, by_alias=True, mode="json")
        response = self.make_request("PUT", f"/api/table-references/{table_reference.id}/", data=data)
        updated = TableReference.model_validate(response.json())
        table_reference.options = updated.options
        table_reference.modified = updated.modified
        logger.debug('Update of table reference "%s" successful', table_reference.name)
        return table_reference

    def create_or_update_table_reference(self, table_reference: TableReference) -> TableReference:
        """
        Creates the table reference, or updates the existing one with the same name.

        Sets the table reference's `id` property.
        """

        existing_id = self._get_table_reference_id_by_name(table_reference.name)
        if existing_id is not None:
            table_reference.id = existing_id
            return self.update_table_reference(table_reference)

        return self.create_table_reference(table_reference)

    def delete_table_reference_by_id_if_exists(self, table_reference_id: TableReferenceId) -> None:
        """
        Deletes the table reference with the given ID.

        The server archives rather than removes it, which frees its name for reuse.
        No-op if the table reference does not exist.
        """

        self._delete_if_exists(f"/api/table-references/{table_reference_id}/")

    def delete_table_reference_by_name_if_exists(self, name: str) -> None:
        """
        Deletes the table reference with the given name.

        No-op if no such table reference exists.
        """

        table_reference_id = self._get_table_reference_id_by_name(name)
        if table_reference_id is not None:
            self.delete_table_reference_by_id_if_exists(table_reference_id)
