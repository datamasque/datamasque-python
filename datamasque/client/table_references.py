import logging
from typing import Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.project import Project, ProjectId, unwrap_project_id
from datamasque.client.models.table_reference import TableReference, TableReferenceId

logger = logging.getLogger(__name__)


class TableReferenceClient(BaseClient):
    """Table reference CRUD API methods. Mixed into `DataMasqueClient`."""

    def list_table_references(self, project: Optional[Union[ProjectId, Project]] = None) -> list[TableReference]:
        """
        Lists all table references, or only those in `project` when it is given.

        `project` is a `Project` or its `ProjectId`.

        Unlike most list endpoints, this one is not paginated;
        the server returns every table reference in a single response.
        Deleted table references are archived rather than removed, and are not listed.
        """

        params = {"project": unwrap_project_id(project)} if project is not None else None
        response = self.make_request("GET", "/api/table-references/", params=params)
        return [TableReference.model_validate(item) for item in response.json()]

    def get_table_reference(self, table_reference_id: TableReferenceId) -> TableReference:
        """Retrieves a single table reference by ID."""

        response = self.make_request("GET", f"/api/table-references/{table_reference_id}/")
        return TableReference.model_validate(response.json())

    def get_table_reference_by_name(self, name: str, project: Union[ProjectId, Project]) -> Optional[TableReference]:
        """
        Looks for a table reference with the given name (case-sensitive, exact match) in the given project.

        Names are unique across a project's live table references, so at most one can match.
        `project` is a `Project` or its `ProjectId`.
        Returns it if found, otherwise `None`.

        The endpoint takes no name filter, so the match is made client-side over the project's listing.
        """

        matches = [reference for reference in self.list_table_references(project) if reference.name == name]
        if not matches:
            return None

        return matches[0]

    def _get_table_reference_id_by_name(self, name: str, project: Union[ProjectId, str]) -> Optional[TableReferenceId]:
        """Return the ID of the table reference with the given name in the given project, or `None` if there is none."""

        response = self.make_request("GET", "/api/table-references/", params={"project": project})
        matches = [item for item in response.json() if item.get("name") == name]
        if not matches:
            return None

        existing_id = matches[0].get("id")
        if existing_id is None:
            raise DataMasqueApiError(
                f'Server returned a table reference named "{name}" without an `id`.',
                response=response,
            )

        return TableReferenceId(existing_id)

    def create_table_reference(self, table_reference: TableReference) -> TableReference:
        """
        Creates a new table reference on the server.

        Sets the table reference's server-assigned fields (`id`, `created`, `modified`)
        and its `options` as stored by the server (with any omitted option filled in with its default),
        then returns the table reference.
        """

        data = table_reference.model_dump(by_alias=True, mode="json")
        if data.get("options") is None:
            data.pop("options", None)
        if data.get("project") is None:
            data.pop("project", None)
        response = self.make_request("POST", "/api/table-references/", data=data)
        created = TableReference.model_validate(response.json())
        table_reference.id = created.id
        table_reference.project = created.project
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

        data = table_reference.model_dump(by_alias=True, mode="json")
        if data.get("options") is None:
            data.pop("options", None)
        if data.get("project") is None:
            data.pop("project", None)
        response = self.make_request("PUT", f"/api/table-references/{table_reference.id}/", data=data)
        updated = TableReference.model_validate(response.json())
        table_reference.options = updated.options
        table_reference.modified = updated.modified
        logger.debug('Update of table reference "%s" successful', table_reference.name)
        return table_reference

    def create_or_update_table_reference(self, table_reference: TableReference) -> TableReference:
        """
        Creates the table reference, or updates the existing one with the same name in the same project.

        With no `project` set, the server's only active project is used,
        and `DataMasqueException` is raised when it has no active project or more than one.
        Sets the table reference's `id` property.
        """

        existing_id = self._get_table_reference_id_by_name(
            table_reference.name, self._resolve_project(table_reference.project)
        )
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

    def delete_table_reference_by_name_if_exists(self, name: str, project: Union[ProjectId, Project]) -> None:
        """
        Deletes the table reference with the given name in the given project.

        `project` is a `Project` or its `ProjectId`.
        No-op if no such table reference exists.
        """

        table_reference_id = self._get_table_reference_id_by_name(name, unwrap_project_id(project))
        if table_reference_id is not None:
            self.delete_table_reference_by_id_if_exists(table_reference_id)
