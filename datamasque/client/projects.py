import logging
from typing import Iterator, Union

from requests import Response

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import ProjectDeleteConflictError
from datamasque.client.models.project import (
    Project,
    ProjectCreateRequest,
    ProjectDeleteConflict,
    ProjectId,
    unwrap_project_id,
)

logger = logging.getLogger(__name__)


def _get_conflicts(response: Response) -> list[ProjectDeleteConflict]:
    return [ProjectDeleteConflict.model_validate(conflict) for conflict in response.json()["conflicts"]]


class ProjectClient(BaseClient):
    """Project API methods. Mixed into `DataMasqueClient`."""

    def iter_projects(self, *, include_archived: bool = False) -> Iterator[Project]:
        """
        Lazily iterate projects via the paginated endpoint.

        Archived projects are skipped unless `include_archived` is set.
        """

        params = {"include_archived": "true"} if include_archived else None
        return self._iter_paginated("/api/projects/", model=Project, params=params)

    def list_projects(self, *, include_archived: bool = False) -> list[Project]:
        """
        Lists projects.

        Archived projects are left out unless `include_archived` is set.
        """

        return list(self.iter_projects(include_archived=include_archived))

    def get_project(self, project: Union[ProjectId, Project]) -> Project:
        """
        Retrieves a single project, given as a `Project` or its `ProjectId`.

        Archived projects can be retrieved too.
        """

        response = self.make_request("GET", f"/api/projects/{unwrap_project_id(project)}/")
        return Project.model_validate(response.json())

    def create_project(self, request: ProjectCreateRequest) -> Project:
        """
        Creates a new project on the server, and returns it as the server stored it.

        Raises:
            DataMasqueApiError: the server rejects the name (see `ProjectCreateRequest`), or any other non-2xx response.
        """

        data = request.model_dump(mode="json")
        response = self.make_request("POST", "/api/projects/", data=data)
        project = Project.model_validate(response.json())
        logger.info('Creation of project "%s" successful', project.name)
        return project

    def get_project_delete_conflicts(
        self,
        project: Union[ProjectId, Project],
        destination: Union[ProjectId, Project],
    ) -> list[ProjectDeleteConflict]:
        """
        Returns the conflicts that would prevent `delete_project` moving the project's resources to the destination.

        An empty list means `delete_project` can delete the project with this destination,
        unless the resources in either project change first.

        Raises:
            DataMasqueApiError: the destination does not exist, is deleted, or is the project;
                the project does not exist or is already deleted; or any other non-2xx response.
        """

        response = self.make_request(
            "GET",
            f"/api/projects/{unwrap_project_id(project)}/delete-check/",
            params={"destination": unwrap_project_id(destination)},
        )
        return _get_conflicts(response)

    def delete_project(self, project: Union[ProjectId, Project], destination: Union[ProjectId, Project]) -> None:
        """
        Deletes a project, after moving every resource in it to the destination project.

        When the resources cannot move, the server changes nothing.
        `get_project_delete_conflicts` lists the same conflicts without trying to delete the project.

        Raises:
            ProjectDeleteConflictError: the resources cannot move to the destination; its `conflicts` says why.
            DataMasqueApiError: the destination does not exist, is deleted, or is the project;
                the project does not exist or is already deleted; or any other non-2xx response.
        """

        project_id = unwrap_project_id(project)
        response = self.make_request(
            "DELETE",
            f"/api/projects/{project_id}/",
            params={"destination": unwrap_project_id(destination)},
            require_status_check=False,
        )
        if response.status_code == 409:
            raise ProjectDeleteConflictError(
                str(response.json()["detail"]),
                response=response,
                conflicts=_get_conflicts(response),
            )

        self._raise_for_status(response)
        logger.info("Deletion of project %s successful", project_id)
