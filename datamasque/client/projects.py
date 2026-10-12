import logging

from datamasque.client.base import BaseClient
from datamasque.client.models.project import Project, ProjectCreateRequest

logger = logging.getLogger(__name__)


class ProjectClient(BaseClient):
    """Project API methods. Mixed into `DataMasqueClient`."""

    def create_project(self, request: ProjectCreateRequest) -> Project:
        """
        Creates a new project on the server, and returns it as the server stored it.

        Only admin users can create projects, and API tokens cannot.
        A name the server rejects (see `ProjectCreateRequest`) raises `DataMasqueApiError` with status 400,
        and a caller who is not an admin user raises it with status 403.
        """

        data = request.model_dump(mode="json")
        response = self.make_request("POST", "/api/projects/", data=data)
        project = Project.model_validate(response.json())
        logger.info('Creation of project "%s" successful', project.name)
        return project
