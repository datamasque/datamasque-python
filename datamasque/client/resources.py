import logging
from collections import defaultdict
from collections.abc import Sequence
from typing import Optional

from datamasque.client.base import BaseClient
from datamasque.client.models.project import ProjectIdentifier, unwrap_project_id
from datamasque.client.models.resource import (
    RESOURCE_MODEL_BY_TYPE_NAME,
    Resource,
    ResourceMoveConflict,
    ResourceMoveResult,
    ResourcesByType,
)
from datamasque.client.models.resource_type import ResourceTypeName

logger = logging.getLogger(__name__)


def _move_request(resources: ResourcesByType, destination: ProjectIdentifier) -> dict[str, object]:
    keys = [
        {"type": type_name.value, "id": resource.id if isinstance(resource, Resource) else resource}
        for type_name, group in resources.items()
        for resource in group
    ]
    if not keys:
        raise ValueError("Specify at least one resource to move")

    # The API takes the destination as a string: an id or `<id>-<slug>`.
    return {"destination": str(unwrap_project_id(destination)), "resources": keys}


class ResourceClient(BaseClient):
    """Resource listing and move API methods. Mixed into `DataMasqueClient`."""

    def list_resources(
        self,
        *,
        projects: Optional[Sequence[ProjectIdentifier]] = None,
        types: Optional[Sequence[ResourceTypeName]] = None,
        search: Optional[str] = None,
        page_size: int = 500,
    ) -> dict[ResourceTypeName, list[Resource]]:
        """
        Returns the resources the caller can manage, grouped by type, fetched in pages of `page_size`.

        `projects` limits them to those projects, each given as a `Project`, its `ProjectId` or its `<id>-<slug>` form;
        `types` to those types; and `search` to resources whose name contains every word of it, ignoring case.
        The groups are the requested types, or every type the server has when `types` is not given.
        An empty `projects` or `types` matches no resources, without a request to the server.
        Connections, rulesets and discovery configs are listed as `ConnectionResource`, `RulesetResource`
        and `DiscoveryConfigResource`.

        Raises:
            DataMasqueApiError: the server rejects the request, for example because a project does not exist.
        """

        # `requests` leaves out a parameter whose value is an empty list, which would remove the filter.
        if projects is not None and not projects:
            return {type_name: [] for type_name in types or []}

        if types is not None and not types:
            return {}

        params: dict[str, object] = {}
        if projects is not None:
            params["project"] = [unwrap_project_id(project) for project in projects]
        if types is not None:
            params["type"] = [type_name.value for type_name in types]
        if search is not None:
            params["search"] = search

        resources: defaultdict[ResourceTypeName, list[Resource]] = defaultdict(list)
        for results in self._iter_pages("/api/resources/", params=params, page_size=page_size):
            for type_name, rows in results.items():
                model = RESOURCE_MODEL_BY_TYPE_NAME.get(type_name, Resource)
                resources[ResourceTypeName(type_name)].extend(model.model_validate(row) for row in rows)

        return dict(resources)

    def get_resource_move_conflicts(
        self,
        resources: ResourcesByType,
        destination: ProjectIdentifier,
    ) -> list[ResourceMoveConflict]:
        """
        Checks whether `resources` can move to the destination project, without moving them.

        `resources` can contain up to 2,000 resources.

        Returns:
            The conflicts that would keep any of `resources` from moving (if any).

        Raises:
            ValueError: `resources` is empty.
            DataMasqueApiError: the server rejects the request, for example because the destination does not exist
                or `resources` contains more than 2,000 resources.
        """

        response = self.make_request("POST", "/api/resources/move-check/", data=_move_request(resources, destination))
        return [ResourceMoveConflict.model_validate(conflict) for conflict in response.json()["conflicts"]]

    def move_resources(
        self,
        resources: ResourcesByType,
        destination: ProjectIdentifier,
    ) -> ResourceMoveResult:
        """
        Moves `resources` to the destination project, with their files and runs.

        `resources` can be the result of `list_resources`, or any part of it, and can contain up to 2,000 resources.
        A resource that conflicts with the destination, or with another resource in `resources`,
        stays where it is and is listed in the result's `conflicts`; every other resource moves.
        Rulesets, discovery configs and RG configs whose library imports resolve differently after the move
        are marked invalid.

        Raises:
            ValueError: `resources` is empty.
            DataMasqueApiError: the server rejects the move, for example because the destination does not exist,
                `resources` contains more than 2,000 resources,
                or the caller does not have permission to move one of the resource types.
        """

        response = self.make_request("POST", "/api/resources/move/", data=_move_request(resources, destination))
        result = ResourceMoveResult.model_validate(response.json())
        logger.info(
            "Moved %d resources to project %s; %d could not move",
            result.moved,
            unwrap_project_id(destination),
            len(result.conflicts),
        )
        return result
