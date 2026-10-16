from collections import defaultdict
from collections.abc import Sequence
from typing import Optional, Union

from datamasque.client.base import BaseClient
from datamasque.client.models.project import Project, ProjectId, unwrap_project_id
from datamasque.client.models.resource import RESOURCE_MODEL_BY_TYPE_NAME, Resource, ResourceTypeName


class ResourceClient(BaseClient):
    """Resource listing API methods. Mixed into `DataMasqueClient`."""

    def list_resources(
        self,
        *,
        projects: Optional[Sequence[Union[ProjectId, Project]]] = None,
        types: Optional[Sequence[ResourceTypeName]] = None,
        search: Optional[str] = None,
        page_size: int = 500,
    ) -> dict[str, list[Resource]]:
        """
        Returns the resources the caller can manage, grouped by type name such as `rulesets`, across every page.

        `projects` limits them to those projects, each given as a `Project` or its `ProjectId`;
        `types` to those types; and `search` to resources whose name contains every word of it, ignoring case.
        The groups are the requested types, or every type the server has when `types` is not given.
        Connections, rulesets and discovery configs are listed as `ConnectionResource`, `RulesetResource`
        and `DiscoveryConfigResource`.

        Raises:
            DataMasqueApiError: a project does not exist, or any other non-2xx response.
        """

        params: dict[str, object] = {}
        if projects is not None:
            params["project"] = [unwrap_project_id(project) for project in projects]
        if types is not None:
            params["type"] = [type_name.value for type_name in types]
        if search is not None:
            params["search"] = search

        resources: defaultdict[str, list[Resource]] = defaultdict(list)
        for results in self._iter_pages("/api/resources/", params=params, page_size=page_size):
            for type_name, rows in results.items():
                model = RESOURCE_MODEL_BY_TYPE_NAME.get(type_name, Resource)
                resources[type_name].extend(model.model_validate(row) for row in rows)

        return dict(resources)
