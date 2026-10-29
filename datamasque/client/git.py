from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal, Optional, Union

from pydantic import BaseModel

from datamasque.client.base import BaseClient
from datamasque.client.models.discovery_config import DiscoveryConfig
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary
from datamasque.client.models.git import GitFile
from datamasque.client.models.ruleset import Ruleset
from datamasque.client.models.ruleset_library import RulesetLibrary

GitExtension = Literal[".yml", ".yaml"]
"""The extensions the server accepts for a pushed file."""

DEFAULT_GIT_EXTENSION: GitExtension = ".yml"


@dataclass(frozen=True)
class _GitResourceType:
    """The endpoint and request field names one resource type's git operations use."""

    endpoint: str
    name_field: str
    content_field: str
    folder_param: str
    """The pull parameter naming the file's folder, as the file list shows it."""


_RULESET = _GitResourceType("/api/ruleset-git/", "ruleset_name", "ruleset_content", "folder")
_RULESET_LIBRARY = _GitResourceType("/api/library-git/", "library_name", "library_content", "namespace")
_DISCOVERY_CONFIG = _GitResourceType("/api/discovery-config-git/", "config_name", "config_content", "folder")
_DISCOVERY_CONFIG_LIBRARY = _GitResourceType(
    "/api/discovery-config-library-git/", "library_name", "library_content", "namespace"
)


class _GitPull(BaseModel):
    """The response to a pull."""

    config_yaml: str


class _GitPushPath(BaseModel):
    """The response to a push-path request."""

    path: str


def _branch_param(branch: Optional[str]) -> dict[str, str]:
    return {} if branch is None else {"branch": branch}


def _require_yaml(yaml: Optional[str], description: str, getter: str) -> str:
    if not yaml:
        raise ValueError(
            f"Cannot push a {description} to git without YAML content (yaml is empty or unset); "
            f"fetch the {description} with its YAML using `{getter}` first"
        )

    return yaml


class GitClient(BaseClient):
    """
    Git push, pull and file listing for rulesets, ruleset libraries, discovery configs and their libraries.

    Mixed into `DataMasqueClient`.
    The server reads and writes the repository in its Git settings with the user's SSH key,
    so both must be set up for the user the client authenticates as.
    Each `branch` parameter defaults to the branch in the Git settings.

    A push writes `<type folder>/<project name>/<name><extension>` under the Git directory path,
    where `<project name>` is the project's `name`, not its `reference`,
    and the type folders are `rulesets/`, `libraries/`, `discovery-configs/` and `discovery-config-libraries/`.
    A resource pushed before the server's upgrade to projects keeps the file it had then,
    until it is renamed or moved to another project:
    a ruleset at the top of the Git directory path,
    a discovery config directly in `discovery-configs/`,
    a library of the `Default` project directly in its type folder,
    and a library of another namespace in the namespace's folder,
    such as `libraries/bank/au/` for the namespace that the upgrade made the project `bank_au`.

    The methods give paths in two forms:

    - `get_*_git_push_path` returns the file's path in the repository without its extension,
      starting with the Git directory path.
    - `list_*_git_files` returns each file's path with its extension,
      relative to the Git directory path for rulesets and to the type folder for the other types.
      The matching `pull_*_from_git` method takes a path in this form.

    For example, to restore a ruleset to the commit before its latest one and push it back:

    .. code-block:: python

        banking = next(project for project in client.iter_projects() if project.name == "Banking")
        file = next(f for f in client.list_ruleset_git_files() if f.path == "rulesets/Banking/customers.yml")
        yaml = client.pull_ruleset_from_git(file.path, file.commits[1].commit_sha)
        ruleset = client.create_or_update_ruleset(Ruleset(name="customers", project=banking.reference, yaml=yaml))
        client.push_ruleset_to_git(ruleset, "Restore the previous customers ruleset")
    """

    def list_ruleset_git_files(self, *, branch: Optional[str] = None) -> list[GitFile]:
        """
        Lists the ruleset files in the git repository.

        Paths are relative to the Git directory path:
        `rulesets/<project name>/customers.yml` for a project's ruleset,
        and `customers.yml` for one pushed before the server's upgrade to projects.
        """

        return self._list_git_files(_RULESET, branch)

    def pull_ruleset_from_git(self, path: str, commit_sha: str, *, branch: Optional[str] = None) -> str:
        """
        Returns the YAML of a ruleset file at `commit_sha`.

        `path` is the file's path as `list_ruleset_git_files` lists it.
        Saves nothing: set it as a ruleset's `yaml` and save that ruleset to keep it.
        """

        return self._pull_from_git(_RULESET, path, commit_sha, branch)

    def push_ruleset_to_git(
        self,
        ruleset: Ruleset,
        commit_message: str,
        *,
        branch: Optional[str] = None,
        extension: GitExtension = DEFAULT_GIT_EXTENSION,
    ) -> None:
        """
        Commits the ruleset's `yaml` to the git repository and pushes it.

        The file is the ruleset's name with `extension` (`.yml` or `.yaml`) in its project's folder,
        unless the saved ruleset kept an older file (see `GitClient`).
        A push with the other extension writes a second file and leaves the first.
        With no `project` set, the project is the saved ruleset's when it has an `id`,
        else the client's `default_project`, else the server's only active project.
        The ruleset's `id`, else its name, type and project, identify the saved ruleset the push is for.
        The push sends the given ruleset's `yaml`, not the saved one, so save the ruleset first.
        The server records the pushed commit as the saved ruleset's git provenance when their YAML matches,
        but the given ruleset is not updated.

        Raises:
            ValueError: The ruleset's `yaml` is empty.
            DataMasqueApiError: With status 409 when another ruleset uses or would write the file,
              and with status 400 when nothing changed, the repository has a file where the path needs a folder,
              or no project is set and the server does not have exactly one active project.
        """

        self._push_to_git(
            _RULESET,
            self._ruleset_git_target(ruleset),
            _require_yaml(ruleset.yaml, "ruleset", "list_rulesets"),
            commit_message,
            branch=branch,
            extension=extension,
        )

    def get_ruleset_git_push_path(self, ruleset: Ruleset) -> str:
        """
        Returns the path `push_ruleset_to_git` would write the ruleset to.

        The path is relative to the repository and has no extension.
        """

        return self._get_git_push_path(_RULESET, self._ruleset_git_target(ruleset))

    def list_ruleset_library_git_files(self, *, branch: Optional[str] = None) -> list[GitFile]:
        """
        Lists the ruleset library files in the git repository.

        Paths are relative to `libraries/` under the Git directory path:
        `<project name>/emails.yml` for a project's library,
        `emails.yml` for a library of the `Default` project pushed before the server's upgrade to projects,
        and `<namespace>/emails.yml` for a library in a namespace whose project the upgrade named differently,
        such as `bank/au/emails.yml` in the project `bank_au`.
        """

        return self._list_git_files(_RULESET_LIBRARY, branch)

    def pull_ruleset_library_from_git(self, path: str, commit_sha: str, *, branch: Optional[str] = None) -> str:
        """
        Returns the YAML of a library file at `commit_sha`.

        `path` is the file's path as `list_ruleset_library_git_files` lists it.
        Saves nothing: set it as a library's `yaml` and save that library to keep it.
        """

        return self._pull_from_git(_RULESET_LIBRARY, path, commit_sha, branch)

    def push_ruleset_library_to_git(
        self,
        library: RulesetLibrary,
        commit_message: str,
        *,
        branch: Optional[str] = None,
        extension: GitExtension = DEFAULT_GIT_EXTENSION,
    ) -> None:
        """
        Commits the library's `yaml` to the git repository and pushes it.

        The file is the library's name with `extension` (`.yml` or `.yaml`) in its project's folder,
        unless the saved library kept an older file (see `GitClient`).
        A push with the other extension writes a second file and leaves the first.
        With no `project` set, the project is the saved library's when it has an `id`,
        else the client's `default_project`, else the server's only active project.
        The library's `id`, else its name and project, identify the saved library the push is for.
        The push sends the given library's `yaml`, not the saved one, so save the library first.
        The server records the pushed commit as the saved library's git provenance when their YAML matches,
        but the given library is not updated.

        Raises:
            ValueError: The library's `yaml` is empty or unset.
            DataMasqueApiError: With status 409 when another library uses or would write the file,
              and with status 400 when nothing changed, the repository has a file where the path needs a folder,
              or no project is set and the server does not have exactly one active project.
        """

        self._push_to_git(
            _RULESET_LIBRARY,
            self._library_git_target(_RULESET_LIBRARY, library),
            _require_yaml(library.yaml, "ruleset library", "get_ruleset_library"),
            commit_message,
            branch=branch,
            extension=extension,
        )

    def get_ruleset_library_git_push_path(self, library: RulesetLibrary) -> str:
        """
        Returns the path `push_ruleset_library_to_git` would write the library to.

        The path is relative to the repository and has no extension.
        """

        return self._get_git_push_path(_RULESET_LIBRARY, self._library_git_target(_RULESET_LIBRARY, library))

    def list_discovery_config_git_files(self, *, branch: Optional[str] = None) -> list[GitFile]:
        """
        Lists the discovery config files in the git repository.

        Paths are relative to `discovery-configs/` under the Git directory path:
        `<project name>/customers.yml` for a project's config,
        and `customers.yml` for one pushed before the server's upgrade to projects.
        """

        return self._list_git_files(_DISCOVERY_CONFIG, branch)

    def pull_discovery_config_from_git(self, path: str, commit_sha: str, *, branch: Optional[str] = None) -> str:
        """
        Returns the YAML of a config file at `commit_sha`.

        `path` is the file's path as `list_discovery_config_git_files` lists it.
        Saves nothing: set it as a config's `yaml` and save that config to keep it.
        """

        return self._pull_from_git(_DISCOVERY_CONFIG, path, commit_sha, branch)

    def push_discovery_config_to_git(
        self,
        config: DiscoveryConfig,
        commit_message: str,
        *,
        branch: Optional[str] = None,
        extension: GitExtension = DEFAULT_GIT_EXTENSION,
    ) -> None:
        """
        Commits the discovery config's `yaml` to the git repository and pushes it.

        The file is the config's name with `extension` (`.yml` or `.yaml`) in its project's folder,
        unless the saved config kept an older file (see `GitClient`).
        A push with the other extension writes a second file and leaves the first.
        With no `project` set, the project is the saved config's when it has an `id`,
        else the client's `default_project`, else the server's only active project.
        The config's `id`, else its name, type and project, identify the saved config the push is for.
        The push sends the given config's `yaml`, not the saved one, so save the config first.
        The server records the pushed commit on the saved config when their YAML matches,
        but `DiscoveryConfig` does not expose it.

        Raises:
            ValueError: The config's `yaml` is empty or unset.
            DataMasqueApiError: With status 409 when another config uses or would write the file,
              and with status 400 when nothing changed, the repository has a file where the path needs a folder,
              or no project is set and the server does not have exactly one active project.
        """

        self._push_to_git(
            _DISCOVERY_CONFIG,
            self._discovery_config_git_target(config),
            _require_yaml(config.yaml, "discovery config", "get_discovery_config"),
            commit_message,
            branch=branch,
            extension=extension,
        )

    def get_discovery_config_git_push_path(self, config: DiscoveryConfig) -> str:
        """
        Returns the path `push_discovery_config_to_git` would write the config to.

        The path is relative to the repository and has no extension.
        """

        return self._get_git_push_path(_DISCOVERY_CONFIG, self._discovery_config_git_target(config))

    def list_discovery_config_library_git_files(self, *, branch: Optional[str] = None) -> list[GitFile]:
        """
        Lists the discovery config library files in the git repository.

        Paths are relative to `discovery-config-libraries/` under the Git directory path:
        `<project name>/emails.yml` for a project's library,
        `emails.yml` for a library of the `Default` project pushed before the server's upgrade to projects,
        and `<namespace>/emails.yml` for a library in a namespace whose project the upgrade named differently,
        such as `bank/au/emails.yml` in the project `bank_au`.
        """

        return self._list_git_files(_DISCOVERY_CONFIG_LIBRARY, branch)

    def pull_discovery_config_library_from_git(
        self, path: str, commit_sha: str, *, branch: Optional[str] = None
    ) -> str:
        """
        Returns the YAML of a library file at `commit_sha`.

        `path` is the file's path as `list_discovery_config_library_git_files` lists it.
        Saves nothing: set it as a library's `yaml` and save that library to keep it.
        """

        return self._pull_from_git(_DISCOVERY_CONFIG_LIBRARY, path, commit_sha, branch)

    def push_discovery_config_library_to_git(
        self,
        library: DiscoveryConfigLibrary,
        commit_message: str,
        *,
        branch: Optional[str] = None,
        extension: GitExtension = DEFAULT_GIT_EXTENSION,
    ) -> None:
        """
        Commits the discovery config library's `yaml` to the git repository and pushes it.

        The file is the library's name with `extension` (`.yml` or `.yaml`) in its project's folder,
        unless the saved library kept an older file (see `GitClient`).
        A push with the other extension writes a second file and leaves the first.
        With no `project` set, the project is the saved library's when it has an `id`,
        else the client's `default_project`, else the server's only active project.
        The library's `id`, else its name and project, identify the saved library the push is for.
        The push sends the given library's `yaml`, not the saved one, so save the library first.
        The server records the pushed commit on the saved library when their YAML matches,
        but `DiscoveryConfigLibrary` does not expose it.

        Raises:
            ValueError: The library's `yaml` is empty or unset.
            DataMasqueApiError: With status 409 when another library uses or would write the file,
              and with status 400 when nothing changed, the repository has a file where the path needs a folder,
              or no project is set and the server does not have exactly one active project.
        """

        self._push_to_git(
            _DISCOVERY_CONFIG_LIBRARY,
            self._library_git_target(_DISCOVERY_CONFIG_LIBRARY, library),
            _require_yaml(library.yaml, "discovery config library", "get_discovery_config_library"),
            commit_message,
            branch=branch,
            extension=extension,
        )

    def get_discovery_config_library_git_push_path(self, library: DiscoveryConfigLibrary) -> str:
        """
        Returns the path `push_discovery_config_library_to_git` would write the library to.

        The path is relative to the repository and has no extension.
        """

        return self._get_git_push_path(
            _DISCOVERY_CONFIG_LIBRARY,
            self._library_git_target(_DISCOVERY_CONFIG_LIBRARY, library),
        )

    def _ruleset_git_target(self, ruleset: Ruleset) -> dict[str, str]:
        return self._git_target(
            _RULESET, ruleset.name, ruleset.project, ruleset.id, mask_type=ruleset.ruleset_type.value
        )

    def _discovery_config_git_target(self, config: DiscoveryConfig) -> dict[str, str]:
        return self._git_target(
            _DISCOVERY_CONFIG, config.name, config.project, config.id, config_type=config.config_type.value
        )

    def _library_git_target(
        self, resource_type: _GitResourceType, library: Union[RulesetLibrary, DiscoveryConfigLibrary]
    ) -> dict[str, str]:
        return self._git_target(resource_type, library.name, library.project, library.id)

    def _git_target(
        self,
        resource_type: _GitResourceType,
        name: str,
        project: Optional[str],
        resource_id: Optional[str],
        **type_fields: str,
    ) -> dict[str, str]:
        """
        Returns the fields that pick the file a push writes, as the push and push-path requests take them.

        An unset `project` is left for the server to take from the saved resource when there is an `id`,
        and is otherwise the client's `default_project` when there is one.
        """

        fields: dict[str, str] = {resource_type.name_field: name, **type_fields}
        if resource_id is not None:
            fields["id"] = resource_id
        if project is not None:
            fields["project"] = project
        elif resource_id is None:
            fields |= self._default_project_field(None)

        return fields

    def _list_git_files(self, resource_type: _GitResourceType, branch: Optional[str]) -> list[GitFile]:
        response = self.make_request("GET", f"{resource_type.endpoint}files/", params=_branch_param(branch))
        return [GitFile(path=path, commits=commits) for path, commits in response.json().items()]

    def _pull_from_git(self, resource_type: _GitResourceType, path: str, commit_sha: str, branch: Optional[str]) -> str:
        file_path = PurePosixPath(path)
        folder = file_path.parent
        params = {
            resource_type.name_field: file_path.stem,
            "extension": file_path.suffix,
            resource_type.folder_param: "" if folder == PurePosixPath(".") else folder.as_posix(),
            "commit_id": commit_sha,
            **_branch_param(branch),
        }
        response = self.make_request("GET", resource_type.endpoint, params=params)
        return _GitPull.model_validate(response.json()).config_yaml

    def _push_to_git(
        self,
        resource_type: _GitResourceType,
        target: dict[str, str],
        content: str,
        commit_message: str,
        *,
        branch: Optional[str],
        extension: GitExtension,
    ) -> None:
        data = {
            **target,
            resource_type.content_field: content,
            "commit_message": commit_message,
            "extension": extension,
            **_branch_param(branch),
        }
        self.make_request("POST", resource_type.endpoint, data=data)

    def _get_git_push_path(self, resource_type: _GitResourceType, target: dict[str, str]) -> str:
        response = self.make_request("GET", f"{resource_type.endpoint}push-path/", params=target)
        return _GitPushPath.model_validate(response.json()).path
