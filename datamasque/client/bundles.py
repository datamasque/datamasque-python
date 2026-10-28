import logging
from collections.abc import Sequence
from typing import Optional, Union

from datamasque.client.base import BaseClient, FileOrContent, UploadFile, read_file_or_content
from datamasque.client.models.bundle import BundleImportResult
from datamasque.client.models.project import ProjectIdentifier, unwrap_project_id
from datamasque.client.models.ruleset import Ruleset, RulesetId, unwrap_ruleset_id

logger = logging.getLogger(__name__)


def _form_bool(value: bool) -> str:
    return "true" if value else "false"


class BundleClient(BaseClient):
    """
    Export and import bundle API methods. Mixed into `DataMasqueClient`.

    The server accepts these only with the login token that `POST /api/auth/token/login/` returns.
    They fail with a 403 for a client whose `token_source` returns a user's long-lived API token,
    the `api_token` that `GET /api/users/me/` gives.
    """

    def export_bundle(
        self,
        rulesets: Optional[Sequence[Union[Ruleset, RulesetId]]] = None,
        *,
        include_seed_files: bool = False,
    ) -> bytes:
        """
        Returns a Version 2 export bundle, a Zip archive that files every item under its project's name.

        The bundle holds `rulesets`, given as `Ruleset`s or their IDs, and the libraries they use;
        or, when `rulesets` is not given, every ruleset and library.
        With `include_seed_files`, it also holds the custom seed files those rulesets and libraries refer to.
        Pass the bundle to `import_bundle`, on this server or another,
        to put each item in the project of the same name.
        For example, `Path("export.zip").write_bytes(client.export_bundle())` saves it to a file.

        Raises:
            TypeError: `rulesets` is a single `Ruleset` or ID, not a sequence of them.
            ValueError: a `Ruleset` in `rulesets` has not been created on the server.
            DataMasqueApiError: the server rejects the request,
                for example with a 403 when the client authenticates with a long-lived API token.
        """

        if isinstance(rulesets, (str, Ruleset)):
            raise TypeError("Pass `rulesets` as a sequence, such as `[ruleset]`")

        data: dict[str, object] = {"include_seed_files": include_seed_files}
        if rulesets is not None:
            data["ruleset_ids"] = [unwrap_ruleset_id(ruleset) for ruleset in rulesets]

        response = self.make_request("POST", "/api/export/v2/", data=data)
        return response.content

    def import_bundle(
        self,
        bundle: FileOrContent,
        *,
        overwrite_rulesets: Optional[bool] = None,
        overwrite_libraries: bool = False,
        overwrite_seed_files: bool = False,
        fallback_project: Optional[ProjectIdentifier] = None,
    ) -> BundleImportResult:
        """
        Imports an export bundle, in the Version 2 or deprecated Version 1 format.

        Returns what the import did with each item.

        `bundle` is a path to the Zip archive (as `str` or `Path`), its `bytes`,
        or a binary file-like object, such as a file opened with `"rb"`.
        Each item goes into the project the bundle files it under,
        which is created when it does not exist, if the caller is an administrator.
        Items that name no project, such as a Version 1 bundle's rulesets and seed files,
        go into `fallback_project`, given as a `Project`, its `ProjectId` or its `<id>-<slug>` form,
        or else the server's earliest-created active project;
        the result's `fallback_project` names the project they went into.
        The client's `default_project` does not apply.

        With `overwrite_rulesets` unset, a ruleset whose ID exists is left unchanged,
        and a ruleset whose name is in use under another ID is imported under a new name;
        `True` overwrites the existing ruleset and `False` leaves it unchanged.
        Whatever the options, a ruleset or library whose name is in use by one with identical content is left unchanged.
        `BundleImportStatus` lists every outcome.
        `overwrite_libraries` and `overwrite_seed_files` overwrite existing libraries and seed files when `True`,
        and leave them unchanged when `False`.

        The import can succeed for some items and fail for others:
        check each type's `metadata.error`, or each item's `status`.

        Raises:
            ValueError: `fallback_project` is a string that is not a project reference in the form `<id>-<slug>`.
            DataMasqueApiError: the server rejects the import, and imports nothing.
                For example, a 400 when the bundle is not a valid export, `fallback_project` is not an active project,
                or the bundle names a project that cannot be created with that name;
                or a 403 when the bundle names a project that does not exist and the caller is not an administrator,
                or when the client authenticates with a long-lived API token.
        """

        file_name, content = read_file_or_content(bundle, "bundle.zip")
        content.seek(0)

        data = {
            "overwrite_libraries": _form_bool(overwrite_libraries),
            "overwrite_seed_files": _form_bool(overwrite_seed_files),
        }
        if overwrite_rulesets is not None:
            data["overwrite_rulesets"] = _form_bool(overwrite_rulesets)
        if fallback_project is not None:
            data["fallback_project"] = str(unwrap_project_id(fallback_project))

        response = self.make_request(
            "POST",
            "/api/import/v2/",
            data=data,
            files=[
                UploadFile(
                    field_name="zip_archive",
                    filename=file_name,
                    content=content,
                    content_type="application/zip",
                ),
            ],
        )
        result = BundleImportResult.model_validate(response.json())
        result.is_deprecated_bundle = response.headers.get("Deprecation", "").lower() == "true"
        if result.is_deprecated_bundle:
            logger.warning("Imported a deprecated Version 1 bundle; export a Version 2 bundle to keep each project")
        if result.fallback_project is not None:
            logger.info('Items that named no project went into project "%s"', result.fallback_project)

        return result
