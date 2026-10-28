"""Export and import bundle models for the DataMasque API."""

from enum import Enum
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict

from datamasque.client.models.ruleset import RulesetId, RulesetType
from datamasque.client.models.ruleset_library import RulesetLibraryId


class BundleImportStatus(str, Enum):
    """What an import did with one item of a bundle."""

    created = "CREATED"
    """No item with that ID or name existed, so it was created. Counted in `created`."""
    created_duplicate = "CREATED_DUPLICATE"
    """
    A ruleset with that name existed and `overwrite_rulesets` was not set,
    so it was created under a new name, given in `imported_name`.
    Counted in `created`.
    """
    not_created = "NOT_CREATED"
    """
    The item was left unchanged, because an item with that name already has identical content,
    or because a ruleset with that ID exists and `overwrite_rulesets` was not set.
    Not counted in any count but `processed`.
    """
    restored = "RESTORED"
    """
    An item with that ID had been deleted, so it was brought back with the bundle's name and content.
    Counted in `restored`.
    """
    updated = "UPDATED"
    """
    An existing item was overwritten, because the import's `overwrite_*` option for its type was `True`.
    Counted in `updated`.
    """
    skipped = "SKIPPED"
    """
    An existing item was left unchanged, because the import's `overwrite_*` option for its type was `False`.
    Counted in `skipped`.
    """
    error = "ERROR"
    """The item could not be imported; `message` says why. Counted in `error`."""


class BundleImportCounts(BaseModel):
    """How many items of one type an import processed, and what it did with them."""

    model_config = ConfigDict(extra="allow")

    processed: int
    """The number of items of the type in the bundle."""
    created: int
    """The number of `CREATED` and `CREATED_DUPLICATE` items."""
    restored: int
    """The number of `RESTORED` items."""
    error: int
    """The number of `ERROR` items."""
    updated: Optional[int] = None
    """The number of `UPDATED` items, or `None` for rulesets imported without `overwrite_rulesets`."""
    skipped: Optional[int] = None
    """The number of `SKIPPED` items, or `None` for rulesets imported without `overwrite_rulesets`."""


class RulesetImportResult(BaseModel):
    """What an import did with one ruleset of a bundle."""

    model_config = ConfigDict(extra="allow")

    exported_name: str
    exported_id: Optional[RulesetId] = None
    """`None` for a hand-built Zip archive without the `manifest.json` that an export writes."""
    exported_project: Optional[str] = None
    """The name of the project the bundle files the ruleset under, or `None` for a Version 1 bundle."""
    imported_name: Optional[str] = None
    """`None` on error."""
    imported_id: Optional[RulesetId] = None
    """`None` on error."""
    imported_project: Optional[str] = None
    """The name of the project the ruleset is in, or `None` on error."""
    ruleset_type: RulesetType
    status: BundleImportStatus
    message: str
    """A description of the outcome, which can change between DataMasque versions; check `status` instead."""


class LibraryImportResult(BaseModel):
    """What an import did with one library of a bundle."""

    model_config = ConfigDict(extra="allow")

    exported_name: str
    exported_namespace: str
    """
    The folder a Version 1 bundle files the library under, which names its project;
    `""` for a library in no folder, and for every library in a Version 2 bundle.
    """
    exported_project: Optional[str] = None
    """The name of the project the bundle files the library under, or `None` for a Version 1 bundle."""
    exported_id: Optional[RulesetLibraryId] = None
    """`None` for a hand-built Zip archive without the `manifest.json` that an export writes."""
    imported_name: Optional[str] = None
    """`None` on error."""
    imported_id: Optional[RulesetLibraryId] = None
    """`None` on error."""
    imported_project: Optional[str] = None
    """The name of the project the library is in, or `None` on error."""
    status: BundleImportStatus
    message: str
    """A description of the outcome, which can change between DataMasque versions; check `status` instead."""


class SeedFileImportResult(BaseModel):
    """What an import did with one seed file of a bundle."""

    model_config = ConfigDict(extra="allow")

    exported_name: str
    exported_project: Optional[str] = None
    """The name of the project the bundle files the seed file under, or `None` for a Version 1 bundle."""
    imported_name: Optional[str] = None
    """`None` on error."""
    imported_project: Optional[str] = None
    """The name of the project the seed file is in, or `None` on error."""
    status: BundleImportStatus
    message: str
    """A description of the outcome, which can change between DataMasque versions; check `status` instead."""


ItemResultT = TypeVar("ItemResultT", RulesetImportResult, LibraryImportResult, SeedFileImportResult)


class BundleImportItemResults(BaseModel, Generic[ItemResultT]):
    """The counts and per-item results of one item type in an import."""

    model_config = ConfigDict(extra="allow")

    metadata: BundleImportCounts
    data: list[ItemResultT]


class BundleImportResult(BaseModel):
    """The outcome of `import_bundle`, grouped by item type."""

    model_config = ConfigDict(extra="allow")

    rulesets: BundleImportItemResults[RulesetImportResult]
    libraries: Optional[BundleImportItemResults[LibraryImportResult]] = None
    """`None` when the bundle has no libraries."""
    seed_files: Optional[BundleImportItemResults[SeedFileImportResult]] = None
    """`None` when the bundle has no seed files."""
    fallback_project: Optional[str] = None
    """
    The name of the project that took the items that named no project, such as a Version 1 bundle's rulesets.
    `None` when every item went into the project it names.
    """
    is_deprecated_bundle: bool = False
    """
    Whether the bundle is in the deprecated Version 1 format,
    or is a hand-built Zip archive without `manifest.json`, which is read as Version 1.
    `import_bundle` sets it from the response's `Deprecation` header, which the response body does not carry.
    """
