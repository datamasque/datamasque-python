"""Typed response shapes for an unstructured discovery run's results."""

import enum
from datetime import datetime
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict

from datamasque.client.models.pagination import Page
from datamasque.client.models.runs import RunConnectionRef, RunId

T = TypeVar("T")


class UnstructuredDiscoveryFileStatus(enum.Enum):
    """Whether an unstructured discovery run read a file's text."""

    read = "read"
    """The run read the file; its `character_count` is 0 when it held no text to search."""
    failed = "failed"
    """The run tried to read the file and could not; `skip_reason` says why."""


class UnstructuredDiscoveryFileOutcome(enum.Enum):
    """What an unstructured discovery run made of a file it tried to read."""

    findings = "findings"
    """The run found at least one label in the file."""
    no_text = "no_text"
    """The run read the file and found no text to search in it."""
    nothing_found = "nothing_found"
    """The run read the file's text and found no label in it."""
    failed = "failed"
    """The run tried to read the file and could not."""


class UnstructuredDiscoveryDirectoryOrdering(enum.Enum):
    """
    A column the directories of an unstructured discovery run are sorted by.

    Directories that tie are sorted by path.
    """

    directory = "directory"
    """The directory's path."""
    files_found = "files_found"
    files_read = "files_read"
    files_with_findings = "files_with_findings"
    """With a `labels` or `categories` filter, only a file holding a matching label counts."""
    size_bytes = "size_bytes"
    occurrences = "occurrences"
    """
    The sum of the directory's `entity_counts`.
    With a `labels` or `categories` filter, only the matching labels count.
    """


class UnstructuredDiscoverySampling(enum.Enum):
    """How many files of each directory an unstructured discovery run read."""

    sampled = "sampled"
    """At most `files_per_directory` files of each directory."""
    all = "all"
    """The run set no limit on how many files of each directory it read."""
    unknown = "unknown"
    """The run's discovery config cannot be read."""


class UnstructuredDiscoverySkippedCount(BaseModel):
    """How many of a directory's files the run skipped for one reason, at one extension."""

    model_config = ConfigDict(extra="allow")

    # A string rather than an enum, because each server release can add reasons.
    skip_reason: str
    file_extension: str
    """The extension without a leading dot, blank for files that have none."""
    count: int


class UnstructuredDiscoveryDirectory(BaseModel):
    """One directory an unstructured discovery run found on the connection, and what its files add up to."""

    model_config = ConfigDict(extra="allow")

    directory: str
    """The directory's path, blank at the connection root."""
    files_found: int
    """How many files were found in the directory, read or not."""
    files_read: int
    """
    How many of the directory's files the run read,
    including those with no text to search but not those it could not read.
    """
    files_with_findings: int
    """
    How many of the directory's files the run read hold a finding.
    With a `labels` or `categories` filter, only a file holding a matching label counts.
    """
    size_bytes: int
    """The total size, in bytes, of the directory's files, read or not."""
    entity_counts: dict[str, int]
    """Each label's occurrences, summed over the directory's files."""
    skipped: list[UnstructuredDiscoverySkippedCount]
    """
    How many of the directory's files the run skipped, by reason and extension.
    A file a running or cancelled run has not reached yet is counted in `files_found` but not here.
    """


class UnstructuredDiscoveryFinding(BaseModel):
    """How many times one label was found in one file."""

    model_config = ConfigDict(extra="allow")

    label: str
    """The label's `name` in the discovery config."""
    description: str
    occurrences: int
    categories: list[str]


class UnstructuredDiscoveryFile(BaseModel):
    """One file an unstructured discovery run tried to read, and how many times each label was found in it."""

    model_config = ConfigDict(extra="allow")

    path: str
    file_extension: str
    size_bytes: int
    character_count: Optional[int] = None
    """How many characters of the file's text the run searched; `None` for a file the run could not read."""
    status: UnstructuredDiscoveryFileStatus
    skip_reason: str
    """Why the run could not read the file, blank if it did."""
    outcome: UnstructuredDiscoveryFileOutcome
    """What the run made of the file."""
    findings: list[UnstructuredDiscoveryFinding]


class UnstructuredDiscoveryPage(Page[T], Generic[T]):
    """
    A page of an unstructured discovery run's directories, or of one directory's files.

    Every page also carries the run it belongs to and how the run sampled each directory.
    `get_unstructured_discovery_summary` returns the run's totals.
    """

    run_id: RunId
    connection: RunConnectionRef
    finished_at: Optional[datetime] = None
    """When the run ended, `None` until it has."""
    sampling: UnstructuredDiscoverySampling
    files_per_directory: Optional[int] = None
    """The sample size when `sampling` is `UnstructuredDiscoverySampling.sampled`, `None` otherwise."""


class UnstructuredDiscoveryLabelTotal(BaseModel):
    """How many times one label was found over an unstructured discovery run's files."""

    model_config = ConfigDict(extra="allow")

    label: str
    """The label's `name` in the discovery config."""
    description: str
    categories: list[str]
    occurrences: int
    """How many times the label was found, summed over the files."""
    file_count: int
    """How many files hold the label."""


class UnstructuredDiscoverySummary(BaseModel):
    """An unstructured discovery run's totals."""

    model_config = ConfigDict(extra="allow")

    run_id: RunId
    files_found: int
    """How many files were found on the connection, summed over the directories."""
    files_read: int
    """How many files the run read, including those with no text to search but not those it could not read."""
    files_failed: int
    """How many files the run tried to read and could not."""
    files_skipped: int
    """
    How many of the files found the run has not tried to read:
    those it skipped, and those a running or cancelled run has not reached.
    `files_found` is `files_read` plus `files_failed` plus `files_skipped`.
    """
    files_without_text: int
    """How many of the files the run read held no text to search."""
    files_with_findings: int
    """How many of the files the run read hold a finding."""
    bytes_opened: int
    """The total size, in bytes, of the files the run read."""
    files_over_run_limit: int
    """How many files the run did not read because it reached the config's `files_per_run`."""
    labels: list[UnstructuredDiscoveryLabelTotal]
    """Each label found, most found first; labels found equally often are listed by name."""
