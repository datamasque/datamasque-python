"""Typed response shapes for an unstructured discovery run's results."""

import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from datamasque.client.models.runs import RunConnectionRef, RunId


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


class UnstructuredDiscoverySampleOrder(enum.Enum):
    """The order an unstructured discovery run takes a directory's files in when it reads only some of them."""

    hash = "hash"
    """An order fixed by each file's path, so every run takes the same files."""
    modified_desc = "modified_desc"
    """The most recently modified files first."""
    name_asc = "name_asc"
    """By path, ascending."""


class UnstructuredDiscoverySkippedCount(BaseModel):
    """How many of a directory's files the run skipped for one reason, at one extension."""

    model_config = ConfigDict(extra="allow")

    # A string rather than an enum, because each server release can add reasons.
    skip_reason: str
    """Why the run did not try to read these files, as a code such as `over_sample_limit`."""
    skip_reason_text: str
    """The same reason as a sentence, as the run report words it."""
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
    """
    The label's `name` in the discovery config.
    `get_unstructured_discovery_summary` gives its description and categories.
    """
    occurrences: int


class UnstructuredDiscoveryFile(BaseModel):
    """One file an unstructured discovery run tried to read, and how many times each label was found in it."""

    model_config = ConfigDict(extra="allow")

    path: str
    file_extension: str
    size_bytes: int
    character_count: Optional[int] = None
    """How many characters of the file's text the run searched; `None` for a file the run could not read."""
    status: UnstructuredDiscoveryFileStatus
    # A string rather than an enum, because each server release can add reasons.
    skip_reason: str
    """Why the run could not read the file, as a code such as `unreadable_file`; blank if it read it."""
    skip_reason_text: str
    """The same reason as a sentence, as the run report words it; blank if the run read the file."""
    outcome: UnstructuredDiscoveryFileOutcome
    """What the run made of the file."""
    findings: list[UnstructuredDiscoveryFinding]


class UnstructuredDiscoverySettings(BaseModel):
    """The sampling and size settings an unstructured discovery run used."""

    model_config = ConfigDict(extra="allow")

    files_per_directory: Optional[int] = None
    """How many files of each directory the run read at most, `None` when it read every file."""
    sample_order: UnstructuredDiscoverySampleOrder
    files_per_run: Optional[int] = None
    """How many files the run read at most in all, `None` when it set no limit."""
    max_bytes_per_file: int
    """The size, in bytes, above which the run did not read a file."""


class UnstructuredDiscoveryAiEngine(BaseModel):
    """The AI Engine an unstructured discovery run's `ai_detect` matchers used."""

    # pydantic before 2.10 warns on import about the `model_` prefix of `model_name`, the API's field name.
    model_config = ConfigDict(extra="allow", protected_namespaces=())

    model_name: Optional[str] = None
    """The model the AI Engine reported, `None` when it reported none."""
    engine_version: Optional[str] = None
    """The version the AI Engine reported, `None` when it reported none."""


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
    """An unstructured discovery run, the settings it used and its totals."""

    model_config = ConfigDict(extra="allow")

    run_id: RunId
    connection: RunConnectionRef
    status: str
    """The run's status, such as `finished`."""
    finished_at: Optional[datetime] = None
    """When the run ended, `None` until it has."""
    settings: Optional[UnstructuredDiscoverySettings] = None
    """The settings the run used, `None` when it ended before it read any directory."""
    ai_engine: Optional[UnstructuredDiscoveryAiEngine] = None
    """The AI Engine the run used, `None` when the run recorded neither its model nor its version."""
    report_rows: int
    """
    How many rows the findings report has.
    `get_unstructured_discovery_report` returns a zip of CSV parts above 1,000,000 rows.
    """
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
    """
    Each label of the run's discovery config, most found first; labels found equally often are listed by name.
    A label the run never found is listed with 0 occurrences.
    """
