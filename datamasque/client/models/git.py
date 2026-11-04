from collections.abc import Mapping
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GitSnapshot(BaseModel):
    """
    Git provenance for a ruleset or ruleset library.

    Identifies the commit the entity's contents came from —
    `commit_sha` on `branch` in `repo_url`, as of `synced_at`.
    """

    branch: str
    commit_sha: str
    repo_url: str
    synced_at: datetime


GIT_RESPONSE_FIELDS = ("git_branch", "git_commit_sha", "git_repo_url", "git_synced_at")


def git_snapshot_from_response(data: Mapping[str, Any]) -> Optional[GitSnapshot]:
    """Build a `GitSnapshot` from the server's flat `git_*` fields, or `None` when not git-synced."""
    if data.get("git_branch") is None:
        return None

    return GitSnapshot(
        branch=data["git_branch"],
        commit_sha=data["git_commit_sha"],
        repo_url=data["git_repo_url"],
        synced_at=data["git_synced_at"],
    )


class GitTrackedEntity(BaseModel):
    """Base class for rulesets and ruleset libraries that carry git provenance."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    git: Optional[GitSnapshot] = Field(default=None, exclude=True)
    """Git provenance, or `None` when the entity is not currently in sync with a git commit."""

    @model_validator(mode="before")
    @classmethod
    def _collapse_git_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "git" in data:
            return data

        data = dict(data)
        snapshot = git_snapshot_from_response(data)
        for field in GIT_RESPONSE_FIELDS:
            data.pop(field, None)

        if snapshot is not None:
            data["git"] = snapshot

        return data
