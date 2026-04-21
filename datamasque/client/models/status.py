import enum


class ValidationStatus(enum.Enum):
    """Validation status of a ruleset or ruleset library."""

    valid = "valid"
    invalid = "invalid"
    in_progress = "in_progress"
    unknown = "unknown"


class MaskingRunStatus(enum.Enum):
    """List of valid masking run statuses."""

    finished = "finished"
    finished_with_warnings = "finished_with_warnings"
    queued = "queued"
    running = "running"
    failed = "failed"
    validating = "validating"
    cancelling = "cancelling"
    cancelled = "cancelled"

    @classmethod
    def get_final_states(cls) -> set["MaskingRunStatus"]:
        """Returns the list of final statuses, i.e. the run is completed, successfully or otherwise."""

        return {cls.finished, cls.finished_with_warnings, cls.cancelled, cls.failed}

    @classmethod
    def get_finished_states(cls) -> set["MaskingRunStatus"]:
        """Returns the list of statuses that indicate the run completed successfully."""

        return {cls.finished, cls.finished_with_warnings}

    @property
    def is_in_final_state(self) -> bool:
        """Returns True if this status is a final status."""

        return self in self.get_final_states()

    @property
    def is_finished(self) -> bool:
        """Returns True if this status is a finished status."""

        return self in self.get_finished_states()


class AsyncRulesetGenerationTaskStatus(enum.Enum):
    """List of statuses of async ruleset generation tasks."""

    finished = "finished"
    failed = "failed"
    running = "running"
    queued = "queued"

    @classmethod
    def get_final_states(cls) -> set["AsyncRulesetGenerationTaskStatus"]:
        """Returns the list of final statuses, i.e. the ruleset generation has completed, successfully or otherwise."""

        return {cls.finished, cls.failed}

    @property
    def is_in_final_state(self) -> bool:
        """Returns True if this status is a final status."""

        return self in self.get_final_states()
