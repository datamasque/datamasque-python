"""Typed request and response shapes for Safe Data Preview, part of In-Data Discovery."""

from enum import Enum
from typing import Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    SerializeAsAny,
    ValidationError,
)


class StringDisclosureLevel(Enum):
    """How much detail a string Safe Data Preview reveals."""

    lengths = "lengths"
    patterns = "patterns"
    first_chars = "first_chars"


class NumericTemporalDisclosureLevel(Enum):
    """How much detail a numeric or temporal Safe Data Preview reveals."""

    summaries = "summaries"
    histograms = "histograms"


class SafeDataPreviewOptions(BaseModel):
    """Configures whether Safe Data Preview runs and how much detail each column reveals."""

    model_config = ConfigDict(extra="forbid")

    enabled: Optional[bool] = None
    min_group_size: Optional[int] = None
    string_level: Optional[StringDisclosureLevel] = None
    numeric_level: Optional[NumericTemporalDisclosureLevel] = None
    temporal_level: Optional[NumericTemporalDisclosureLevel] = None


class ColumnKind(str, Enum):
    """Which shape a column's Safe Data Preview takes."""

    boolean = "boolean"
    numeric = "numeric"
    string = "string"
    temporal = "temporal"
    unsupported = "unsupported"


class UnsupportedPreviewReason(str, Enum):
    """Known reasons an `UnsupportedPreview` gives for declining a column."""

    no_rows = "No rows sampled"
    all_null = "All sampled values are null"
    all_identical = "All sampled values are identical"
    below_min_group_size = "A value count is below the minimum group size"
    uninterpretable = "Values couldn't be interpreted as a single consistent type"
    is_binary = "Binary data isn't previewed"
    datatype_not_previewed = "This data type has no Safe Data Preview"
    too_many_columns = "Table has too many columns for Safe Data Preview"
    unavailable = "This column could not be previewed"


class CommonStatistics(BaseModel):
    """Row counts shared by every preview kind."""

    model_config = ConfigDict(extra="allow")

    count_row: int
    count_null: int
    count_distinct: Optional[int] = None


class LengthEntry(BaseModel):
    """A value length and how many sampled rows had it."""

    model_config = ConfigDict(extra="allow")

    length: int
    count: int


class LengthsStatistics(BaseModel):
    """Distribution of value lengths."""

    model_config = ConfigDict(extra="allow")

    min: int
    max: int
    mean: float
    median: float
    most_common: list[LengthEntry]


class PatternEntry(BaseModel):
    """A masked character pattern and how many sampled rows matched it."""

    model_config = ConfigDict(extra="allow")

    pattern: str
    count: int


class PatternComposition(BaseModel):
    """The proportion of letter, digit, and other characters across sampled values."""

    model_config = ConfigDict(extra="allow")

    letter: float
    digit: float
    other: float


class PatternsStatistics(BaseModel):
    """The most common character patterns and the overall character composition."""

    model_config = ConfigDict(extra="allow")

    top: list[PatternEntry]
    composition: PatternComposition


class MaskedFormEntry(BaseModel):
    """A masked leading form of a value and how many sampled rows had it."""

    model_config = ConfigDict(extra="allow")

    masked: str
    count: int


class FirstCharsStatistics(BaseModel):
    """The most common masked leading forms."""

    model_config = ConfigDict(extra="allow")

    top: list[MaskedFormEntry]


class StringStatistics(BaseModel):
    """String preview statistics; `patterns`/`first_chars` appear only at the matching disclosure level."""

    model_config = ConfigDict(extra="allow")

    lengths: LengthsStatistics
    patterns: Optional[PatternsStatistics] = None
    first_chars: Optional[FirstCharsStatistics] = None


class NumericSummaries(BaseModel):
    """Numeric mean and percentile summary."""

    model_config = ConfigDict(extra="allow")

    mean: float
    q1: float
    q2: float
    q3: float
    p5: float
    p95: float


class NumericBin(BaseModel):
    """One half-open interval `[lower_bound, upper_bound)` of a numeric histogram and its row count."""

    model_config = ConfigDict(extra="allow")

    lower_bound: float
    upper_bound: float
    count: int


class NumericHistograms(BaseModel):
    """The bins of a numeric histogram."""

    model_config = ConfigDict(extra="allow")

    bins: list[NumericBin]


class NumericStatistics(BaseModel):
    """Numeric preview statistics; `histograms` is present only at the histogram disclosure level."""

    model_config = ConfigDict(extra="allow")

    summaries: NumericSummaries
    histograms: Optional[NumericHistograms] = None


class TemporalSummaries(BaseModel):
    """Temporal mean and percentile summary, each an ISO-formatted string."""

    model_config = ConfigDict(extra="allow")

    mean: str
    q1: str
    q2: str
    q3: str
    p5: str
    p95: str


class TemporalBin(BaseModel):
    """One interval of a temporal histogram, with ISO-formatted bounds and a human-readable label."""

    model_config = ConfigDict(extra="allow")

    lower_bound: str
    upper_bound: str
    label: str
    count: int


class TemporalHistograms(BaseModel):
    """The bins of a temporal histogram."""

    model_config = ConfigDict(extra="allow")

    bins: list[TemporalBin]


class TemporalStatistics(BaseModel):
    """Temporal preview statistics; `histograms` is present only at the histogram disclosure level."""

    model_config = ConfigDict(extra="allow")

    summaries: TemporalSummaries
    histograms: Optional[TemporalHistograms] = None


class BooleanStatistics(BaseModel):
    """Counts of true and false values."""

    model_config = ConfigDict(extra="allow")

    count_true: int
    count_false: int


class UnsupportedStatistics(BaseModel):
    """Why a column could not be previewed. See `UnsupportedPreviewReason` for known values."""

    model_config = ConfigDict(extra="allow")

    reason: str


class ColumnPreview(BaseModel):
    """Fields present on every Safe Data Preview."""

    model_config = ConfigDict(extra="allow")

    # For grouped file discovery, the file whose sample produced this preview; None for database columns.
    sampled_from: Optional[str] = None


class StringPreview(ColumnPreview):
    """A preview of a string column."""

    kind: Literal[ColumnKind.string] = ColumnKind.string
    statistics_common: CommonStatistics
    statistics_kind: StringStatistics


class NumericPreview(ColumnPreview):
    """A preview of a numeric column."""

    kind: Literal[ColumnKind.numeric] = ColumnKind.numeric
    statistics_common: CommonStatistics
    statistics_kind: NumericStatistics


class TemporalPreview(ColumnPreview):
    """A preview of a date/time column."""

    kind: Literal[ColumnKind.temporal] = ColumnKind.temporal
    statistics_common: CommonStatistics
    statistics_kind: TemporalStatistics


class BooleanPreview(ColumnPreview):
    """A preview of a boolean column."""

    kind: Literal[ColumnKind.boolean] = ColumnKind.boolean
    statistics_common: CommonStatistics
    statistics_kind: BooleanStatistics


class UnsupportedPreview(ColumnPreview):
    """A column the preview ran against but declined; `statistics_kind.reason` says why."""

    kind: Literal[ColumnKind.unsupported] = ColumnKind.unsupported
    statistics_common: CommonStatistics
    statistics_kind: UnsupportedStatistics


class UnknownPreview(ColumnPreview):
    """A Safe Data Preview whose `kind` or shape this client can't parse; raw fields stay on `model_extra`."""

    kind: Optional[str] = None
    statistics_common: Optional[CommonStatistics] = None


PREVIEW_BY_KIND: dict[str, type[ColumnPreview]] = {
    ColumnKind.string.value: StringPreview,
    ColumnKind.numeric.value: NumericPreview,
    ColumnKind.temporal.value: TemporalPreview,
    ColumnKind.boolean.value: BooleanPreview,
    ColumnKind.unsupported.value: UnsupportedPreview,
}


def parse_safe_data_preview(value: Union[dict[str, JsonValue], ColumnPreview, None]) -> Optional[ColumnPreview]:
    """Parse a raw preview into its typed variant, or `UnknownPreview` if the kind or shape is unrecognised."""

    if value is None:
        return None
    if isinstance(value, ColumnPreview):
        return value
    if not isinstance(value, dict):
        return None
    kind = value.get("kind")
    preview_type = PREVIEW_BY_KIND.get(kind) if isinstance(kind, str) else None
    if preview_type is not None:
        try:
            return preview_type.model_validate(value)
        except ValidationError:
            pass
    try:
        return UnknownPreview.model_validate(value)
    except ValidationError:
        # Fallback to raw
        return UnknownPreview.model_construct(**value)  # type: ignore[arg-type]


SafeDataPreview = SerializeAsAny[ColumnPreview]
