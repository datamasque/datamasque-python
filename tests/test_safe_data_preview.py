"""Tests for the typed Safe Data Preview models and their tolerant parsing."""

from pydantic import BaseModel

from datamasque.client import FileDiscoveryResult, SchemaDiscoveryColumn
from datamasque.client.models.safe_data_preview import (
    BooleanPreview,
    CommonStatistics,
    NumericPreview,
    StringPreview,
    TemporalPreview,
    UnknownPreview,
    UnsupportedPreview,
    UnsupportedPreviewReason,
    parse_safe_data_preview,
)

_STRING_PREVIEW = {
    "kind": "string",
    "sampled_from": None,
    "statistics_common": {"count_row": 1000, "count_null": 12, "count_distinct": 988},
    "statistics_kind": {
        "lengths": {
            "min": 9,
            "max": 34,
            "mean": 21.4,
            "median": 21.0,
            "most_common": [{"length": 20, "count": 140}, {"length": 22, "count": 118}],
        },
        "patterns": {
            "top": [
                {"pattern": "aaaa@aaaa.aaa", "count": 512},
                {"pattern": "aaaa.aaaa@aaaa.aaa", "count": 300},
            ],
            "composition": {"letter": 0.78, "digit": 0.05, "other": 0.17},
        },
        "first_chars": {"top": [{"masked": "j*******", "count": 40}, {"masked": "s*******", "count": 33}]},
    },
}

_NUMERIC_PREVIEW = {
    "kind": "numeric",
    "sampled_from": None,
    "statistics_common": {"count_row": 1000, "count_null": 0, "count_distinct": 71},
    "statistics_kind": {
        "summaries": {"mean": 41.3, "q1": 29.0, "q2": 41.0, "q3": 54.0, "p5": 19.0, "p95": 68.0},
        "histograms": {
            "bins": [
                {"lower_bound": 18.0, "upper_bound": 28.0, "count": 210},
                {"lower_bound": 28.0, "upper_bound": 38.0, "count": 240},
            ]
        },
    },
}

_TEMPORAL_PREVIEW = {
    "kind": "temporal",
    "sampled_from": None,
    "statistics_common": {"count_row": 500, "count_null": 3, "count_distinct": 480},
    "statistics_kind": {
        "summaries": {
            "mean": "1987-04-12",
            "q1": "1975-01-01",
            "q2": "1988-06-30",
            "q3": "2001-03-15",
            "p5": "1960-01-01",
            "p95": "2010-12-31",
        },
        "histograms": {
            "bins": [
                {"lower_bound": "1970-01-01", "upper_bound": "1980-01-01", "label": "1970s", "count": 88},
                {"lower_bound": "1980-01-01", "upper_bound": "1990-01-01", "label": "1980s", "count": 142},
            ]
        },
    },
}

_BOOLEAN_PREVIEW = {
    "kind": "boolean",
    "sampled_from": None,
    "statistics_common": {"count_row": 1000, "count_null": 0, "count_distinct": 2},
    "statistics_kind": {"count_true": 640, "count_false": 360},
}

_UNSUPPORTED_PREVIEW = {
    "kind": "unsupported",
    "sampled_from": None,
    "statistics_common": {"count_row": 0, "count_null": 0, "count_distinct": None},
    "statistics_kind": {"reason": "Binary data isn't previewed"},
}


def _assert_no_unexpected_extras(value: object) -> None:
    """Recursively assert no field landed in a model's `model_extra` -- i.e. everything is typed."""
    if isinstance(value, BaseModel):
        assert not value.model_extra, f"unexpected extra fields on {type(value).__name__}: {value.model_extra}"
        for field_value in value.__dict__.values():
            _assert_no_unexpected_extras(field_value)
    elif isinstance(value, list):
        for item in value:
            _assert_no_unexpected_extras(item)


def test_each_preview_parses_to_its_typed_variant():
    assert type(parse_safe_data_preview(_STRING_PREVIEW)) is StringPreview
    assert type(parse_safe_data_preview(_NUMERIC_PREVIEW)) is NumericPreview
    assert type(parse_safe_data_preview(_TEMPORAL_PREVIEW)) is TemporalPreview
    assert type(parse_safe_data_preview(_BOOLEAN_PREVIEW)) is BooleanPreview
    assert type(parse_safe_data_preview(_UNSUPPORTED_PREVIEW)) is UnsupportedPreview


def test_each_preview_round_trips():
    assert parse_safe_data_preview(_STRING_PREVIEW).model_dump(mode="json") == _STRING_PREVIEW
    assert parse_safe_data_preview(_NUMERIC_PREVIEW).model_dump(mode="json") == _NUMERIC_PREVIEW
    assert parse_safe_data_preview(_TEMPORAL_PREVIEW).model_dump(mode="json") == _TEMPORAL_PREVIEW
    assert parse_safe_data_preview(_BOOLEAN_PREVIEW).model_dump(mode="json") == _BOOLEAN_PREVIEW
    assert parse_safe_data_preview(_UNSUPPORTED_PREVIEW).model_dump(mode="json") == _UNSUPPORTED_PREVIEW


def test_each_preview_has_no_untyped_fields():
    _assert_no_unexpected_extras(parse_safe_data_preview(_STRING_PREVIEW))
    _assert_no_unexpected_extras(parse_safe_data_preview(_NUMERIC_PREVIEW))
    _assert_no_unexpected_extras(parse_safe_data_preview(_TEMPORAL_PREVIEW))
    _assert_no_unexpected_extras(parse_safe_data_preview(_BOOLEAN_PREVIEW))
    _assert_no_unexpected_extras(parse_safe_data_preview(_UNSUPPORTED_PREVIEW))


def test_numeric_preview_typed_access():
    preview = parse_safe_data_preview(_NUMERIC_PREVIEW)
    assert isinstance(preview, NumericPreview)
    assert preview.statistics_common.count_row == 1000
    assert preview.statistics_kind.summaries.mean == 41.3
    assert preview.statistics_kind.histograms.bins[0].count == 210


def test_string_preview_typed_access():
    preview = parse_safe_data_preview(_STRING_PREVIEW)
    assert isinstance(preview, StringPreview)
    assert preview.statistics_kind.lengths.most_common[0].length == 20
    assert preview.statistics_kind.patterns.composition.letter == 0.78
    assert preview.statistics_kind.first_chars.top[0].masked == "j*******"


def test_unsupported_preview_reason_reads_as_str_and_matches_known_constant():
    preview = parse_safe_data_preview(_UNSUPPORTED_PREVIEW)
    assert isinstance(preview, UnsupportedPreview)
    assert UnsupportedPreviewReason(preview.statistics_kind.reason) is UnsupportedPreviewReason.is_binary
    assert preview.statistics_kind.reason == "Binary data isn't previewed"


def test_none_passes_through():
    assert parse_safe_data_preview(None) is None


def test_already_parsed_model_passes_through():
    preview = parse_safe_data_preview(_BOOLEAN_PREVIEW)
    assert parse_safe_data_preview(preview) is preview


def test_unknown_kind_degrades():
    payload = {
        "kind": "geospatial",
        "sampled_from": None,
        "statistics_common": {"count_row": 5, "count_null": 0, "count_distinct": 5},
        "statistics_kind": {"crs": "EPSG:4326"},
    }
    preview = parse_safe_data_preview(payload)
    assert isinstance(preview, UnknownPreview)
    assert preview.kind == "geospatial"
    assert preview.model_extra["statistics_kind"] == {"crs": "EPSG:4326"}


def test_known_kind_with_broken_shape_degrades():
    # `numeric`, but missing the required `statistics_kind`.
    payload = {
        "kind": "numeric",
        "sampled_from": None,
        "statistics_common": {"count_row": 5, "count_null": 0, "count_distinct": 5},
    }
    preview = parse_safe_data_preview(payload)
    assert isinstance(preview, UnknownPreview)
    assert preview.kind == "numeric"
    assert isinstance(preview.statistics_common, CommonStatistics)
    assert preview.statistics_common.count_null == 0


def test_unparseable_payload_falls_back_without_raising():
    payload = {"kind": 123, "sampled_from": "f.csv", "statistics_common": {"count_row": "oops"}}
    preview = parse_safe_data_preview(payload)
    assert isinstance(preview, UnknownPreview)
    assert preview.kind == 123


def test_non_dict_preview_is_none():
    assert parse_safe_data_preview("not a preview") is None
    assert parse_safe_data_preview(42) is None


def _column_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "data_type": "text",
        "foreign_keys": [],
        "discovery_matches": [],
        "constraint_columns": [],
        "unique_index_names": [],
        "referencing_foreign_keys": [],
        "constraint": "",
    }
    data.update(overrides)
    return data


def test_column_null_preview_is_none():
    column = SchemaDiscoveryColumn.model_validate(_column_data(safe_data_preview=None))
    assert column.safe_data_preview is None


def test_column_absent_preview_is_none():
    column = SchemaDiscoveryColumn.model_validate(_column_data())
    assert column.safe_data_preview is None


def test_column_unsupported_preview_is_distinct_from_none():
    column = SchemaDiscoveryColumn.model_validate(_column_data(safe_data_preview=_UNSUPPORTED_PREVIEW))
    assert column.safe_data_preview is not None
    assert isinstance(column.safe_data_preview, UnsupportedPreview)


def test_column_unknown_preview_does_not_break_the_result():
    column = SchemaDiscoveryColumn.model_validate(
        _column_data(safe_data_preview={"kind": "geospatial", "statistics_common": {"count_row": 1, "count_null": 0}})
    )
    assert isinstance(column.safe_data_preview, UnknownPreview)
    assert column.data_type == "text"  # the rest of the column still parsed


def test_column_model_dump_preserves_preview_fields():
    # SerializeAsAny guard: dumping through the ColumnPreview-typed field must keep the concrete fields.
    column = SchemaDiscoveryColumn.model_validate(_column_data(safe_data_preview=_NUMERIC_PREVIEW))
    dumped = column.model_dump(mode="json")["safe_data_preview"]
    assert dumped["kind"] == "numeric"
    assert dumped["statistics_kind"]["summaries"]["mean"] == 41.3


def _file_discovery_payload(locator_extra: dict[str, object]) -> dict[str, object]:
    return {
        "id": 7,
        "connection": {"id": "conn-1", "name": "my files"},
        "file_type": "csv",
        "files": [{"path": "data/people.csv", "file_type": "csv"}],
        "results": [{"locator": "age", "data_types": ["integer"], "matches": [], **locator_extra}],
    }


def test_file_locator_preview_parses_with_sampled_from():
    payload = _file_discovery_payload({"safe_data_preview": {**_NUMERIC_PREVIEW, "sampled_from": "data/people.csv"}})
    result = FileDiscoveryResult.model_validate(payload)
    preview = result.results[0].safe_data_preview
    assert isinstance(preview, NumericPreview)
    assert preview.sampled_from == "data/people.csv"


def test_file_locator_without_preview_is_none():
    result = FileDiscoveryResult.model_validate(_file_discovery_payload({}))
    assert result.results[0].safe_data_preview is None
