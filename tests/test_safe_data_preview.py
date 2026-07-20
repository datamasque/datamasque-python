"""Tests for the typed Safe Data Preview models."""

import pytest
from pydantic import BaseModel, ValidationError

from datamasque.client import FileDiscoveryResult, SchemaDiscoveryColumn
from datamasque.client.models.safe_data_preview import (
    NumericPreview,
    UnsupportedPreview,
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

_KNOWN_PREVIEWS = [
    pytest.param(_STRING_PREVIEW, id="string"),
    pytest.param(_NUMERIC_PREVIEW, id="numeric"),
    pytest.param(_TEMPORAL_PREVIEW, id="temporal"),
    pytest.param(_BOOLEAN_PREVIEW, id="boolean"),
    pytest.param(_UNSUPPORTED_PREVIEW, id="unsupported"),
]


def _assert_no_unexpected_extras(value: object) -> None:
    """Recursively assert no field landed in a model's `model_extra` -- i.e. everything is typed."""
    if isinstance(value, BaseModel):
        assert not value.model_extra, f"unexpected extra fields on {type(value).__name__}: {value.model_extra}"
        for field_value in value.__dict__.values():
            _assert_no_unexpected_extras(field_value)
    elif isinstance(value, list):
        for item in value:
            _assert_no_unexpected_extras(item)


def _build_column_data(**overrides: object) -> dict[str, object]:
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


@pytest.mark.parametrize("payload", _KNOWN_PREVIEWS)
def test_each_preview_has_no_untyped_fields(payload):
    column = SchemaDiscoveryColumn.model_validate(_build_column_data(safe_data_preview=payload))
    _assert_no_unexpected_extras(column.safe_data_preview)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"safe_data_preview": None}, id="explicit-null"),
        pytest.param({}, id="absent"),
    ],
)
def test_column_without_preview_is_none(overrides):
    column = SchemaDiscoveryColumn.model_validate(_build_column_data(**overrides))
    assert column.safe_data_preview is None


def test_column_unsupported_preview_is_distinct_from_none():
    column = SchemaDiscoveryColumn.model_validate(_build_column_data(safe_data_preview=_UNSUPPORTED_PREVIEW))
    assert column.safe_data_preview is not None
    assert isinstance(column.safe_data_preview, UnsupportedPreview)


@pytest.mark.parametrize(
    "safe_data_preview",
    [
        pytest.param({"kind": "geospatial", "statistics_common": {"count_row": 1, "count_null": 0}}, id="unknown-kind"),
        pytest.param({"kind": "numeric", "statistics_common": {"count_row": 5, "count_null": 0}}, id="broken-shape"),
        pytest.param({"kind": 123, "statistics_common": {"count_row": "oops"}}, id="malformed"),
        pytest.param("not a preview", id="non-dict"),
    ],
)
def test_column_rejects_unparseable_preview(safe_data_preview):
    with pytest.raises(ValidationError):
        SchemaDiscoveryColumn.model_validate(_build_column_data(safe_data_preview=safe_data_preview))


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
