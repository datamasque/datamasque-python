"""Tests for exporting and importing bundles with the DataMasque client."""

import json
from io import BytesIO
from pathlib import Path

import pytest
import requests_mock

from datamasque.client import (
    BundleImportStatus,
    DataMasqueApiError,
    DataMasqueClient,
    Ruleset,
    RulesetId,
    RulesetType,
)
from tests.helpers import OTHER_PROJECT, parse_multipart_form

BUNDLE = b"PK\x03\x04 bundle"

V2_IMPORT_RESPONSE = {
    "rulesets": {
        "metadata": {"processed": 1, "created": 1, "restored": 0, "error": 0},
        "data": [
            {
                "exported_name": "Customers",
                "exported_project": "payments",
                "exported_id": "9d641e97-adf7-4f22-9089-afc3711bf222",
                "imported_name": "Customers",
                "imported_id": "9d641e97-adf7-4f22-9089-afc3711bf222",
                "imported_project": "payments",
                "ruleset_type": "database",
                "status": "CREATED",
                "message": 'Ruleset named "Customers" did not exist, and was created.',
            }
        ],
    },
    "libraries": {
        "metadata": {"processed": 1, "created": 0, "restored": 0, "error": 0, "updated": 0, "skipped": 1},
        "data": [
            {
                "exported_name": "pii_masks",
                "exported_namespace": "",
                "exported_project": "payments",
                "exported_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "imported_name": "pii_masks",
                "imported_namespace": "payments",
                "imported_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "imported_project": "payments",
                "status": "SKIPPED",
                "message": 'A library with ID "a1b2c3d4-e5f6-7890-abcd-ef1234567890" already exists.',
            }
        ],
    },
}

V1_IMPORT_RESPONSE = {
    "rulesets": {
        "metadata": {"processed": 1, "created": 1, "restored": 0, "error": 0},
        "data": [
            {
                "exported_name": "Customers",
                "exported_project": None,
                "exported_id": None,
                "imported_name": "Customers",
                "imported_id": "04ea20f0-ad4c-498e-881f-b0bc79d83ba7",
                "imported_project": "Banking",
                "ruleset_type": "file",
                "status": "CREATED",
                "message": 'Ruleset named "Customers" did not exist, and was created.',
            }
        ],
    },
    "seed_files": {
        "metadata": {"processed": 1, "created": 0, "restored": 0, "error": 1, "updated": 0, "skipped": 0},
        "data": [
            {
                "exported_name": "names.csv",
                "exported_project": None,
                "imported_name": None,
                "imported_project": None,
                "status": "ERROR",
                "message": "The seed file could not be written.",
            }
        ],
    },
    "fallback_project": "Banking",
}


def test_export_bundle_exports_everything_by_default(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/export/v2/", content=BUNDLE, headers={"Content-Type": "application/zip"})
        bundle = client.export_bundle()

    assert bundle == BUNDLE
    assert m.last_request.json() == {"include_seed_files": False}


def test_export_bundle_sends_the_rulesets_and_seed_file_option(client: DataMasqueClient) -> None:
    ruleset = Ruleset(name="Customers")
    ruleset.id = RulesetId("9d641e97-adf7-4f22-9089-afc3711bf222")
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/export/v2/", content=BUNDLE)
        client.export_bundle([ruleset, RulesetId("04ea20f0-ad4c-498e-881f-b0bc79d83ba7")], include_seed_files=True)

    assert m.last_request.json() == {
        "include_seed_files": True,
        "ruleset_ids": ["9d641e97-adf7-4f22-9089-afc3711bf222", "04ea20f0-ad4c-498e-881f-b0bc79d83ba7"],
    }


def test_export_bundle_rejects_a_ruleset_not_yet_created(client: DataMasqueClient) -> None:
    with pytest.raises(ValueError, match="not been created"):
        client.export_bundle([Ruleset(name="Customers")])


@pytest.mark.parametrize("rulesets", [RulesetId("9d641e97-adf7-4f22-9089-afc3711bf222"), Ruleset(name="Customers")])
def test_export_bundle_rejects_a_single_ruleset(client: DataMasqueClient, rulesets: object) -> None:
    with pytest.raises(TypeError, match="as a sequence"):
        client.export_bundle(rulesets)  # type: ignore[arg-type]


def test_import_bundle_uploads_the_archive_with_default_options(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/import/v2/", json=V2_IMPORT_RESPONSE, status_code=201)
        result = client.import_bundle(BUNDLE)

    form = parse_multipart_form(m.last_request)
    assert form == {
        "overwrite_libraries": "false",
        "overwrite_seed_files": "false",
        "zip_archive": {"filename": "bundle.zip", "content_type": "application/zip", "content": BUNDLE},
    }
    (ruleset,) = result.rulesets.data
    assert ruleset.imported_project == "payments"
    assert ruleset.ruleset_type is RulesetType.database
    assert ruleset.status is BundleImportStatus.created
    assert result.rulesets.metadata.updated is None
    assert result.libraries is not None
    assert result.libraries.metadata.skipped == 1
    assert result.libraries.data[0].status is BundleImportStatus.skipped
    assert result.seed_files is None
    assert result.fallback_project is None
    assert result.is_deprecated_bundle is False


@pytest.mark.parametrize(("overwrite", "form_value"), [(True, "true"), (False, "false")])
def test_import_bundle_sends_its_options(
    client: DataMasqueClient, tmp_path: Path, overwrite: bool, form_value: str
) -> None:
    bundle_path = tmp_path / "export.zip"
    bundle_path.write_bytes(BUNDLE)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/import/v2/", json=V2_IMPORT_RESPONSE)
        client.import_bundle(
            bundle_path,
            overwrite_rulesets=overwrite,
            overwrite_libraries=overwrite,
            overwrite_seed_files=overwrite,
            fallback_project=OTHER_PROJECT,
        )

    form = parse_multipart_form(m.last_request)
    assert form["zip_archive"]["filename"] == "export.zip"
    assert form["overwrite_rulesets"] == form_value
    assert form["overwrite_libraries"] == form_value
    assert form["overwrite_seed_files"] == form_value
    assert form["fallback_project"] == "2"


def test_import_bundle_uploads_all_of_a_partly_read_file(client: DataMasqueClient) -> None:
    bundle = BytesIO(BUNDLE)
    bundle.read(4)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/import/v2/", json=V2_IMPORT_RESPONSE)
        client.import_bundle(bundle)

    archive = parse_multipart_form(m.last_request)["zip_archive"]
    assert archive["filename"] == "bundle.zip"
    assert archive["content"] == BUNDLE


def test_import_bundle_rejects_a_fallback_project_that_is_not_a_reference(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError, match="not a project ID or reference"):
            client.import_bundle(BUNDLE, fallback_project="banking")

    assert not m.called


def test_import_bundle_does_not_send_the_client_default_project(client: DataMasqueClient) -> None:
    client.default_project = "1-default"
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/import/v2/", json=V1_IMPORT_RESPONSE)
        client.import_bundle(BUNDLE)

    assert "fallback_project" not in parse_multipart_form(m.last_request)


def test_import_bundle_reports_a_v1_bundle_and_its_fallback_project(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/import/v2/",
            json=V1_IMPORT_RESPONSE,
            status_code=201,
            headers={"Deprecation": "true"},
        )
        result = client.import_bundle(BUNDLE, fallback_project="2-banking")

    assert parse_multipart_form(m.last_request)["fallback_project"] == "2"
    assert result.is_deprecated_bundle is True
    assert result.fallback_project == "Banking"
    assert result.rulesets.data[0].exported_project is None
    assert result.seed_files is not None
    (seed_file,) = result.seed_files.data
    assert seed_file.status is BundleImportStatus.error
    assert seed_file.imported_project is None


def test_import_bundle_raises_when_the_server_rejects_it(client: DataMasqueClient) -> None:
    detail = 'The bundle names the project "payments", which does not exist. Creating it requires an administrator.'
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/import/v2/", status_code=403, text=json.dumps({"detail": detail}))
        with pytest.raises(DataMasqueApiError) as error:
            client.import_bundle(BUNDLE)

    assert error.value.response.status_code == 403
