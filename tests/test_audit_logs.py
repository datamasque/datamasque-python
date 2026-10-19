"""Tests for `AuditLogClient` (listing audit log entries)."""

from datetime import datetime, timezone

import requests_mock

from datamasque.client.models.audit_log import AuditLog, AuditLogAction

AUDIT_LOGS_URL = "http://test-server/api/audit-logs/"


def audit_log_json(id, *, action="created", project=None, project_name=None):
    return {
        "id": id,
        "timestamp": f"2026-10-{id:02d}T09:30:00Z",
        "username": "admin",
        "category": "settings",
        "action": action,
        "description": f"admin made change {id}",
        "project": project,
        "project_name": project_name,
    }


def test_list_audit_logs_parses_fields(client):
    page = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [audit_log_json(7, action="deleted", project="3-payments", project_name="Payments")],
    }
    with requests_mock.Mocker() as m:
        m.get(AUDIT_LOGS_URL, json=page, status_code=200)
        audit_logs = client.list_audit_logs()

    assert audit_logs == [
        AuditLog(
            id=7,
            timestamp=datetime(2026, 10, 7, 9, 30, tzinfo=timezone.utc),
            username="admin",
            category="settings",
            action=AuditLogAction.deleted,
            description="admin made change 7",
            project="3-payments",
            project_name="Payments",
        )
    ]
    assert audit_logs[0].action is AuditLogAction.deleted
    assert audit_logs[0].action == "deleted"


def test_list_audit_logs_accepts_entries_without_project_fields(client):
    entry = audit_log_json(1, action="logged_in")
    del entry["project"]
    del entry["project_name"]
    with requests_mock.Mocker() as m:
        m.get(AUDIT_LOGS_URL, json={"count": 1, "next": None, "previous": None, "results": [entry]})
        audit_logs = client.list_audit_logs()

    assert audit_logs[0].action is AuditLogAction.logged_in
    assert audit_logs[0].project is None
    assert audit_logs[0].project_name is None


def test_list_audit_logs_follows_pages_newest_first(client):
    page1 = {
        "count": 3,
        "next": f"{AUDIT_LOGS_URL}?limit=100&offset=100",
        "previous": None,
        "results": [audit_log_json(3), audit_log_json(2)],
    }
    page2 = {
        "count": 3,
        "next": None,
        "previous": f"{AUDIT_LOGS_URL}?limit=100",
        "results": [audit_log_json(1)],
    }
    with requests_mock.Mocker() as m:
        m.get(AUDIT_LOGS_URL, [{"json": page1, "status_code": 200}, {"json": page2, "status_code": 200}])
        audit_logs = client.list_audit_logs()

    assert [audit_log.id for audit_log in audit_logs] == [3, 2, 1]
    assert m.request_history[0].qs == {"limit": ["100"], "offset": ["0"]}
    assert m.request_history[1].qs == {"limit": ["100"], "offset": ["100"]}


def test_list_audit_logs_with_limit_stops_fetching_pages(client):
    page1 = {
        "count": 3,
        "next": f"{AUDIT_LOGS_URL}?limit=100&offset=100",
        "previous": None,
        "results": [audit_log_json(3), audit_log_json(2)],
    }
    with requests_mock.Mocker() as m:
        m.get(AUDIT_LOGS_URL, json=page1, status_code=200)
        audit_logs = client.list_audit_logs(limit=1)

    assert [audit_log.id for audit_log in audit_logs] == [3]
    assert m.call_count == 1


def test_list_audit_logs_with_zero_limit_sends_no_request(client):
    with requests_mock.Mocker() as m:
        audit_logs = client.list_audit_logs(limit=0)

    assert audit_logs == []
    assert m.call_count == 0


def test_iter_audit_logs_is_lazy(client):
    page1 = {
        "count": 2,
        "next": f"{AUDIT_LOGS_URL}?limit=100&offset=100",
        "previous": None,
        "results": [audit_log_json(2)],
    }
    with requests_mock.Mocker() as m:
        m.get(AUDIT_LOGS_URL, json=page1, status_code=200)
        newest = next(client.iter_audit_logs())

    assert newest.id == 2
    assert m.call_count == 1
