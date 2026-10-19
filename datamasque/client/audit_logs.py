from itertools import islice
from typing import Iterator, Optional

from datamasque.client.base import BaseClient
from datamasque.client.models.audit_log import AuditLog


class AuditLogClient(BaseClient):
    """Audit log API methods. Mixed into `DataMasqueClient`."""

    def iter_audit_logs(self) -> Iterator[AuditLog]:
        """
        Lazily iterate audit log entries, newest first, via the paginated endpoint.

        Requires an admin user.
        """

        return self._iter_paginated("/api/audit-logs/", model=AuditLog)

    def list_audit_logs(self, *, limit: Optional[int] = None) -> list[AuditLog]:
        """
        Lists audit log entries, newest first.

        When `limit` is given, returns at most that many of the newest entries,
        and fetches only the pages they are on.
        When it is `None` (the default), returns every entry.
        Requires an admin user.
        """

        entries = self.iter_audit_logs()
        if limit is None:
            return list(entries)

        return list(islice(entries, limit))
