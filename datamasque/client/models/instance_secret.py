"""Models for the instance secret of a DataMasque instance, and for linking it with another DataMasque instance."""

import enum
from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict

InstanceSecretEventId = NewType("InstanceSecretEventId", str)


class InstanceSecretEventKind(enum.Enum):
    """Something that happened to the instance secret of a DataMasque instance."""

    initialised = "initialised"
    """The instance secret was created when DataMasque was installed."""
    rotated = "rotated"
    """The instance secret was replaced with a new random one."""
    shared = "shared"
    """The instance secret was sent to another DataMasque instance."""
    adopted = "adopted"
    """The instance secret was replaced with the one from another DataMasque instance."""


class InFlightSyncResult(enum.Enum):
    """
    Whether the IFM service successfully received a new instance secret.

    Masking runs use the new instance secret with every result; only in-flight masking can need a DataMasque restart.
    """

    notified = "notified"
    """The IFM service uses the new instance secret, and nothing more is needed."""
    notify_failed = "notify_failed"
    """Restart DataMasque so that the IFM service uses the new instance secret."""
    file_write_failed = "file_write_failed"
    """Restart DataMasque so that the IFM service uses the new instance secret."""


class InstanceSecretEvent(BaseModel):
    """An initialisation, rotation, share or adoption of the instance secret of a DataMasque instance."""

    model_config = ConfigDict(extra="allow")

    id: InstanceSecretEventId
    created_at: datetime
    kind: InstanceSecretEventKind
    peer_hostname: Optional[str]
    """The hostname of the other DataMasque instance, or `None` when the event involved no other instance."""
    peer_fingerprint: Optional[str]
    """The fingerprint compared with the other DataMasque instance, or `None` when the event involved no other instance."""
    username: str
    """The user who initiated the event."""


class InstanceSecretLinkHistory(BaseModel):
    """The events of the instance secret of a DataMasque instance."""

    model_config = ConfigDict(extra="allow")

    current_links: list[InstanceSecretEvent]
    """The shares and adoptions that no later change to the instance secret has ended."""
    events: list[InstanceSecretEvent]
    """Every event, newest first."""


class InstanceSecretLinkRequest(BaseModel):
    """A request code that the joining DataMasque instance creates, to be pasted on the sharing instance."""

    model_config = ConfigDict(extra="allow")

    request_code: str
    fingerprint: str
    """The request fingerprint, which must match the one shown on the sharing DataMasque instance."""
    expires_at: datetime


class InstanceSecretLinkRequestVerification(BaseModel):
    """What the sharing DataMasque instance reads from a request code, for comparison before sharing."""

    model_config = ConfigDict(extra="allow")

    hostname: str
    """The reported hostname of the joining DataMasque instance."""
    fingerprint: str
    """The request fingerprint, which must match the one shown on the joining DataMasque instance."""
    expires_at: datetime
    """When the request expires."""
    datamasque_version: Optional[str]
    """The version of DataMasque that created the request code. Will always be non-empty for production builds."""


class InstanceSecretLinkResponse(BaseModel):
    """A response code that the sharing DataMasque instance creates, to be pasted on the joining instance."""

    model_config = ConfigDict(extra="allow")

    response_code: str
    response_fingerprint: str
    """The response fingerprint, which must match the one shown on the joining DataMasque instance."""


class InstanceSecretLinkResponseVerification(BaseModel):
    """What the joining DataMasque instance reads from a response code, for comparison before accepting it."""

    model_config = ConfigDict(extra="allow")

    hostname: str
    """The reported hostname of the sharing DataMasque instance."""
    response_fingerprint: str
    """The response fingerprint, which must match the one shown on the sharing DataMasque instance."""
    links_ending: list[InstanceSecretEvent]
    """The current links that accepting the response code will end."""
    verification_token: str
    """Pass this when accepting the response code, which then fails if the instance secret or its history has changed."""
