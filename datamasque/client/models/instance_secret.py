"""Models for the instance secret of a DataMasque instance, and for linking it with another DataMasque instance."""

import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class InstanceSecretLinkKind(enum.Enum):
    """How a DataMasque instance came to share its instance secret with another DataMasque instance."""

    shared = "shared"
    """The instance secret was sent to another DataMasque instance."""
    adopted = "adopted"
    """The instance secret was replaced with the one from another DataMasque instance."""


class InFlightSyncResult(enum.Enum):
    """
    Whether the in-flight masking service successfully received a new instance secret.

    Only in-flight masking can need a DataMasque restart after a new instance secret.
    """

    notified = "notified"
    """The in-flight masking service uses the new instance secret, and nothing more is needed."""
    notify_failed = "notify_failed"
    """Restart DataMasque so that the in-flight masking service uses the new instance secret."""
    file_write_failed = "file_write_failed"
    """Restart DataMasque so that the in-flight masking service uses the new instance secret."""


class InstanceSecretLink(BaseModel):
    """A share or adoption that linked the instance secret of a DataMasque instance with another instance."""

    model_config = ConfigDict(extra="allow")

    id: str
    created_at: datetime
    kind: InstanceSecretLinkKind
    peer_hostname: str
    """The hostname of the other DataMasque instance."""
    request_fingerprint: str
    """The fingerprint of the request code."""
    response_fingerprint: str
    """The fingerprint of the response code."""
    username: str
    """The user who initiated the share or adoption."""


class InstanceSecretLinkRequest(BaseModel):
    """A request code that the joining DataMasque instance creates, to be pasted on the sharing instance."""

    model_config = ConfigDict(extra="allow")

    request_code: str
    request_fingerprint: str
    """The request fingerprint, which must match the one shown on the sharing DataMasque instance."""
    expires_at: datetime


class InstanceSecretLinkRequestVerification(BaseModel):
    """What the sharing DataMasque instance reads from a request code, for comparison before sharing."""

    model_config = ConfigDict(extra="allow")

    peer_hostname: str
    """The reported hostname of the joining DataMasque instance."""
    request_fingerprint: str
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

    peer_hostname: str
    """The reported hostname of the sharing DataMasque instance."""
    response_fingerprint: str
    """The response fingerprint, which must match the one shown on the sharing DataMasque instance."""
    links_ending: list[InstanceSecretLink]
    """The current links that accepting the response code will end."""
    verification_token: str
    """
    Pass this when accepting the response code.
    The accept fails if the instance secret changed or was shared since.
    """
