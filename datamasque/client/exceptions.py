from requests import Response


class DataMasqueException(Exception):
    """Generic exception base class."""


class DataMasqueUserError(DataMasqueException):
    """Raised when error occurs during user creation or configuration."""


class DataMasqueApiError(DataMasqueException):
    """
    Raised when the DataMasque server responds to a request with a non-2xx status code.

    The triggering `Response` is always available on the `.response` attribute,
    so callers can inspect the status code, headers, and body for richer error handling.

    502 Bad Gateway responses are raised as `DataMasqueNotReadyError` instead.
    """

    def __init__(self, message: str, *, response: Response) -> None:
        super().__init__(message)
        self.response = response


class FailedToStartError(DataMasqueApiError):
    """
    Raised when `start_masking_run` fails to create the run.

    Inherits `.response` from `DataMasqueApiError`,
    so callers can read the server's status code and error body directly.
    """


class InvalidRulesetError(FailedToStartError):
    """Specific error for when runs fail to start due to having an invalid ruleset."""


class InvalidLibraryError(FailedToStartError):
    """Specific error for when runs fail to start due to having an invalid ruleset library."""


class DataMasqueTransportError(DataMasqueException):
    """
    Raised when a request to the DataMasque server fails before any response is received.

    Covers connection refused, timeout, DNS failure, SSL handshake failure,
    and similar transport-layer errors.
    The originating `requests` exception is chained via `__cause__`.
    """


class DataMasqueNotReadyError(DataMasqueException):
    """Raised when the DataMasque server is not healthy, normally because it is still starting up."""


class AsyncRulesetGenerationInProgressError(DataMasqueException):
    """Raised when attempting to retrieve results from a ruleset generation request that has not yet completed."""


class DataMasqueIfmError(DataMasqueException):
    """Generic base exception for IFM (in-flight masking) client errors."""


class IfmAuthError(DataMasqueIfmError):
    """Raised when the IFM client cannot obtain or refresh a JWT (e.g. invalid credentials, missing scope)."""


class RunNotCancellableError(DataMasqueUserError):
    """
    Raised when `cancel_run` is called against a run that is no longer eligible for cancellation.

    Typically this happens when the run is already finished, failed, or in the cancelling state itself.
    """
