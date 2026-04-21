"""Typed response shape for the license endpoint."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class SwitchableLicenseMetadata(BaseModel):
    """Metadata for switchable license management (AWS Marketplace, etc.)."""

    model_config = ConfigDict(extra="allow")

    can_switch_license_source: Optional[bool] = None
    license_source: Optional[str] = None
    license_select_time: Optional[datetime] = None
    aws_account_number: Optional[str] = None
    last_checkout_success_time: Optional[datetime] = None
    last_checkout_success_type: Optional[str] = None
    last_checkout_error: Optional[str] = None
    last_checkout_license_arn: Optional[str] = None
    last_checkout_product_name: Optional[str] = None
    last_checkout_contract_expiry: Optional[datetime] = None
    last_checkout_agreement_id: Optional[str] = None
    last_checkout_agreement_url: Optional[str] = None
    checkout_mode: Optional[str] = None
    selected_product_sku: Optional[str] = None
    allow_fallback: Optional[bool] = None
    last_checkout_success_license_count: Optional[int] = None
    iam_role_arn: Optional[str] = None


class LicenseInfo(BaseModel):
    """
    License information returned by `GET /api/license/`.

    Core fields (`uuid`, `name`, `type`, `is_expired`, `uploadable`)
    are always present in the server response.
    Other fields vary by license type and server version.
    """

    model_config = ConfigDict(extra="allow")

    uuid: str
    name: str
    type: str
    is_expired: bool
    uploadable: bool
    version: Optional[str] = None
    raw_type: Optional[str] = None
    expiry_date: Optional[datetime] = None
    quota_tb: Optional[float] = None
    maximum_node_count: Optional[int] = None
    row_limit: Optional[int] = None
    platform_name: Optional[str] = None
    platform_code: Optional[str] = None
    days_until_expiry: Optional[int] = None
    is_contract_product: Optional[bool] = None
    contract_license_type: Optional[str] = None
    switchable_license_metadata: Optional[SwitchableLicenseMetadata] = None
