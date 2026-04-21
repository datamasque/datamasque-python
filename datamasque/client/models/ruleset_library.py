from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

from datamasque.client.models.status import ValidationStatus

RulesetLibraryId = NewType("RulesetLibraryId", str)


class RulesetLibrary(BaseModel):
    """Represents a ruleset library."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    namespace: str = ""
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    id: Optional[RulesetLibraryId] = None
    is_valid: Optional[ValidationStatus] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
