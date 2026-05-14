from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

DiscoveryConfigId = NewType("DiscoveryConfigId", str)


class DiscoveryConfig(BaseModel):
    """Represents a named, persisted YAML discovery configuration."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    yaml: Optional[str] = Field(default=None, alias="config_yaml")
    id: Optional[DiscoveryConfigId] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
