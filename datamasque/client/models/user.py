import secrets
import string
from enum import Enum
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, Field

UserId = NewType("UserId", int)

GENERATED_PASSWORD_LENGTH = 16


class UserRole(Enum):
    """
    List of supported user roles.

    `ruleset_library_manager` can be optionally included alongside `mask_builder`.
    It is not valid as a standalone role.
    """

    superuser = "admin"
    mask_builder = "mask_builder"
    ruleset_library_manager = "ruleset_library_managers"
    mask_runner = "mask_runner"


class User(BaseModel):
    """Represents a DataMasque user account."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    username: str
    email: str
    roles: list[UserRole] = Field(alias="user_roles")
    id: Optional[UserId] = None
    password: Optional[str] = Field(default=None, exclude=True)

    @staticmethod
    def generate_password() -> str:
        """
        Generates a password suitable for DataMasque authentication.

        The password consists of 16 characters
        without the same character occurring three times in a row
        and without any three consecutive characters forming an increasing or decreasing sequence.
        """

        def is_sequential(s: str) -> bool:
            """Check if the last three characters are in an increasing or decreasing sequence."""

            if len(s) < 3:
                return False
            return (ord(s[-1]) == ord(s[-2]) + 1 == ord(s[-3]) + 2) or (ord(s[-1]) == ord(s[-2]) - 1 == ord(s[-3]) - 2)

        chars = string.ascii_letters + string.digits
        result = secrets.choice(chars)

        while len(result) < GENERATED_PASSWORD_LENGTH:
            next_char = secrets.choice(chars)
            if len(result) >= 2 and next_char == result[-1] == result[-2]:
                continue
            if is_sequential(result + next_char):
                continue
            result += next_char

        return result

    def __str__(self) -> str:
        return self.username
