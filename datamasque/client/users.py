from typing import Optional

from datamasque.client.base import BaseClient
from datamasque.client.exceptions import DataMasqueException, DataMasqueUserError
from datamasque.client.models.user import User, UserId, UserRole


class UserClient(BaseClient):
    """User CRUD API methods. Mixed into `DataMasqueClient`."""

    def list_users(self) -> list[User]:
        """Returns all active users configured on the server."""

        users = []
        for user_data in self.make_request("GET", "/api/users/").json():
            if user_data.get("is_active", False):
                users.append(User.model_validate(user_data))

        return users

    def create_or_update_user(self, user: User, new_password: Optional[str] = None) -> User:
        """
        Creates or updates the user.

        An update will be performed if `user.id` is set, otherwise a create.
        To also set the user's password,
        put the old password in the user's `password` field (for an existing user)
        and pass the new password in the `new_password` parameter.
        Returns the same User object but with the id and password fields populated.
        """

        if not user.roles:
            raise DataMasqueUserError("User must have at least one role")
        if UserRole.ruleset_library_manager in user.roles and UserRole.mask_builder not in user.roles:
            raise DataMasqueUserError("`ruleset_library_manager` role requires `mask_builder` role")

        if user.id is None:
            temp_password = User.generate_password()

            data = user.model_dump(exclude_none=True, by_alias=True, mode="json") | {
                "password": temp_password,
                "re_password": temp_password,
            }
            resp = self.make_request("POST", "/api/users/", data=data).json()
            user.id = resp["id"]
            user.password = temp_password
        else:
            self.make_request(
                "PATCH",
                f"/api/users/{user.id}/",
                data=user.model_dump(exclude_none=True, by_alias=True, mode="json"),
            ).json()

        if new_password:
            self.make_request(
                "PATCH",
                f"/api/users/{user.id}/",
                data={
                    "current_password": user.password,
                    "new_password": new_password,
                    "re_new_password": new_password,
                },
            )
            user.password = new_password

        return user

    def reset_password_for_user(self, user: User) -> str:
        """
        Resets the user's password.

        The temporary password is stored on the User object and also returned.
        """

        if user.id is None:
            raise DataMasqueUserError("User must be created first")

        resp = self.make_request("POST", f"/api/users/{user.id}/reset-password/").json()
        user.password = resp["password"]
        return user.password

    def delete_user_by_id_if_exists(self, user_id: UserId) -> None:
        """Deletes the user with the given ID. No-op if the user does not exist."""

        self._delete_if_exists(f"/api/users/{user_id}/")

    def delete_user_by_username_if_exists(self, username: str) -> None:
        """Deletes the user with the given username. No-op if the user does not exist."""

        all_users = self.list_users()
        users_matching_username = [u for u in all_users if u.username == username]
        for user in users_matching_username:
            if user.id is None:
                raise DataMasqueException(f'Server returned a user named "{user.username}" without an `id`.')

            self.delete_user_by_id_if_exists(user.id)
