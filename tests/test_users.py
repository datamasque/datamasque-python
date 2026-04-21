"""Tests for `UserClient` (CRUD + password reset + password generation helper)."""

import string
from unittest.mock import patch

import pytest
import requests_mock

from datamasque.client.exceptions import DataMasqueUserError
from datamasque.client.models.user import GENERATED_PASSWORD_LENGTH, User, UserId, UserRole
from tests.helpers import fake


def test_generate_password_properties():
    """
    Generated passwords satisfy every documented constraint.

    The password is the right length, uses only `string.ascii_letters + string.digits`,
    never contains the same character three times in a row,
    and never contains three consecutive characters that form an increasing or decreasing arithmetic run
    (e.g. `abc`, `cba`, `123`, `321`).
    """
    password = User.generate_password()

    assert len(password) == GENERATED_PASSWORD_LENGTH

    allowed_chars = set(string.ascii_letters + string.digits)
    assert set(password) <= allowed_chars, f'unexpected chars in "{password}"'

    for i in range(2, len(password)):
        a, b, c = ord(password[i - 2]), ord(password[i - 1]), ord(password[i])
        assert not (a == b == c), f'triple repeat at index {i} in "{password}"'
        assert not (c == b + 1 == a + 2), f'ascending run at index {i} in "{password}"'
        assert not (c == b - 1 == a - 2), f'descending run at index {i} in "{password}"'


def test_generate_password_rejects_triples_and_sequential_runs():
    """
    Drive `secrets.choice` with a fixed sequence that forces every rejection path.

    Verifies the generator:
    - uses `secrets.choice` (not `random.choice`);
    - allows the pair `aa` but rejects the triple `aaa`;
    - rejects a character that would complete an ascending run of 3;
    - rejects a character that would complete a descending run of 3;
    - keeps consuming the stream until the result is `GENERATED_PASSWORD_LENGTH` chars.
    """
    mock_sequence = iter(
        [
            # Triple repeat: third `a` gets skipped.
            "a",
            "a",
            "a",
            "m",
            # Ascending run: `d` gets skipped after `bc`.
            "b",
            "c",
            "d",
            "p",
            # Descending run: `x` gets skipped after `zy`.
            "z",
            "y",
            "x",
            "q",
            "r",
            # Remaining 6 benign chars,
            # chosen so no three consecutive form a run or a triple.
            "A",
            "b",
            "C",
            "d",
            "E",
            "f",
        ]
    )

    with patch(
        "datamasque.client.models.user.secrets.choice",
        side_effect=lambda _chars: next(mock_sequence),
    ) as mock_choice:
        password = User.generate_password()

    # 19 mock inputs - 3 rejections (one per bad pattern) = 16 accepted chars.
    assert password == "aambcpzyqrAbCdEf"
    assert mock_choice.call_count == 19


def test_user_create(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/users/",
            json={"id": 1, "email": fake.email(), "username": "builder", "user_roles": ["mask_builder"]},
            status_code=201,
        )

        user = client.create_or_update_user(User(email=fake.email(), username="builder", roles=[UserRole.mask_builder]))
        assert user.id == 1

    assert m.call_count == 1
    assert m.request_history[0].method == "POST"
    actual_request_data = m.request_history[0].json()
    expected_request_data = {
        "username": "builder",
        "password": user.password,
        "re_password": user.password,
        "user_roles": ["mask_builder"],
    }
    for key, value in expected_request_data.items():
        assert actual_request_data[key] == value


def test_user_create_with_password(client):
    new_password = "better_p@ssw0rd!"
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/users/",
            json={"id": 1, "email": fake.email(), "username": "builder", "user_roles": ["mask_builder"]},
            status_code=201,
        )
        m.patch("http://test-server/api/users/1/", status_code=200)

        user = client.create_or_update_user(
            User(email=fake.email(), username="builder", roles=[UserRole.mask_builder]),
            new_password=new_password,
        )
        assert user.id == 1
        assert user.password == new_password

    assert m.call_count == 2
    assert m.request_history[0].method == "POST"
    assert m.request_history[1].method == "PATCH"
    actual_request_data = m.request_history[1].json()
    expected_request_data = {
        "new_password": new_password,
        "re_new_password": new_password,
    }
    assert len(actual_request_data["current_password"]) == 16  # initial random password
    for key, value in expected_request_data.items():
        assert actual_request_data[key] == value


def test_user_update(client):
    with requests_mock.Mocker() as m:
        m.patch(
            "http://test-server/api/users/1/",
            json={
                "id": 1,
                "email": fake.email(),
                "username": "builder",
                "password": "temp_password1",
            },
            status_code=200,
        )

        user = User(email=fake.email(), username="builder", roles=[UserRole.mask_builder])
        user.id = 1
        user.password = "shouldn't be changed"
        modified_user = client.create_or_update_user(user)
        assert modified_user.id == 1
        assert modified_user.password == "shouldn't be changed"


def test_user_update_with_password(client):
    old_password = "old_password"
    new_password = "better_p@ssw0rd!"
    with requests_mock.Mocker() as m:
        m.patch(
            "http://test-server/api/users/1/",
            json={"id": 1, "email": fake.email(), "username": "builder"},
            status_code=201,
        )

        user = User(email=fake.email(), username="builder", roles=[UserRole.mask_builder])
        user.id = 1
        user.password = old_password
        modified_user = client.create_or_update_user(
            user,
            new_password=new_password,
        )
        assert modified_user.id == 1
        assert modified_user.password == new_password

    assert m.call_count == 2
    assert m.request_history[0].method == "PATCH"
    assert m.request_history[1].method == "PATCH"
    assert m.request_history[1].json() == {
        "current_password": old_password,
        "new_password": new_password,
        "re_new_password": new_password,
    }


def test_user_creation_must_specify_at_least_one_role(client):
    user = User(email=fake.email(), username="builder", roles=[])
    with pytest.raises(DataMasqueUserError, match=r'User must have at least one role'):
        client.create_or_update_user(user)


def test_user_create_superuser(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/users/",
            json={"id": 1, "email": fake.email(), "username": "admin2", "user_roles": ["admin"]},
            status_code=201,
        )

        user = client.create_or_update_user(User(email=fake.email(), username="admin2", roles=[UserRole.superuser]))
        assert user.id == 1

    assert m.call_count == 1
    assert m.request_history[0].method == "POST"
    actual_request_data = m.request_history[0].json()
    assert actual_request_data["user_roles"] == ["admin"]


def test_user_create_with_ruleset_library_manager(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/users/",
            json={
                "id": 1,
                "email": fake.email(),
                "username": "lib_builder",
                "user_roles": ["mask_builder", "ruleset_library_managers"],
            },
            status_code=201,
        )

        user = client.create_or_update_user(
            User(
                email=fake.email(),
                username="lib_builder",
                roles=[UserRole.mask_builder, UserRole.ruleset_library_manager],
            )
        )
        assert user.id == 1

    assert m.call_count == 1
    actual_request_data = m.request_history[0].json()
    assert actual_request_data["user_roles"] == ["mask_builder", "ruleset_library_managers"]


def test_user_create_ruleset_library_manager_without_mask_builder_fails(client):
    user = User(
        email=fake.email(),
        username="lib_only",
        roles=[UserRole.ruleset_library_manager],
    )
    with pytest.raises(DataMasqueUserError, match=r"ruleset_library_manager.*requires.*mask_builder"):
        client.create_or_update_user(user)


def test_user_reset_password(client):
    temp_password = "temp_password1"
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/users/1/reset-password/",
            json={"password": temp_password},
            status_code=200,
        )
        user = User(email=fake.email(), username="builder", roles=[UserRole.mask_builder])
        user.id = 1
        password = client.reset_password_for_user(user)
        assert password == temp_password
        assert user.password == temp_password


def test_uncreated_user_cannot_reset_password(client):
    user = User(email=fake.email(), username="builder", roles=[UserRole.mask_builder])
    with pytest.raises(DataMasqueUserError):
        # id is not set, so this fails
        client.reset_password_for_user(user)


def test_list_users(client):
    fake_emails = [fake.email() for _ in range(5)]
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/users/",
            json=[
                {
                    "id": 1,
                    "username": "admin",
                    "is_active": True,
                    "email": fake_emails[0],
                    "user_roles": ["admin"],
                },
                {
                    "id": 2,
                    "username": "builder",
                    "is_active": True,
                    "email": fake_emails[1],
                    "user_roles": ["mask_builder"],
                },
                {
                    "id": 3,
                    "username": "runner",
                    "is_active": True,
                    "email": fake_emails[2],
                    "user_roles": ["mask_runner"],
                },
                {
                    "id": 4,
                    "username": "disabled",
                    "is_active": False,
                    "email": fake_emails[3],
                    "user_roles": ["mask_builder"],
                },
                {
                    "id": 5,
                    "username": "no_role",
                    "is_active": True,
                    "email": fake_emails[4],
                    "user_roles": [],
                },
            ],
            status_code=200,
        )

        users = client.list_users()
        # 4 active users returned (inactive user id=4 excluded)
        assert len(users) == 4
        assert users[0].username == "admin"
        assert users[0].id == 1
        assert users[0].roles == [UserRole.superuser]
        assert users[1].username == "builder"
        assert users[1].id == 2
        assert users[2].username == "runner"
        assert users[2].id == 3
        assert users[3].username == "no_role"
        assert users[3].id == 5


def test_delete_user_by_id(client):
    with requests_mock.Mocker() as m:
        m.delete("http://test-server/api/users/1/", status_code=204)
        client.delete_user_by_id_if_exists(UserId(1))

    assert m.call_count == 1
    assert m.request_history[0].method == "DELETE"


def test_delete_user_by_id_not_found(client):
    with requests_mock.Mocker() as m:
        m.delete("http://test-server/api/users/99/", status_code=404)
        client.delete_user_by_id_if_exists(UserId(99))

    assert m.call_count == 1


def test_delete_user_by_username(client):
    fake_email = fake.email()
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/users/",
            json=[
                {
                    "id": 1,
                    "username": "admin",
                    "is_active": True,
                    "email": fake_email,
                    "user_roles": ["admin"],
                },
                {
                    "id": 2,
                    "username": "target",
                    "is_active": True,
                    "email": fake_email,
                    "user_roles": ["mask_builder"],
                },
            ],
        )
        m.delete("http://test-server/api/users/2/", status_code=204)
        client.delete_user_by_username_if_exists("target")

    assert m.call_count == 2
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"


def test_delete_user_by_username_not_found(client):
    fake_email = fake.email()
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/users/",
            json=[
                {
                    "id": 1,
                    "username": "admin",
                    "is_active": True,
                    "email": fake_email,
                    "user_roles": ["admin"],
                },
            ],
        )
        client.delete_user_by_username_if_exists("nonexistent")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"
