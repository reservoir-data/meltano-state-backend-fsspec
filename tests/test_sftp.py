from __future__ import annotations

import unittest.mock
import uuid
from typing import Any, TYPE_CHECKING

import pytest
from testcontainers.sftp import SFTPContainer
from meltano.core.state_store import MeltanoState

from meltano_state_backend_fsspec import FSSpecStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(scope="module")
def sftp() -> Generator[SFTPContainer, None, None]:
    with SFTPContainer() as sftp:
        yield sftp


@pytest.fixture(params=["basic", "keypair", "keyfile"])
def manager(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    sftp: SFTPContainer,
) -> FSSpecStateStoreManager:
    prefix = str(uuid.uuid4())
    storage_options: dict[str, Any] = {
        "sftp.port": str(sftp.get_exposed_port(22)),
        "sftp.host": sftp.get_container_host_ip(),
    }
    match request.param:
        case "basic":
            storage_options["sftp.username"] = sftp.users[0].name
            storage_options["sftp.password"] = sftp.users[0].password
        case "keypair":
            storage_options["sftp.username"] = sftp.users[1].name
            storage_options["sftp.pkey"] = sftp.users[1].private_key.decode("utf-8")  # type: ignore[union-attr] # ty: ignore[possibly-missing-attribute]
        case "keyfile":
            filepath = tmp_path / f"keyfile-{uuid.uuid4()}.pem"
            filepath.write_text(sftp.users[1].private_key.decode("utf-8"))  # type: ignore[union-attr] # ty: ignore[possibly-missing-attribute]
            storage_options["sftp.username"] = sftp.users[1].name
            storage_options["sftp.key_filename"] = filepath.as_posix()
        case _:  # pragma: no cover
            ...

    return FSSpecStateStoreManager(
        uri=f"fs:///upload/{prefix}",
        protocol="sftp",
        storage_options=storage_options,
    )


def test_unsupported_key_format() -> None:
    storage_options = {"sftp.host": "localhost", "sftp.pkey": "invalid key"}
    with pytest.raises(ValueError, match="SFTP private key is not in a valid format"):
        FSSpecStateStoreManager(
            uri="fs:///upload/test",
            protocol="sftp",
            storage_options=storage_options,
        )


def test_set_state(manager: FSSpecStateStoreManager) -> None:
    """Test setting state."""
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)
    assert manager.get_state_file(state.state_id).read_text() == state.json()


def test_get_state(manager: FSSpecStateStoreManager) -> None:
    """Test getting state."""
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    assert manager.get(state.state_id) is None

    manager.mkdir(state.state_id)
    manager.get_state_file(state.state_id).write_text(state.json())
    assert manager.get(state.state_id) == state


def test_delete_state(manager: FSSpecStateStoreManager) -> None:
    """Test deleting state."""
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.delete(state.state_id)

    state_file = manager.get_state_file(state.state_id)
    manager.set(state)
    assert state_file.exists()

    manager.delete(state.state_id)
    assert manager.get(state.state_id) is None
    assert not state_file.exists()


def test_get_state_ids(manager: FSSpecStateStoreManager) -> None:
    """Test getting state ids."""
    state1 = MeltanoState(
        state_id="test_job1",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    state2 = MeltanoState(
        state_id="test_job2",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state1)
    manager.set(state2)
    assert manager.get_state_ids() == ["test_job1", "test_job2"]


def test_get_state_ids_with_pattern(manager: FSSpecStateStoreManager) -> None:
    """Test getting state ids with pattern."""
    state11 = MeltanoState(
        state_id="test_job11",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    state12 = MeltanoState(
        state_id="test_job12",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    state21 = MeltanoState(
        state_id="test_job21",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state11)
    manager.set(state12)
    manager.set(state21)
    assert manager.get_state_ids(pattern="test_job1*") == ["test_job11", "test_job12"]
    assert manager.get_state_ids(pattern="test_job2*") == ["test_job21"]


def test_clear_all(manager: FSSpecStateStoreManager) -> None:
    state1 = MeltanoState(
        state_id="test_job1",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    state2 = MeltanoState(
        state_id="test_job2",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state1)
    manager.set(state2)
    assert manager.get_state_ids() == ["test_job1", "test_job2"]

    manager.clear_all()
    assert manager.get_state_ids() == []


def test_acquire_lock(manager: FSSpecStateStoreManager) -> None:
    """Test acquiring lock."""
    state_id = "test_job"

    mock_time = 1000.0

    def mock_utc_now() -> float:
        return mock_time

    with (
        unittest.mock.patch(
            "meltano_state_backend_fsspec.manager.utc_now", side_effect=mock_utc_now
        ),
        manager.acquire_lock(state_id, retry_seconds=5),
    ):
        assert manager.is_locked(state_id)

        # Shift time forward by 80 seconds (beyond 60 second timeout)
        mock_time_after = 1080.0
        with unittest.mock.patch(
            "meltano_state_backend_fsspec.manager.utc_now", return_value=mock_time_after
        ):
            assert not manager.is_locked(state_id)

    assert not manager.is_locked(state_id)


def test_acquire_lock_retry(manager: FSSpecStateStoreManager) -> None:
    """Test that lock acquisition retries when lock is held."""
    state_id = "test_job"
    retry_seconds = 10

    # Mock is_locked to return True for first 5 calls, then False
    call_count = 0
    original_is_locked = manager.is_locked

    def mock_is_locked(state_id: str) -> bool:
        nonlocal call_count
        call_count += 1
        if call_count <= 5:
            return True
        return original_is_locked(state_id)

    with (
        unittest.mock.patch.object(manager, "is_locked", side_effect=mock_is_locked),
        unittest.mock.patch("meltano_state_backend_fsspec.manager.sleep") as mock_sleep,
    ):
        with manager.acquire_lock(state_id, retry_seconds=retry_seconds):
            pass

    # Verify sleep was called 5 times (once for each time is_locked returned True)
    assert mock_sleep.call_count == 5
    assert all(call[0][0] == retry_seconds for call in mock_sleep.call_args_list)
