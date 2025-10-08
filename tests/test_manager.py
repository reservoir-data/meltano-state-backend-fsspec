import shutil
import unittest.mock
from pathlib import Path

from upath.implementations.cloud import AzurePath, GCSPath, S3Path
from upath.implementations.local import LocalPath
from upath.implementations.sftp import SFTPPath
from meltano.core.project import Project
from meltano.core.state_store import (
    MeltanoState,
    state_store_manager_from_project_settings,
)
from meltano_state_backend_fsspec import FSSpecStateStoreManager

import pytest
import time_machine


def test_manager() -> None:
    manager = FSSpecStateStoreManager(
        uri="fsspec:///tmp/test",
        protocol="file",
        storage_options={},
    )
    assert manager.path.protocol == "file"
    assert manager.path.as_uri() == "file:///tmp/test"


def test_s3_protocol() -> None:
    manager = FSSpecStateStoreManager(
        uri="fsspec://my-bucket/path/to/state",
        protocol="s3",
        storage_options={},
    )
    assert manager.path.protocol == "s3"
    assert manager.path.as_uri() == "s3://my-bucket/path/to/state"


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve())  # type: ignore[no-any-return]


def test_load_local_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_PROTOCOL", "file")
    manager = state_store_manager_from_project_settings(project.settings)
    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, LocalPath)
    assert manager.path.protocol == "file"

    uri = manager.path.as_uri()
    assert uri.startswith("file://")
    assert uri.endswith("/path/to/state")


def test_load_s3_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_PROTOCOL", "s3")
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_S3_KEY", "my_key")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_S3_SECRET", "my_secret"
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_S3_ENDPOINT_URL",
        "https://my-endpoint.com",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_S3_REGION", "us-east-1"
    )
    manager = state_store_manager_from_project_settings(project.settings)

    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, S3Path)
    assert manager.path.protocol == "s3"
    assert manager.path.as_uri() == "s3://path/to/state"
    assert manager.path.storage_options == {
        "key": "my_key",
        "secret": "my_secret",
        "endpoint_url": "https://my-endpoint.com",
        "region": "us-east-1",
    }


def test_load_gcs_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_PROTOCOL", "gcs")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_GCS_PROJECT", "my-project"
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_GCS_TOKEN", "my-token"
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_GCS_ENDPOINT_URL",
        "https://my-endpoint.com",
    )
    manager = state_store_manager_from_project_settings(project.settings)

    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, GCSPath)
    assert manager.path.protocol == "gcs"
    assert manager.path.as_uri() == "gcs://path/to/state"
    assert manager.path.storage_options == {
        "project": "my-project",
        "token": "my-token",
        "endpoint_url": "https://my-endpoint.com",
    }


def test_load_azure_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_PROTOCOL", "azure")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_AZURE_CONNECTION_STRING",
        "my-connection-string",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_AZURE_ACCOUNT_NAME",
        "my-account-name",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS_AZURE_ACCOUNT_KEY",
        "my-account-key",
    )
    manager = state_store_manager_from_project_settings(project.settings)

    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, AzurePath)
    assert manager.path.protocol == "abfs"
    assert manager.path.as_uri() == "abfs://path/to/state"
    assert manager.path.storage_options == {
        "connection_string": "my-connection-string",
        "account_name": "my-account-name",
        "account_key": "my-account-key",
    }


def test_load_arbitrary_settings(
    project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FSSPEC_PROTOCOL", "sftp")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FSSPEC_STORAGE_OPTIONS", '{"sftp.foo": "bar", "sftp.baz": "qux"}'
    )
    manager = state_store_manager_from_project_settings(project.settings)
    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, SFTPPath)
    assert manager.path.protocol == "sftp"
    assert manager.path.as_uri() == "sftp:///to/state"
    assert manager.path.storage_options == {
        "host": "path",
        "foo": "bar",
        "baz": "qux",
    }


@pytest.fixture
def manager(tmp_path: Path) -> FSSpecStateStoreManager:
    return FSSpecStateStoreManager(
        uri=f"fsspec://{tmp_path}",
        protocol="file",
        storage_options={},
    )


def test_set_state(manager: FSSpecStateStoreManager) -> None:
    """Test setting state."""
    # Test setting new state
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
    with (
        time_machine.travel("2025-01-01 00:00:00", tick=False) as traveller,
        manager.acquire_lock(state_id, retry_seconds=5),
    ):
        assert manager.is_locked(state_id)

        traveller.shift(80)
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
