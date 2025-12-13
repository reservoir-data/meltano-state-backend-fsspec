import shutil
from pathlib import Path

from upath.implementations.cloud import AzurePath, GCSPath, S3Path
from upath.implementations.local import LocalPath
from upath.implementations.sftp import SFTPPath
from meltano.core.project import Project
from meltano.core.state_store import state_store_manager_from_project_settings
from meltano_state_backend_fsspec import FSSpecStateStoreManager

import pytest


def test_manager() -> None:
    manager = FSSpecStateStoreManager(
        uri="fs:///tmp/test",
        protocol="file",
        storage_options={},
    )
    assert manager.path.protocol == "file"
    assert manager.path.as_uri() == "file:///tmp/test"


def test_s3_protocol() -> None:
    manager = FSSpecStateStoreManager(
        uri="fs://my-bucket/path/to/state",
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
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_PROTOCOL", "file")
    manager = state_store_manager_from_project_settings(project.settings)
    assert isinstance(manager, FSSpecStateStoreManager)
    assert isinstance(manager.path, LocalPath)
    assert manager.path.protocol == "file"

    uri = manager.path.as_uri()
    assert uri.startswith("file://")
    assert uri.endswith("/path/to/state")


def test_load_s3_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_PROTOCOL", "s3")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_S3_KEY",
        "my_key",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_S3_SECRET",
        "my_secret",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_S3_ENDPOINT_URL",
        "https://my-endpoint.com",
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
    }


def test_load_gcs_settings(project: Project, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_PROTOCOL", "gcs")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_GCS_PROJECT", "my-project"
    )
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_GCS_TOKEN", "my-token")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_GCS_ENDPOINT_URL",
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
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_PROTOCOL", "azure")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_AZURE_CONNECTION_STRING",
        "my-connection-string",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_AZURE_ACCOUNT_NAME",
        "my-account-name",
    )
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS_AZURE_ACCOUNT_KEY",
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
    monkeypatch.setenv("MELTANO_STATE_BACKEND_FS_PROTOCOL", "sftp")
    monkeypatch.setenv(
        "MELTANO_STATE_BACKEND_FS_STORAGE_OPTIONS",
        '{"sftp.foo": "bar", "sftp.baz": "qux"}',
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
