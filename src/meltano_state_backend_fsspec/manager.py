from __future__ import annotations

import contextlib
import io
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from time import sleep
from typing import TYPE_CHECKING, Any

from meltano.core.state_store import MeltanoState, StateStoreManager
from upath import UPath

if TYPE_CHECKING:
    from collections.abc import Iterable, Generator
    from upath.implementations.local import LocalPath
    from paramiko.pkey import PKey

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

logger = logging.getLogger(__name__)


def utc_now() -> float:
    return datetime.now(timezone.utc).timestamp()


def _guess_key_class(pkey: str, *, passphrase: str | None = None) -> PKey:
    from paramiko.ed25519key import Ed25519Key
    from paramiko.rsakey import RSAKey
    from paramiko.ecdsakey import ECDSAKey
    from paramiko.ssh_exception import SSHException

    # Try loading the key using paramiko's key classes
    # The order matters - try most common types first
    for key_class in (Ed25519Key, RSAKey, ECDSAKey):
        try:
            return key_class.from_private_key(  # type: ignore[no-any-return, no-untyped-call]
                io.StringIO(pkey),
                password=passphrase,
            )
        except SSHException:
            continue
    raise ValueError("SFTP private key is not in a valid format")


def _preprocess_storage_options(
    protocol: str,
    options: dict[str, Any | None],
) -> dict[str, Any]:
    """Preprocess the storage options."""
    if protocol == "sftp" and (pkey := options.get("pkey")):
        options["pkey"] = _guess_key_class(pkey, passphrase=options.get("passphrase"))

    return options


PROTOCOL_MAPPING: dict[str, str] = {
    "azure": "abfs",
}


class FSSpecStateStoreManager(StateStoreManager):
    def __init__(
        self,
        uri: str,
        protocol: str,
        lock_timeout_seconds: int = 60,
        lock_retry_seconds: int = 1,
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the FSSpecStateStoreManager.

        Args:
            uri: The URI of the filesystem to use.
            protocol: The protocol of the filesystem to use.
            lock_timeout_seconds: The timeout for the lock in seconds.
            lock_retry_seconds: The retry interval for the lock in seconds.
            storage_options: The storage options to use.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(**kwargs)
        self._path: UPath | LocalPath | None = None
        protocol = PROTOCOL_MAPPING.get(protocol, protocol)
        self._fsuri = uri.replace("fs://", f"{protocol}://")
        self.lock_timeout_seconds = lock_timeout_seconds
        self.lock_retry_seconds = lock_retry_seconds

        opts: dict[str, Any] = {}
        storage_options = storage_options or {}
        for key, value in storage_options.items():
            setting_prefix, setting_name = key.split(".")
            protocol_name = PROTOCOL_MAPPING.get(setting_prefix, setting_prefix)
            if protocol_name != protocol:
                continue
            opts[setting_name] = value

        self.storage_options = _preprocess_storage_options(protocol, opts)

    @property
    def path(self) -> UPath | LocalPath:
        if self._path is None:
            self._path = UPath.from_uri(self._fsuri, **self.storage_options)
        return self._path

    @override
    @property
    def label(self) -> str:
        return self.path.protocol

    def _get_lock_file(self, state_id: str) -> UPath | LocalPath:
        """Get the path to the lock file for the given state_id.

        Args:
            state_id: the state_id to get path for

        Returns:
            The path to the lock file for the given state_id.
        """
        return self.path.joinpath(state_id, "lock")

    def get_state_file(self, state_id: str) -> UPath | LocalPath:
        """Get the path to the file/blob storing complete state for the given state_id.

        Args:
            state_id: the state_id to get path for

        Returns:
            the path to the file/blob storing complete state for the given state_id.
        """
        return self.path.joinpath(state_id, "state.json")

    def is_locked(self, state_id: str) -> bool:
        """Indicate whether the given state_id is currently locked.

        Args:
            state_id: the state_id to check

        Returns:
            True if locked, else False

        Raises:
            Exception: if error not indicating file is not found is thrown
        """
        lock_path = self._get_lock_file(state_id)
        try:
            with lock_path.open() as reader:
                if utc_now() > (float(reader.read()) + self.lock_timeout_seconds):
                    with contextlib.suppress(FileNotFoundError, OSError):
                        # Use fs.rm_file() to avoid Content-MD5 issues with MinIO
                        lock_path.fs.rm_file(lock_path.path)  # type: ignore[no-untyped-call]
                    return False
                return True
        except FileNotFoundError:
            return False

    def mkdir(self, state_id: str) -> None:
        """Create the directory for the given state_id."""
        self.path.joinpath(state_id).mkdir(parents=True, exist_ok=True)

    @override
    def set(self, state: MeltanoState) -> None:
        """Set the state for the given state_id."""
        logger.info("Writing state to %s", self.label)
        self.mkdir(state.state_id)
        with self.get_state_file(state.state_id).open(
            "w",
            ContentType="application/json",
        ) as writer:
            writer.write(state.json())

    @override
    def get(self, state_id: str) -> MeltanoState | None:
        """Get the state for the given state_id."""
        logger.info("Reading state from %s", self.label)
        try:
            with self.get_state_file(state_id).open() as reader:
                return MeltanoState.from_file(state_id, reader)
        except FileNotFoundError:
            logger.info("No state found for %s.", state_id)
            return None

    @override
    def delete(self, state_id: str) -> None:
        """Delete the state for the given state_id."""
        state_dir = self.path.joinpath(state_id)
        if not state_dir.exists():
            return

        # Delete files individually to avoid Content-MD5 issues with MinIO's DeleteObjects
        for file_path in state_dir.iterdir():
            with contextlib.suppress(FileNotFoundError, OSError):
                file_path.fs.rm_file(file_path.path)  # type: ignore[no-untyped-call]

        # Remove the directory itself
        with contextlib.suppress(FileNotFoundError, OSError):
            state_dir.rmdir()

    @override
    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get the state ids for the given pattern."""
        if not self.path.exists():
            return []
        paths = self.path.glob(pattern) if pattern else self.path.iterdir()
        return [path.name for path in paths if path.joinpath("state.json").exists()]

    @override
    def clear_all(self) -> int:
        """Clear all state."""
        count = 0
        for state_id in list(self.get_state_ids()):
            self.delete(state_id)
            count += 1
        return count

    @override
    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: int,
    ) -> Generator[None, None, None]:
        """Acquire the lock for the given state_id."""
        lock_path = self._get_lock_file(state_id)
        try:
            self.mkdir(state_id)

            while self.is_locked(state_id):
                sleep(retry_seconds)
            with lock_path.open("w") as writer:
                writer.write(str(utc_now()))
            yield
        finally:
            try:
                # Use fs.rm_file() to avoid Content-MD5 issues with MinIO
                lock_path.fs.rm_file(lock_path.path)  # type: ignore[no-untyped-call]
            except (FileNotFoundError, OSError):
                # Lock file already deleted or permission error
                pass
