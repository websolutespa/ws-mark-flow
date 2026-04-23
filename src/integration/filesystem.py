"""
Local filesystem integration (source + destination).
"""
import asyncio
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

from pydantic import BaseModel, Field

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class FilesystemConfig(BaseModel):
    """Configuration for local filesystem integration."""
    path: str = Field(description="Base directory path on local filesystem")

    class Config:
        extra = "ignore"


class _FilesystemBase:
    """Shared logic for filesystem source and destination."""

    def __init__(self, config: dict[str, Any]):
        self._config = FilesystemConfig.model_validate(config)
        self._base_path: Optional[Path] = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.LOCAL

    async def disconnect(self) -> None:
        self._base_path = None

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None
    ) -> list[FileInfo]:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")

        base = self._base_path
        if folder_path:
            base = self._base_path / folder_path.lstrip('/')

        if not base.exists():
            return []

        files: list[FileInfo] = []
        await asyncio.to_thread(self._list_recursive, base, files, extensions)
        return files

    def _list_recursive(
        self,
        current_path: Path,
        files: list[FileInfo],
        extensions: Optional[list[str]]
    ) -> None:
        try:
            for entry in current_path.iterdir():
                if entry.is_file():
                    if extensions:
                        ext = entry.suffix.lower()
                        if ext not in [e.lower() for e in extensions]:
                            continue

                    stat = entry.stat()
                    relative_path = "/" + str(entry.relative_to(self._base_path))

                    files.append(FileInfo(
                        name=entry.name,
                        path=relative_path,
                        modified_at=datetime.fromtimestamp(stat.st_mtime),
                        size=stat.st_size,
                    ))
                elif entry.is_dir():
                    if not entry.name.startswith('.'):
                        self._list_recursive(entry, files, extensions)
        except PermissionError:
            logger.warning(f"Permission denied: {current_path}")
        except Exception as e:
            logger.error(f"Error listing {current_path}: {e}")


class FilesystemSource(_FilesystemBase, SourceIntegration):
    """Local filesystem source integration for reading files from disk."""

    async def connect(self) -> bool:
        try:
            self._base_path = Path(self._config.path).resolve()
            if not self._base_path.exists():
                logger.error(f"Path does not exist: {self._base_path}")
                return False
            if not self._base_path.is_dir():
                logger.error(f"Path is not a directory: {self._base_path}")
                return False
            logger.info(f"Connected to filesystem source: {self._base_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to filesystem: {e}")
            return False

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            source_path = self._base_path / file_info.path.lstrip('/')

            if not source_path.exists():
                logger.error(f"Source file not found: {source_path}")
                return False

            local_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copy2, source_path, local_path)

            logger.debug(f"Copied file: {source_path} -> {local_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to copy file {file_info.path}: {e}")
            return False


class FilesystemDestination(_FilesystemBase, DestinationIntegration):
    """Local filesystem destination integration for writing files to disk."""

    async def connect(self) -> bool:
        try:
            self._base_path = Path(self._config.path).resolve()

            if not self._base_path.exists():
                self._base_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created destination directory: {self._base_path}")

            if not self._base_path.is_dir():
                logger.error(f"Path is not a directory: {self._base_path}")
                return False

            logger.info(f"Connected to filesystem destination: {self._base_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to filesystem destination: {e}")
            return False

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            dest_path = self._base_path / remote_path.lstrip('/')
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copy2, local_path, dest_path)

            logger.debug(f"Uploaded file: {local_path} -> {dest_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload file to {remote_path}: {e}")
            return False

    async def file_exists(self, remote_path: str) -> bool:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")

        dest_path = self._base_path / remote_path.lstrip('/')
        return dest_path.exists()

    async def delete_file(self, remote_path: str) -> bool:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            dest_path = self._base_path / remote_path.lstrip('/')
            if dest_path.exists():
                dest_path.unlink()
                logger.debug(f"Deleted file: {dest_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file {remote_path}: {e}")
            return False

    async def create_folder(self, folder_path: str) -> bool:
        if not self._base_path:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            new_folder = self._base_path / folder_path.lstrip('/')
            new_folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created folder: {new_folder}")
            return True
        except Exception as e:
            logger.error(f"Failed to create folder {folder_path}: {e}")
            return False
