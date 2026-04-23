"""
SFTP integration (source + destination) using Paramiko.
"""
import asyncio
import logging
import stat as stat_module
from datetime import datetime
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Optional, Any

import paramiko
from pydantic import BaseModel, Field, AliasChoices

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class SFTPConfig(BaseModel):
    """Configuration for SFTP integration."""
    host: str = Field(description="SFTP server hostname or IP")
    port: int = Field(default=22, description="SFTP port")
    username: str = Field(description="SFTP username")
    password: Optional[str] = Field(default=None, description="SFTP password")
    private_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("privateKey", "private_key"),
        description="Private key content or path"
    )
    private_key_passphrase: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("privateKeyPassphrase", "private_key_passphrase"),
        description="Passphrase for private key"
    )
    base_path: Optional[str] = Field(
        default="/",
        validation_alias=AliasChoices("basePath", "base_path"),
        description="Base directory path on SFTP server"
    )

    class Config:
        extra = "ignore"


class _SFTPBase:
    """Shared logic for SFTP source and destination."""

    def __init__(self, config: dict[str, Any]):
        self._config = SFTPConfig.model_validate(config)
        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.SFTP

    async def connect(self) -> bool:
        try:
            return await asyncio.to_thread(self._connect_sync)
        except Exception as e:
            logger.error(f"Failed to connect to SFTP: {e}")
            return False

    def _connect_sync(self) -> bool:
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict[str, Any] = {
            "hostname": self._config.host,
            "port": self._config.port,
            "username": self._config.username,
        }

        if self._config.password:
            connect_kwargs["password"] = self._config.password

        if self._config.private_key:
            key_content = self._config.private_key
            if key_content.startswith("-----"):
                pkey = paramiko.RSAKey.from_private_key(
                    StringIO(key_content),
                    password=self._config.private_key_passphrase
                )
            else:
                pkey = paramiko.RSAKey.from_private_key_file(
                    key_content,
                    password=self._config.private_key_passphrase
                )
            connect_kwargs["pkey"] = pkey

        self._ssh_client.connect(**connect_kwargs)
        self._sftp = self._ssh_client.open_sftp()

        logger.info(f"Connected to SFTP: {self._config.host}")
        return True

    async def disconnect(self) -> None:
        await asyncio.to_thread(self._disconnect_sync)

    def _disconnect_sync(self) -> None:
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None
    ) -> list[FileInfo]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        base = folder_path or self._config.base_path or "/"
        files: list[FileInfo] = []

        await asyncio.to_thread(
            self._list_recursive, base, "", files, extensions
        )

        return files

    def _list_recursive(
        self,
        base_path: str,
        relative_path: str,
        files: list[FileInfo],
        extensions: Optional[list[str]]
    ) -> None:
        current_path = str(PurePosixPath(base_path) / relative_path) if relative_path else base_path

        try:
            items = self._sftp.listdir_attr(current_path)
        except IOError as e:
            logger.warning(f"Cannot list directory {current_path}: {e}")
            return

        for item in items:
            item_relative = str(PurePosixPath(relative_path) / item.filename) if relative_path else item.filename

            if stat_module.S_ISDIR(item.st_mode):
                self._list_recursive(base_path, item_relative, files, extensions)
            elif stat_module.S_ISREG(item.st_mode):
                file_ext = Path(item.filename).suffix.lower()
                if extensions is None or file_ext in [e.lower() for e in extensions]:
                    try:
                        mtime = datetime.fromtimestamp(item.st_mtime)
                    except Exception:
                        mtime = datetime.utcnow()

                    files.append(FileInfo(
                        name=item.filename,
                        path=f"/{item_relative}",
                        modified_at=mtime,
                        size=item.st_size,
                        content_type=None
                    ))

    def _mkdir_p(self, remote_path: str) -> None:
        if not remote_path or remote_path == "/":
            return

        parts = PurePosixPath(remote_path).parts
        current = ""

        for part in parts:
            if not part:
                continue
            current = f"{current}/{part}"

            try:
                self._sftp.stat(current)
            except IOError:
                try:
                    self._sftp.mkdir(current)
                except IOError:
                    pass


class SFTPSource(_SFTPBase, SourceIntegration):
    """SFTP source integration for reading files via SSH/SFTP."""

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            return await asyncio.to_thread(
                self._download_file_sync, file_info, local_path
            )
        except Exception as e:
            logger.error(f"Download error for {file_info.path}: {e}")
            return False

    def _download_file_sync(self, file_info: FileInfo, local_path: Path) -> bool:
        base = self._config.base_path or ""
        remote_path = str(PurePosixPath(base) / file_info.path.lstrip("/"))

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._sftp.get(remote_path, str(local_path))

        return True


class SFTPDestination(_SFTPBase, DestinationIntegration):
    """SFTP destination integration for uploading files via SSH/SFTP."""

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            return await asyncio.to_thread(
                self._upload_file_sync, local_path, remote_path
            )
        except Exception as e:
            logger.error(f"Upload error for {remote_path}: {e}")
            return False

    def _upload_file_sync(self, local_path: Path, remote_path: str) -> bool:
        base = self._config.base_path or ""
        full_remote_path = str(PurePosixPath(base) / remote_path.lstrip("/"))

        parent = str(PurePosixPath(full_remote_path).parent)
        self._mkdir_p(parent)

        self._sftp.put(str(local_path), full_remote_path)
        return True

    async def create_folder(self, folder_path: str) -> bool:
        try:
            return await asyncio.to_thread(
                self._create_folder_sync, folder_path
            )
        except Exception as e:
            logger.error(f"Failed to create folder {folder_path}: {e}")
            return False

    def _create_folder_sync(self, folder_path: str) -> bool:
        base = self._config.base_path or ""
        full_path = str(PurePosixPath(base) / folder_path.lstrip("/"))
        self._mkdir_p(full_path)
        return True
