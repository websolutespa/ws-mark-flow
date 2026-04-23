"""
Azure Blob Storage integration (source + destination).
"""
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

from pydantic import BaseModel, Field, AliasChoices

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class AzureBlobConfig(BaseModel):
    """Configuration for Azure Blob Storage integration."""
    connection_string: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("connectionString", "connection_string"),
        description="Azure Storage connection string"
    )
    account_name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("accountName", "account_name"),
        description="Azure Storage account name"
    )
    account_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("accountKey", "account_key"),
        description="Azure Storage account key"
    )
    container: str = Field(description="Blob container name")
    prefix: Optional[str] = Field(default=None, description="Optional blob prefix (folder path)")

    class Config:
        extra = "ignore"


class _AzureBlobBase:
    """Shared logic for Azure Blob source and destination."""

    def __init__(self, config: dict[str, Any]):
        self._config = AzureBlobConfig.model_validate(config)
        self._container_client = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.AZURE_BLOB

    async def connect(self) -> bool:
        try:
            from azure.storage.blob.aio import ContainerClient

            if self._config.connection_string:
                self._container_client = ContainerClient.from_connection_string(
                    conn_str=self._config.connection_string,
                    container_name=self._config.container,
                )
            elif self._config.account_name and self._config.account_key:
                account_url = f"https://{self._config.account_name}.blob.core.windows.net"
                self._container_client = ContainerClient(
                    account_url=account_url,
                    container_name=self._config.container,
                    credential=self._config.account_key,
                )
            else:
                logger.error("Azure Blob: provide connection_string or account_name+account_key")
                return False

            await self._container_client.get_container_properties()
            logger.info(f"Connected to Azure Blob container: {self._config.container}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Azure Blob Storage: {e}")
            self._container_client = None
            return False

    async def disconnect(self) -> None:
        if self._container_client:
            await self._container_client.close()
            self._container_client = None

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
    ) -> list[FileInfo]:
        if not self._container_client:
            raise RuntimeError("Not connected. Call connect() first.")

        prefix = folder_path or self._config.prefix or ""
        files: list[FileInfo] = []

        async for blob in self._container_client.list_blobs(name_starts_with=prefix or None):
            if blob.name.endswith("/"):
                continue

            name = Path(blob.name).name
            ext = Path(name).suffix.lower()
            if extensions and ext not in [e.lower() for e in extensions]:
                continue

            path = f"/{blob.name}" if not blob.name.startswith("/") else blob.name
            modified_at = blob.last_modified or datetime.utcnow()

            files.append(FileInfo(
                name=name,
                path=path,
                modified_at=modified_at,
                size=blob.size,
                content_type=blob.content_settings.content_type if blob.content_settings else None,
            ))

        return files


class AzureBlobSource(_AzureBlobBase, SourceIntegration):
    """Azure Blob Storage source integration for reading files."""

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._container_client:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            blob_name = file_info.path.lstrip("/")
            blob_client = self._container_client.get_blob_client(blob_name)

            local_path.parent.mkdir(parents=True, exist_ok=True)
            download = await blob_client.download_blob()
            data = await download.readall()
            await asyncio.to_thread(local_path.write_bytes, data)

            logger.debug(f"Downloaded blob: {blob_name} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download blob {file_info.path}: {e}")
            return False


class AzureBlobDestination(_AzureBlobBase, DestinationIntegration):
    """Azure Blob Storage destination integration for uploading files."""

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._container_client:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            blob_name = remote_path.lstrip("/")
            if self._config.prefix:
                prefix = self._config.prefix.strip("/")
                blob_name = f"{prefix}/{blob_name}"
            blob_client = self._container_client.get_blob_client(blob_name)

            data = await asyncio.to_thread(local_path.read_bytes)
            await blob_client.upload_blob(data, overwrite=True)

            logger.debug(f"Uploaded blob: {local_path} -> {blob_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload blob {remote_path}: {e}")
            return False

    async def create_folder(self, folder_path: str) -> bool:
        return True
