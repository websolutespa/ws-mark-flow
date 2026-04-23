"""
Google Cloud Storage integration (source + destination).
"""
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

from pydantic import BaseModel, Field, AliasChoices

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class GCSConfig(BaseModel):
    """Configuration for Google Cloud Storage integration."""
    bucket: str = Field(description="GCS bucket name")
    prefix: Optional[str] = Field(default=None, description="Optional key prefix (folder path)")
    project: Optional[str] = Field(
        default=None,
        description="GCP project ID"
    )
    service_account_json: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("serviceAccountJson", "service_account_json"),
        description="Path to service account JSON file or the JSON content itself"
    )

    class Config:
        extra = "ignore"


class _GCSBase:
    """Shared logic for GCS source and destination."""

    def __init__(self, config: dict[str, Any]):
        self._config = GCSConfig.model_validate(config)
        self._client = None
        self._bucket = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.GCS

    async def connect(self) -> bool:
        try:
            from google.cloud import storage as gcs_storage
            import json as _json

            kwargs: dict[str, Any] = {}
            if self._config.project:
                kwargs["project"] = self._config.project

            if self._config.service_account_json:
                from google.oauth2 import service_account

                sa_json = self._config.service_account_json
                if sa_json.startswith("{"):
                    info = _json.loads(sa_json)
                else:
                    with open(sa_json) as f:
                        info = _json.load(f)

                credentials = service_account.Credentials.from_service_account_info(info)
                kwargs["credentials"] = credentials

            self._client = await asyncio.to_thread(gcs_storage.Client, **kwargs)
            self._bucket = self._client.bucket(self._config.bucket)

            # Verify connectivity
            await asyncio.to_thread(self._bucket.reload)
            logger.info(f"Connected to GCS bucket: {self._config.bucket}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {e}")
            self._client = None
            self._bucket = None
            return False

    async def disconnect(self) -> None:
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None
        self._bucket = None

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
    ) -> list[FileInfo]:
        if not self._client:
            raise RuntimeError("Not connected. Call connect() first.")

        prefix = folder_path or self._config.prefix or ""

        blobs = await asyncio.to_thread(
            lambda: list(self._client.list_blobs(self._config.bucket, prefix=prefix or None))
        )

        files: list[FileInfo] = []
        for blob in blobs:
            # Skip "directory" markers
            if blob.name.endswith("/"):
                continue

            name = Path(blob.name).name
            ext = Path(name).suffix.lower()
            if extensions and ext not in [e.lower() for e in extensions]:
                continue

            path = f"/{blob.name}" if not blob.name.startswith("/") else blob.name
            modified_at = blob.updated or datetime.utcnow()

            files.append(FileInfo(
                name=name,
                path=path,
                modified_at=modified_at,
                size=blob.size,
                content_type=blob.content_type,
            ))

        return files


class GCSSource(_GCSBase, SourceIntegration):
    """Google Cloud Storage source integration for reading files."""

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._bucket:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            key = file_info.path.lstrip("/")
            blob = self._bucket.blob(key)

            local_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(blob.download_to_filename, str(local_path))

            logger.debug(f"Downloaded gs://{self._config.bucket}/{key} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {file_info.path}: {e}")
            return False


class GCSDestination(_GCSBase, DestinationIntegration):
    """Google Cloud Storage destination integration for uploading files."""

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._bucket:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            key = remote_path.lstrip("/")
            if self._config.prefix:
                prefix = self._config.prefix.strip("/")
                key = f"{prefix}/{key}"

            blob = self._bucket.blob(key)
            await asyncio.to_thread(blob.upload_from_filename, str(local_path))

            logger.debug(f"Uploaded {local_path} -> gs://{self._config.bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {remote_path}: {e}")
            return False

    async def create_folder(self, folder_path: str) -> bool:
        # GCS folders are virtual; no-op
        return True
