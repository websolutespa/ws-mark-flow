"""
AWS S3 integration (source + destination).
"""
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

from pydantic import BaseModel, Field, AliasChoices

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class S3Config(BaseModel):
    """Configuration for AWS S3 integration."""
    bucket: str = Field(description="S3 bucket name")
    prefix: Optional[str] = Field(default=None, description="Optional key prefix (folder path)")
    region: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("region", "region_name"),
        description="AWS region name"
    )
    access_key_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("accessKeyId", "access_key_id", "aws_access_key_id"),
        description="AWS access key ID"
    )
    secret_access_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("secretAccessKey", "secret_access_key", "aws_secret_access_key"),
        description="AWS secret access key"
    )
    session_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("sessionToken", "session_token", "aws_session_token"),
        description="AWS session token (for temporary credentials)"
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("endpointUrl", "endpoint_url"),
        description="Custom endpoint URL (for S3-compatible services like MinIO)"
    )

    class Config:
        extra = "ignore"


class _S3Base:
    """Shared logic for S3 source and destination."""

    def __init__(self, config: dict[str, Any]):
        self._config = S3Config.model_validate(config)
        self._client = None
        self._session = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.S3

    async def connect(self) -> bool:
        try:
            import aiobotocore.session

            self._session = aiobotocore.session.get_session()

            kwargs: dict[str, Any] = {}
            if self._config.region:
                kwargs["region_name"] = self._config.region
            if self._config.access_key_id:
                kwargs["aws_access_key_id"] = self._config.access_key_id
            if self._config.secret_access_key:
                kwargs["aws_secret_access_key"] = self._config.secret_access_key
            if self._config.session_token:
                kwargs["aws_session_token"] = self._config.session_token
            if self._config.endpoint_url:
                kwargs["endpoint_url"] = self._config.endpoint_url

            self._client = await self._session.create_client("s3", **kwargs).__aenter__()

            # Verify connectivity
            await self._client.head_bucket(Bucket=self._config.bucket)
            logger.info(f"Connected to S3 bucket: {self._config.bucket}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            self._client = None
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None
        self._session = None

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
    ) -> list[FileInfo]:
        if not self._client:
            raise RuntimeError("Not connected. Call connect() first.")

        prefix = folder_path or self._config.prefix or ""
        files: list[FileInfo] = []

        paginator = self._client.get_paginator("list_objects_v2")
        params: dict[str, Any] = {"Bucket": self._config.bucket}
        if prefix:
            params["Prefix"] = prefix

        async for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Skip "directory" markers
                if key.endswith("/"):
                    continue

                name = Path(key).name
                ext = Path(name).suffix.lower()
                if extensions and ext not in [e.lower() for e in extensions]:
                    continue

                path = f"/{key}" if not key.startswith("/") else key
                modified_at = obj.get("LastModified", datetime.utcnow())

                files.append(FileInfo(
                    name=name,
                    path=path,
                    modified_at=modified_at,
                    size=obj.get("Size"),
                    content_type=None,
                ))

        return files


class S3Source(_S3Base, SourceIntegration):
    """AWS S3 source integration for reading files."""

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._client:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            key = file_info.path.lstrip("/")

            response = await self._client.get_object(
                Bucket=self._config.bucket,
                Key=key
            )

            local_path.parent.mkdir(parents=True, exist_ok=True)
            data = await response["Body"].read()
            await asyncio.to_thread(local_path.write_bytes, data)

            logger.debug(f"Downloaded s3://{self._config.bucket}/{key} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {file_info.path}: {e}")
            return False


class S3Destination(_S3Base, DestinationIntegration):
    """AWS S3 destination integration for uploading files."""

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._client:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            key = remote_path.lstrip("/")
            if self._config.prefix:
                prefix = self._config.prefix.strip("/")
                key = f"{prefix}/{key}"

            data = await asyncio.to_thread(local_path.read_bytes)
            await self._client.put_object(
                Bucket=self._config.bucket,
                Key=key,
                Body=data
            )

            logger.debug(f"Uploaded {local_path} -> s3://{self._config.bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {remote_path}: {e}")
            return False

    async def create_folder(self, folder_path: str) -> bool:
        # S3 folders are virtual; no-op
        return True
