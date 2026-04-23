"""
SharePoint integration (source + destination) using Microsoft Graph API.
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import httpx
from pydantic import BaseModel, Field, AliasChoices, field_validator

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class SharePointConfig(BaseModel):
    """Configuration for SharePoint integration."""
    client_id: str = Field(validation_alias=AliasChoices("clientId", "client_id"))
    client_secret: str = Field(validation_alias=AliasChoices("clientSecret", "client_secret"))
    tenant_id: str = Field(validation_alias=AliasChoices("tenantId", "tenant_id"))
    site_id: str = Field(validation_alias=AliasChoices("siteId", "site_id"))
    drive_id: str = Field(validation_alias=AliasChoices("driveId", "drive_id"))
    folder_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("folderId", "folder_id")
    )
    tags_to_include: list[dict] = Field(
        default_factory=list,
        validation_alias=AliasChoices("tagsToInclude", "tags_to_include")
    )
    tags_to_exclude: list[dict] = Field(
        default_factory=list,
        validation_alias=AliasChoices("tagsToExclude", "tags_to_exclude")
    )

    @field_validator("tags_to_include", "tags_to_exclude", mode="before")
    @classmethod
    def parse_json_string(cls, v):
        if isinstance(v, str):
            if not v.strip():
                return []
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return []
        return v

    class Config:
        extra = "ignore"


class _SharePointBase:
    """Shared logic for SharePoint source and destination."""

    GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, config: dict[str, Any]):
        self._config = SharePointConfig.model_validate(config)
        self._access_token: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.SHAREPOINT

    async def connect(self) -> bool:
        try:
            self._client = httpx.AsyncClient(timeout=120.0)
            self._access_token = await self._get_access_token()
            return self._access_token is not None
        except Exception as e:
            logger.error(f"Failed to connect to SharePoint: {e}")
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        self._access_token = None

    async def _get_access_token(self) -> Optional[str]:
        url = f"https://login.microsoftonline.com/{self._config.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        response = await self._client.post(url, data=data)

        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            logger.error(f"Auth error: {response.status_code} - {response.text}")
            return None

    def _get_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    def _drive_url(self) -> str:
        return f"{self.GRAPH_API_BASE}/sites/{self._config.site_id}/drives/{self._config.drive_id}"

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None
    ) -> list[FileInfo]:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        files: list[FileInfo] = []
        folder_id = self._config.folder_id
        await self._list_folder_recursive(folder_id, "", files, extensions)
        return files

    async def _list_folder_recursive(
        self,
        folder_id: Optional[str],
        current_path: str,
        files: list[FileInfo],
        extensions: Optional[list[str]]
    ) -> None:
        has_tag_filters = bool(self._config.tags_to_include or self._config.tags_to_exclude)
        items = await self._get_children(folder_id, expand_list_item=has_tag_filters)

        for item in items:
            item_name = item.get("name", "")
            item_path = f"{current_path}/{item_name}" if current_path else f"/{item_name}"

            if "folder" in item:
                await self._list_folder_recursive(
                    item["id"], item_path, files, extensions
                )
            elif "file" in item:
                file_ext = Path(item_name).suffix.lower()
                if extensions is None or file_ext in [e.lower() for e in extensions]:
                    if not self._check_tags(item):
                        continue

                    modified_str = item.get("lastModifiedDateTime", "")
                    try:
                        modified_at = datetime.fromisoformat(modified_str.replace("Z", "+00:00"))
                    except Exception:
                        modified_at = datetime.utcnow()

                    files.append(FileInfo(
                        name=item_name,
                        path=item_path,
                        modified_at=modified_at,
                        size=item.get("size"),
                        content_type=item.get("file", {}).get("mimeType")
                    ))

    def _check_tags(self, item: dict) -> bool:
        tags_to_include = self._config.tags_to_include
        tags_to_exclude = self._config.tags_to_exclude

        if not tags_to_include and not tags_to_exclude:
            return True

        file_tags: dict = item.get("listItem", {}).get("fields", {})

        for tag_filter in tags_to_include:
            column_name = tag_filter.get("columnName")
            column_values = tag_filter.get("columnValues", [])
            if column_name not in file_tags or file_tags[column_name] not in column_values:
                return False

        for tag_filter in tags_to_exclude:
            column_name = tag_filter.get("columnName")
            column_values = tag_filter.get("columnValues", [])
            if column_name in file_tags and file_tags[column_name] in column_values:
                return False

        return True

    async def _get_children(
        self,
        item_id: Optional[str] = None,
        expand_list_item: bool = False
    ) -> list[dict]:
        if item_id is None:
            url = f"{self._drive_url()}/root/children"
        else:
            url = f"{self._drive_url()}/items/{item_id}/children"

        if expand_list_item:
            url += "?$expand=listItem($expand=fields)"

        all_items: list[dict] = []

        while url:
            response = await self._client.get(url, headers=self._get_headers())

            if response.status_code == 200:
                data = response.json()
                all_items.extend(data.get("value", []))
                url = data.get("@odata.nextLink")
            else:
                logger.warning(f"Failed to get children: {response.status_code}")
                break

        return all_items

    async def _get_item_id_by_path(self, path: str) -> Optional[str]:
        url = f"{self.GRAPH_API_BASE}/sites/{self._config.site_id}/drive/root:/{path.lstrip('/')}"

        response = await self._client.get(url, headers=self._get_headers())

        if response.status_code == 200:
            return response.json().get("id")
        return None

    async def _get_folder_path(self, folder_id: str) -> Optional[str]:
        url = f"{self._drive_url()}/items/{folder_id}"
        response = await self._client.get(url, headers=self._get_headers())

        if response.status_code == 200:
            data = response.json()
            parent_ref = data.get("parentReference", {})
            parent_path = parent_ref.get("path", "")
            if "root:" in parent_path:
                parent_path = parent_path.split("root:", 1)[1]
            name = data.get("name", "")
            return f"{parent_path}/{name}".strip("/")
        return None


class SharePointSource(_SharePointBase, SourceIntegration):
    """SharePoint source integration for reading files via Microsoft Graph API."""

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            item_id = await self._get_item_id_by_path(file_info.path)
            if not item_id:
                logger.error(f"Could not find item: {file_info.path}")
                return False

            url = f"{self.GRAPH_API_BASE}/sites/{self._config.site_id}/drives/{self._config.drive_id}/items/{item_id}/content"
            response = await self._client.get(url, headers=self._get_headers(), follow_redirects=True)

            if response.status_code == 200:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(response.content)
                return True
            else:
                logger.error(f"Download failed: {response.status_code} - {file_info.path}")
                return False

        except Exception as e:
            logger.error(f"Download error for {file_info.path}: {e}")
            return False


class SharePointDestination(_SharePointBase, DestinationIntegration):
    """SharePoint destination integration for uploading converted files."""

    SIMPLE_UPLOAD_LIMIT = 4 * 1024 * 1024

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            content = await asyncio.to_thread(local_path.read_bytes)
            encoded_path = remote_path.lstrip("/")

            if self._config.folder_id:
                base_path = await self._get_folder_path(self._config.folder_id)
                if base_path:
                    encoded_path = f"{base_path.lstrip('/')}/{encoded_path}"

            if len(content) <= self.SIMPLE_UPLOAD_LIMIT:
                return await self._simple_upload(encoded_path, content)
            else:
                return await self._session_upload(encoded_path, content)

        except Exception as e:
            logger.error(f"Upload error for {remote_path}: {e}")
            return False

    async def _simple_upload(self, remote_path: str, content: bytes) -> bool:
        url = f"{self._drive_url()}/root:/{remote_path}:/content"
        headers = {**self._get_headers(), "Content-Type": "application/octet-stream"}

        response = await self._client.put(url, headers=headers, content=content)

        if response.status_code in (200, 201):
            logger.debug(f"Uploaded (simple): {remote_path}")
            return True
        else:
            logger.error(f"Simple upload failed for {remote_path}: {response.status_code} - {response.text}")
            return False

    async def _session_upload(self, remote_path: str, content: bytes) -> bool:
        url = f"{self._drive_url()}/root:/{remote_path}:/createUploadSession"
        body = {
            "item": {
                "@microsoft.graph.conflictBehavior": "replace",
                "name": Path(remote_path).name
            }
        }
        headers = {**self._get_headers(), "Content-Type": "application/json"}
        response = await self._client.post(url, headers=headers, content=json.dumps(body))

        if response.status_code not in (200, 201):
            logger.error(f"Failed to create upload session for {remote_path}: {response.status_code}")
            return False

        upload_url = response.json().get("uploadUrl")
        if not upload_url:
            return False

        chunk_size = 10 * 1024 * 1024
        total = len(content)

        for offset in range(0, total, chunk_size):
            end = min(offset + chunk_size, total)
            chunk = content[offset:end]
            chunk_headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {offset}-{end - 1}/{total}"
            }
            resp = await self._client.put(upload_url, headers=chunk_headers, content=chunk)
            if resp.status_code not in (200, 201, 202):
                logger.error(f"Chunk upload failed at {offset}: {resp.status_code}")
                return False

        logger.debug(f"Uploaded (session): {remote_path}")
        return True

    async def create_folder(self, folder_path: str) -> bool:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            parts = [p for p in folder_path.split("/") if p]
            current_parent_path = ""

            if self._config.folder_id:
                base = await self._get_folder_path(self._config.folder_id)
                if base:
                    current_parent_path = base

            for part in parts:
                if current_parent_path:
                    target_path = f"{current_parent_path}/{part}"
                else:
                    target_path = part

                check_url = f"{self._drive_url()}/root:/{target_path}"
                resp = await self._client.get(check_url, headers=self._get_headers())

                if resp.status_code == 200:
                    current_parent_path = target_path
                    continue

                if current_parent_path:
                    create_url = f"{self._drive_url()}/root:/{current_parent_path}:/children"
                else:
                    create_url = f"{self._drive_url()}/root/children"

                body = {
                    "name": part,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "fail"
                }
                headers = {**self._get_headers(), "Content-Type": "application/json"}
                resp = await self._client.post(create_url, headers=headers, content=json.dumps(body))

                if resp.status_code in (200, 201, 409):
                    current_parent_path = target_path
                else:
                    logger.error(f"Failed to create folder {part}: {resp.status_code} - {resp.text}")
                    return False

            return True
        except Exception as e:
            logger.error(f"Failed to create folder {folder_path}: {e}")
            return False
