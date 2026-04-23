"""
Google Drive integration (source + destination) using Google Drive API.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import httpx
from pydantic import BaseModel, Field, AliasChoices

from .base import SourceIntegration, DestinationIntegration, FileInfo, IntegrationType

logger = logging.getLogger(__name__)


class GoogleDriveConfig(BaseModel):
    """Configuration for Google Drive integration."""
    # Service account credentials
    service_account_json: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("serviceAccountJson", "service_account_json"),
        description="Path to service account JSON file or the JSON content itself"
    )
    # Or OAuth2 tokens
    access_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("accessToken", "access_token")
    )
    refresh_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("refreshToken", "refresh_token")
    )
    client_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("clientId", "client_id")
    )
    client_secret: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("clientSecret", "client_secret")
    )
    # Target folder
    folder_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("folderId", "folder_id"),
        description="Root folder ID (default: root)"
    )

    class Config:
        extra = "ignore"


class _GoogleDriveBase:
    """Shared logic for Google Drive source and destination."""

    DRIVE_API_BASE = "https://www.googleapis.com/drive/v3"
    UPLOAD_API_BASE = "https://www.googleapis.com/upload/drive/v3"

    # Subclasses set the OAuth scope they need
    _oauth_scope: str = "https://www.googleapis.com/auth/drive"

    def __init__(self, config: dict[str, Any]):
        self._config = GoogleDriveConfig.model_validate(config)
        self._access_token: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def integration_type(self) -> IntegrationType:
        return IntegrationType.GOOGLE_DRIVE

    async def connect(self) -> bool:
        try:
            self._client = httpx.AsyncClient(timeout=120.0)

            if self._config.service_account_json:
                self._access_token = await self._get_service_account_token()
            elif self._config.access_token:
                self._access_token = self._config.access_token
                if self._config.refresh_token:
                    await self._refresh_token()
            else:
                raise ValueError("No credentials provided. Need service_account_json or access_token")

            return self._access_token is not None
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive: {e}")
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        self._access_token = None

    async def _get_service_account_token(self) -> Optional[str]:
        import jwt

        if self._config.service_account_json.startswith("{"):
            sa_info = json.loads(self._config.service_account_json)
        else:
            with open(self._config.service_account_json) as f:
                sa_info = json.load(f)

        now = int(time.time())
        payload = {
            "iss": sa_info["client_email"],
            "scope": self._oauth_scope,
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600
        }

        signed_jwt = jwt.encode(
            payload,
            sa_info["private_key"],
            algorithm="RS256"
        )

        response = await self._client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": signed_jwt
            }
        )

        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            logger.error(f"Service account auth error: {response.text}")
            return None

    async def _refresh_token(self) -> bool:
        if not self._config.refresh_token or not self._config.client_id:
            return False

        response = await self._client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._config.refresh_token,
                "client_id": self._config.client_id,
                "client_secret": self._config.client_secret or ""
            }
        )

        if response.status_code == 200:
            self._access_token = response.json().get("access_token")
            return True
        return False

    def _get_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def list_files(
        self,
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None
    ) -> list[FileInfo]:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        files: list[FileInfo] = []
        root_folder_id = self._config.folder_id or "root"

        await self._list_folder_recursive(
            root_folder_id, "", files, extensions
        )

        return files

    async def _list_folder_recursive(
        self,
        folder_id: str,
        current_path: str,
        files: list[FileInfo],
        extensions: Optional[list[str]]
    ) -> None:
        items = await self._list_folder_contents(folder_id)

        for item in items:
            item_name = item.get("name", "")
            item_path = f"{current_path}/{item_name}" if current_path else f"/{item_name}"

            if item.get("mimeType") == "application/vnd.google-apps.folder":
                await self._list_folder_recursive(
                    item["id"], item_path, files, extensions
                )
            else:
                file_ext = Path(item_name).suffix.lower()
                if extensions is None or file_ext in [e.lower() for e in extensions]:
                    modified_str = item.get("modifiedTime", "")
                    try:
                        modified_at = datetime.fromisoformat(modified_str.replace("Z", "+00:00"))
                    except Exception:
                        modified_at = datetime.utcnow()

                    files.append(FileInfo(
                        name=item_name,
                        path=item_path,
                        modified_at=modified_at,
                        size=int(item.get("size", 0)) if item.get("size") else None,
                        content_type=item.get("mimeType")
                    ))

    async def _list_folder_contents(self, folder_id: str) -> list[dict]:
        all_items: list[dict] = []
        page_token = None

        while True:
            params = {
                "q": f"'{folder_id}' in parents and trashed = false",
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                "pageSize": 1000
            }
            if page_token:
                params["pageToken"] = page_token

            response = await self._client.get(
                f"{self.DRIVE_API_BASE}/files",
                headers=self._get_headers(),
                params=params
            )

            if response.status_code == 200:
                data = response.json()
                all_items.extend(data.get("files", []))
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            else:
                logger.warning(f"Failed to list folder: {response.status_code}")
                break

        return all_items

    async def _get_file_id_by_name(self, filename: str, parent_id: str) -> Optional[str]:
        params = {
            "q": f"name = '{filename}' and '{parent_id}' in parents and trashed = false",
            "fields": "files(id)"
        }

        response = await self._client.get(
            f"{self.DRIVE_API_BASE}/files",
            headers=self._get_headers(),
            params=params
        )

        if response.status_code == 200:
            files = response.json().get("files", [])
            if files:
                return files[0]["id"]
        return None

    async def _get_folder_id_by_name(self, name: str, parent_id: str) -> Optional[str]:
        params = {
            "q": f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            "fields": "files(id)"
        }

        response = await self._client.get(
            f"{self.DRIVE_API_BASE}/files",
            headers=self._get_headers(),
            params=params
        )

        if response.status_code == 200:
            files = response.json().get("files", [])
            if files:
                return files[0]["id"]
        return None


class GoogleDriveSource(_GoogleDriveBase, SourceIntegration):
    """Google Drive source integration for downloading files to convert."""

    _oauth_scope = "https://www.googleapis.com/auth/drive.readonly"

    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            file_id = await self._resolve_file_id(file_info)
            if not file_id:
                logger.error(f"Could not resolve file ID for {file_info.path}")
                return False

            response = await self._client.get(
                f"{self.DRIVE_API_BASE}/files/{file_id}",
                headers=self._get_headers(),
                params={"alt": "media"}
            )

            if response.status_code == 200:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(local_path.write_bytes, response.content)
                logger.debug(f"Downloaded: {file_info.path} -> {local_path}")
                return True
            else:
                logger.error(f"Download failed for {file_info.path}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Failed to download {file_info.path}: {e}")
            return False

    async def _resolve_file_id(self, file_info: FileInfo) -> Optional[str]:
        parts = [p for p in file_info.path.split("/") if p]
        if not parts:
            return None

        current_parent = self._config.folder_id or "root"

        for folder_name in parts[:-1]:
            folder_id = await self._get_folder_id_by_name(folder_name, current_parent)
            if folder_id:
                current_parent = folder_id
            else:
                return None

        return await self._get_file_id_by_name(parts[-1], current_parent)


class GoogleDriveDestination(_GoogleDriveBase, DestinationIntegration):
    """Google Drive destination integration for uploading converted files."""

    _oauth_scope = "https://www.googleapis.com/auth/drive"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self._folder_cache: dict[str, str] = {}

    async def disconnect(self) -> None:
        await super().disconnect()
        self._folder_cache.clear()

    async def _list_folder_recursive(
        self,
        folder_id: str,
        current_path: str,
        files: list[FileInfo],
        extensions: Optional[list[str]]
    ) -> None:
        items = await self._list_folder_contents(folder_id)

        for item in items:
            item_name = item.get("name", "")
            item_path = f"{current_path}/{item_name}" if current_path else f"/{item_name}"

            if item.get("mimeType") == "application/vnd.google-apps.folder":
                self._folder_cache[item_path] = item["id"]
                await self._list_folder_recursive(
                    item["id"], item_path, files, extensions
                )
            else:
                file_ext = Path(item_name).suffix.lower()
                if extensions is None or file_ext in [e.lower() for e in extensions]:
                    modified_str = item.get("modifiedTime", "")
                    try:
                        modified_at = datetime.fromisoformat(modified_str.replace("Z", "+00:00"))
                    except Exception:
                        modified_at = datetime.utcnow()

                    files.append(FileInfo(
                        name=item_name,
                        path=item_path,
                        modified_at=modified_at,
                        size=int(item.get("size", 0)) if item.get("size") else None,
                        content_type=item.get("mimeType")
                    ))

    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        if not self._access_token:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            parent_path = str(Path(remote_path).parent)
            filename = Path(remote_path).name

            if parent_path and parent_path != "/" and parent_path != ".":
                parent_id = await self._ensure_folder_path(parent_path)
            else:
                parent_id = self._config.folder_id or "root"

            existing_id = await self._get_file_id_by_name(filename, parent_id)
            content = local_path.read_bytes()

            if existing_id:
                return await self._update_file(existing_id, content)
            else:
                return await self._create_file(filename, parent_id, content)

        except Exception as e:
            logger.error(f"Upload error for {remote_path}: {e}")
            return False

    async def _create_file(self, filename: str, parent_id: str, content: bytes) -> bool:
        metadata = {
            "name": filename,
            "parents": [parent_id]
        }

        boundary = "===multipart_boundary==="

        body = (
            f"--{boundary}\r\n"
            'Content-Type: application/json; charset=UTF-8\r\n\r\n'
            f'{json.dumps(metadata)}\r\n'
            f"--{boundary}\r\n"
            'Content-Type: application/octet-stream\r\n\r\n'
        ).encode('utf-8') + content + f"\r\n--{boundary}--".encode('utf-8')

        headers = self._get_headers()
        headers["Content-Type"] = f"multipart/related; boundary={boundary}"

        response = await self._client.post(
            f"{self.UPLOAD_API_BASE}/files?uploadType=multipart",
            headers=headers,
            content=body
        )

        return response.status_code in (200, 201)

    async def _update_file(self, file_id: str, content: bytes) -> bool:
        headers = self._get_headers()
        headers["Content-Type"] = "application/octet-stream"

        response = await self._client.patch(
            f"{self.UPLOAD_API_BASE}/files/{file_id}?uploadType=media",
            headers=headers,
            content=content
        )

        return response.status_code == 200

    async def create_folder(self, folder_path: str) -> bool:
        try:
            await self._ensure_folder_path(folder_path)
            return True
        except Exception as e:
            logger.error(f"Failed to create folder {folder_path}: {e}")
            return False

    async def _ensure_folder_path(self, path: str) -> str:
        if path in self._folder_cache:
            return self._folder_cache[path]

        parts = [p for p in path.split("/") if p]
        current_parent = self._config.folder_id or "root"
        current_path = ""

        for part in parts:
            current_path = f"{current_path}/{part}"

            if current_path in self._folder_cache:
                current_parent = self._folder_cache[current_path]
                continue

            existing_id = await self._get_folder_id_by_name(part, current_parent)

            if existing_id:
                current_parent = existing_id
            else:
                current_parent = await self._create_drive_folder(part, current_parent)

            self._folder_cache[current_path] = current_parent

        return current_parent

    async def _create_drive_folder(self, name: str, parent_id: str) -> str:
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]
        }

        response = await self._client.post(
            f"{self.DRIVE_API_BASE}/files",
            headers={**self._get_headers(), "Content-Type": "application/json"},
            content=json.dumps(metadata)
        )

        if response.status_code in (200, 201):
            return response.json()["id"]
        else:
            raise RuntimeError(f"Failed to create folder {name}: {response.text}")
