"""
Base interfaces for Source and Destination integrations.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class FileInfo(BaseModel):
    """Represents a file with its metadata."""
    name: str = Field(description="File name with extension")
    path: str = Field(description="Full path relative to root (e.g., /folder/subfolder/file.pdf)")
    modified_at: datetime = Field(description="Last modification timestamp")
    size: Optional[int] = Field(default=None, description="File size in bytes")
    content_type: Optional[str] = Field(default=None, description="MIME type of the file")
    
    @property
    def stem(self) -> str:
        """Returns filename without extension."""
        return Path(self.name).stem
    
    @property
    def suffix(self) -> str:
        """Returns file extension including dot."""
        return Path(self.name).suffix.lower()
    
    @property
    def parent_path(self) -> str:
        """Returns parent directory path."""
        return str(Path(self.path).parent)
    
    def to_markdown_path(self) -> str:
        """Returns the expected markdown file path."""
        parent = Path(self.path).parent
        return str(parent / f"{self.stem}.md")


class IntegrationType(str, Enum):
    """Supported integration types."""
    SHAREPOINT = "sharepoint"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"
    GOOGLE_DRIVE = "google_drive"
    SFTP = "sftp"
    LOCAL = "local"
    SITEMAP = "sitemap"


class BaseIntegration(ABC):
    """Base class for all source and destination integrations."""
    
    @property
    @abstractmethod
    def integration_type(self) -> IntegrationType:
        """Returns the type of integration."""
        pass
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the storage.
        Returns True if connection is successful.
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up connection resources."""
        pass
    
    async def __aenter__(self):
        if not await self.connect():
            raise RuntimeError(f"Failed to connect to {self.integration_type.value}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    @abstractmethod
    async def list_files(
        self, 
        extensions: Optional[list[str]] = None,
        folder_path: Optional[str] = None
    ) -> list[FileInfo]:
        """
        List all files in the source.
        
        Args:
            extensions: Filter by file extensions (e.g., ['.pdf', '.docx', '.pptx'])
            folder_path: Optional folder path to start from
            
        Returns:
            List of FileInfo objects for matching files
        """
        pass        


class SourceIntegration(BaseIntegration):
    """
    Interface for source integrations (where files are read from).
    Sources list files and download them for conversion.
    """
    
    @abstractmethod
    async def download_file(self, file_info: FileInfo, local_path: Path) -> bool:
        """
        Download a file to a local path.
        
        Args:
            file_info: The file to download
            local_path: Local destination path
            
        Returns:
            True if download was successful
        """
        pass
    
    async def download_files(
        self, 
        files: list[FileInfo], 
        local_dir: Path,
        preserve_structure: bool = True
    ) -> dict[str, Path]:
        """
        Download multiple files to a local directory.
        
        Args:
            files: List of files to download
            local_dir: Local directory to download to
            preserve_structure: If True, preserve folder structure
            
        Returns:
            Dict mapping original paths to local paths
        """
        results = {}
        for file_info in files:
            if preserve_structure:
                local_path = local_dir / file_info.path.lstrip('/')
            else:
                local_path = local_dir / file_info.name
            
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            if await self.download_file(file_info, local_path):
                results[file_info.path] = local_path
        
        return results


class DestinationIntegration(BaseIntegration):
    """
    Interface for destination integrations (where converted files are uploaded).
    Destinations list existing files and upload new ones.
    """
    
    @abstractmethod
    async def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """
        Upload a file to the destination.
        
        Args:
            local_path: Local file path
            remote_path: Destination path (including filename)
            
        Returns:
            True if upload was successful
        """
        pass
    
    @abstractmethod
    async def create_folder(self, folder_path: str) -> bool:
        """
        Create a folder at the specified path.
        
        Args:
            folder_path: Path of the folder to create
            
        Returns:
            True if creation was successful (or folder already exists)
        """
        pass
    
    async def upload_files(
        self, 
        files: dict[Path, str],
        create_folders: bool = True
    ) -> dict[str, bool]:
        """
        Upload multiple files to the destination.
        
        Args:
            files: Dict mapping local paths to remote paths
            create_folders: If True, create necessary folders
            
        Returns:
            Dict mapping remote paths to success status
        """
        results = {}
        
        # Create necessary folders first
        if create_folders:
            folders = set()
            for remote_path in files.values():
                parent = str(Path(remote_path).parent)
                if parent and parent != '.':
                    folders.add(parent)
            
            for folder in sorted(folders):  # Sort to create parent folders first
                await self.create_folder(folder)
        
        # Upload files
        for local_path, remote_path in files.items():
            results[remote_path] = await self.upload_file(local_path, remote_path)
        
        return results


# Supported file extensions for conversion
CONVERTIBLE_EXTENSIONS = [
    '.pdf', '.docx', '.doc', '.pptx', '.ppt', 
    '.xlsx', '.xls', '.csv', '.txt', '.rtf',
    '.html', '.htm', '.xml', '.json', '.yaml', '.yml',
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff',
    #'.mp3', '.wav', '.m4a',  # Audio (for transcription)
]

MARKDOWN_EXTENSIONS = ['.md']
