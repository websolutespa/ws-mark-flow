"""Integration package for source and destination connectors."""
from .base import (
    BaseIntegration,
    SourceIntegration,
    DestinationIntegration,
    FileInfo,
    IntegrationType,
    CONVERTIBLE_EXTENSIONS,
    MARKDOWN_EXTENSIONS,
)
from .filesystem import FilesystemConfig, FilesystemSource, FilesystemDestination
from .azure_blob import AzureBlobConfig, AzureBlobSource, AzureBlobDestination
from .sftp import SFTPConfig, SFTPSource, SFTPDestination
from .google_drive import GoogleDriveConfig, GoogleDriveSource, GoogleDriveDestination
from .sharepoint import SharePointConfig, SharePointSource, SharePointDestination
from .s3 import S3Config, S3Source, S3Destination
from .gcs import GCSConfig, GCSSource, GCSDestination

__all__ = [
    # Base classes
    "BaseIntegration",
    "SourceIntegration",
    "DestinationIntegration",
    "FileInfo",
    "IntegrationType",
    "CONVERTIBLE_EXTENSIONS",
    "MARKDOWN_EXTENSIONS",
    # Filesystem
    "FilesystemConfig",
    "FilesystemSource",
    "FilesystemDestination",
    # Azure Blob
    "AzureBlobConfig",
    "AzureBlobSource",
    "AzureBlobDestination",
    # SFTP
    "SFTPConfig",
    "SFTPSource",
    "SFTPDestination",
    # Google Drive
    "GoogleDriveConfig",
    "GoogleDriveSource",
    "GoogleDriveDestination",
    # SharePoint
    "SharePointConfig",
    "SharePointSource",
    "SharePointDestination",
    # S3
    "S3Config",
    "S3Source",
    "S3Destination",
    # GCS
    "GCSConfig",
    "GCSSource",
    "GCSDestination",
]
