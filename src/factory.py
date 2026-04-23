"""
Integration factory for creating source and destination instances.
"""
from typing import Any, Type

from .integration import (
    SourceIntegration,
    DestinationIntegration,
    IntegrationType,
    FilesystemSource,
    FilesystemDestination,
    SharePointSource,
    SharePointDestination,
    GoogleDriveSource,
    GoogleDriveDestination,
    SFTPSource,
    SFTPDestination,
    AzureBlobSource,
    AzureBlobDestination,
    S3Source,
    S3Destination,
    GCSSource,
    GCSDestination,
)


# Registry of source integrations
SOURCE_REGISTRY: dict[IntegrationType, Type[SourceIntegration]] = {
    IntegrationType.LOCAL: FilesystemSource, 
    IntegrationType.SHAREPOINT: SharePointSource,
    IntegrationType.SFTP: SFTPSource,
    IntegrationType.AZURE_BLOB: AzureBlobSource,
    IntegrationType.GOOGLE_DRIVE: GoogleDriveSource,
    IntegrationType.S3: S3Source,
    IntegrationType.GCS: GCSSource,
}

# Registry of destination integrations
DESTINATION_REGISTRY: dict[IntegrationType, Type[DestinationIntegration]] = {
    IntegrationType.LOCAL: FilesystemDestination,
    IntegrationType.GOOGLE_DRIVE: GoogleDriveDestination,
    IntegrationType.SHAREPOINT: SharePointDestination,
    IntegrationType.SFTP: SFTPDestination,
    IntegrationType.AZURE_BLOB: AzureBlobDestination,
    IntegrationType.S3: S3Destination,
    IntegrationType.GCS: GCSDestination,
}


def create_source(integration_type: IntegrationType, config: dict[str, Any]) -> SourceIntegration:
    """
    Create a source integration instance.
    
    Args:
        integration_type: Type of integration
        config: Integration-specific configuration
        
    Returns:
        Configured SourceIntegration instance
        
    Raises:
        ValueError: If integration type is not supported
    """
    source_class = SOURCE_REGISTRY.get(integration_type)
    if source_class is None:
        raise ValueError(f"Unsupported source type: {integration_type}")
    
    return source_class(config)


def create_destination(integration_type: IntegrationType, config: dict[str, Any]) -> DestinationIntegration:
    """
    Create a destination integration instance.
    
    Args:
        integration_type: Type of integration
        config: Integration-specific configuration
        
    Returns:
        Configured DestinationIntegration instance
        
    Raises:
        ValueError: If integration type is not supported
    """
    dest_class = DESTINATION_REGISTRY.get(integration_type)
    if dest_class is None:
        raise ValueError(f"Unsupported destination type: {integration_type}")
    
    return dest_class(config)


def get_supported_sources() -> list[str]:
    """Get list of supported source integration types."""
    return [t.value for t in SOURCE_REGISTRY.keys()]


def get_supported_destinations() -> list[str]:
    """Get list of supported destination integration types."""
    return [t.value for t in DESTINATION_REGISTRY.keys()]
