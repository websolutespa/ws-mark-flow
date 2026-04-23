"""
Data models for conversion jobs, pipelines, and status tracking.
"""
from datetime import datetime
from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field
from bson import ObjectId

from .integration.base import IntegrationType


class PyObjectId(str):
    """Custom type for MongoDB ObjectId."""
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v, handler):
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, str) and ObjectId.is_valid(v):
            return v
        raise ValueError("Invalid ObjectId")


class JobStatus(str, Enum):
    """Status of a conversion job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class FileConversionStatus(str, Enum):
    """Status of individual file conversion."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    CONVERTING = "converting"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class IntegrationConfig(BaseModel):
    """Configuration for a source or destination integration."""
    type: IntegrationType = Field(description="Type of integration")
    config: dict[str, Any] = Field(default_factory=dict, description="Integration-specific configuration")
    
    class Config:
        use_enum_values = True


class FileConversionResult(BaseModel):
    """Result of converting a single file."""
    source_path: str = Field(description="Original file path in source")
    destination_path: Optional[str] = Field(default=None, description="Converted file path in destination")
    status: FileConversionStatus = Field(default=FileConversionStatus.PENDING)
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    file_size: Optional[int] = Field(default=None, description="Original file size in bytes")
    converted_size: Optional[int] = Field(default=None, description="Converted file size in bytes")


class ConversionStats(BaseModel):
    """Statistics for a conversion job."""
    total_files: int = Field(default=0)
    pending_files: int = Field(default=0)
    completed_files: int = Field(default=0)
    failed_files: int = Field(default=0)
    skipped_files: int = Field(default=0)
    total_bytes: int = Field(default=0)
    converted_bytes: int = Field(default=0)
    completion_percentage: float = Field(default=0.0)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate excluding pending files."""
        processed = self.completed_files + self.failed_files + self.skipped_files
        if processed == 0:
            return 0.0
        return round((self.completed_files / processed) * 100, 2)

class ConversionStrategy(str, Enum):
    """Strategy for converting files to markdown"""
    FAST = "fast"  # use faster conversion lib
    BALANCED = "balanced"  # use more accurate conversion lib, specially for pdf and images
    ACCURATE = "accurate"  # use the most accurate conversion method regardless of file format, allowing LLM analysis/conversion for pdf/images and other complex formats, with longer processing time and cost


class JobLLMSettings(BaseModel):
    """
    Per-job LLM overrides for the ACCURATE conversion strategy.
    Any field left as None falls back to the global Settings value.
    """
    llm_provider: Optional[str] = Field(default=None, description="LLM provider override (openai, anthropic, google, ollama)")
    llm_model: Optional[str] = Field(default=None, description="Model name override")
    llm_api_key: Optional[str] = Field(default=None, description="API key override")
    llm_base_url: Optional[str] = Field(default=None, description="Base URL override (e.g. Ollama endpoint)")
    llm_max_pages: Optional[int] = Field(default=None, description="Max PDF pages before skipping LLM")
    pdf_complexity_threshold: Optional[float] = Field(default=None, description="Complexity score below which Docling is used instead of LLM")


class ConversionJob(BaseModel):
    """
    Represents a conversion job/pipeline.
    Stored in MongoDB for persistence and resumability.
    """
    id: Optional[str] = Field(default=None, alias="_id", description="MongoDB ObjectId")
    name: str = Field(description="Human-readable job name")
    description: Optional[str] = Field(default=None)
    
    # Source and destination configuration
    source: IntegrationConfig = Field(description="Source integration configuration")
    destination: IntegrationConfig = Field(description="Destination integration configuration")    
    
    # File filters
    source_extensions: list[str] = Field(
        default=['.pdf', '.docx', '.pptx', '.xlsx', '.csv'],
        description="File extensions to convert from source"
    )
    source_folder: Optional[str] = Field(default=None, description="Optional source folder path")
    destination_folder: Optional[str] = Field(default=None, description="Optional destination folder path")

    # conversion accuracy settings
    conversion_strategy: ConversionStrategy = Field(default=ConversionStrategy.FAST, description="Strategy for handling existing converted files")
    batch_size: int = Field(default=4, description="Number of files to process concurrently")
    llm_settings: Optional[JobLLMSettings] = Field(default=None, description="Per-job LLM overrides for the ACCURATE strategy. Omit to use global settings.")

    # Scheduling
    schedule_cron: Optional[str] = Field(default=None, description="Cron expression for recurring execution (e.g. '0 2 * * *' = daily at 2 AM)")
    schedule_enabled: bool = Field(default=False, description="Whether the cron schedule is active")

    # Status tracking
    status: JobStatus = Field(default=JobStatus.PENDING)
    stats: ConversionStats = Field(default_factory=ConversionStats)
    file_results: list[FileConversionResult] = Field(default_factory=list)
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    
    # Error handling
    error_message: Optional[str] = Field(default=None)
    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)
    
    class Config:
        populate_by_name = True
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat() if v else None
        }
    
    def update_stats(self) -> None:
        """Recalculate statistics from file results."""
        total = len(self.file_results)
        completed = sum(1 for f in self.file_results if f.status == FileConversionStatus.COMPLETED)
        skipped = sum(1 for f in self.file_results if f.status == FileConversionStatus.SKIPPED)
        self.stats = ConversionStats(
            total_files=total,
            pending_files=sum(1 for f in self.file_results if f.status == FileConversionStatus.PENDING),
            completed_files=completed + skipped,
            failed_files=sum(1 for f in self.file_results if f.status == FileConversionStatus.FAILED),
            skipped_files=skipped,
            total_bytes=sum(f.file_size or 0 for f in self.file_results),
            converted_bytes=sum(f.converted_size or 0 for f in self.file_results if f.status == FileConversionStatus.COMPLETED),
            completion_percentage=round(((completed + skipped) / total) * 100, 2) if total > 0 else 0.0
        )
        self.updated_at = datetime.utcnow()


class JobCreateRequest(BaseModel):
    """Request model for creating a new conversion job."""
    name: str = Field(description="Human-readable job name")
    description: Optional[str] = Field(default=None)
    source: IntegrationConfig
    destination: IntegrationConfig
    source_extensions: list[str] = Field(default=['.pdf', '.docx', '.pptx', '.xlsx', '.csv'])
    source_folder: Optional[str] = Field(default=None)
    destination_folder: Optional[str] = Field(default=None)
    conversion_strategy: ConversionStrategy = Field(default=ConversionStrategy.FAST)
    batch_size: int = Field(default=4, description="Number of files to process concurrently")
    llm_settings: Optional[JobLLMSettings] = Field(default=None)
    schedule_cron: Optional[str] = Field(default=None)
    schedule_enabled: bool = Field(default=False)


class JobUpdateRequest(BaseModel):
    """Request model for updating a conversion job."""
    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    source: Optional[IntegrationConfig] = Field(default=None)
    destination: Optional[IntegrationConfig] = Field(default=None)
    source_extensions: Optional[list[str]] = Field(default=None)
    source_folder: Optional[str] = Field(default=None)
    destination_folder: Optional[str] = Field(default=None)
    conversion_strategy: Optional[ConversionStrategy] = Field(default=None)
    batch_size: Optional[int] = Field(default=None, description="Number of files to process concurrently")
    llm_settings: Optional[JobLLMSettings] = Field(default=None)
    schedule_cron: Optional[str] = Field(default=None)
    schedule_enabled: Optional[bool] = Field(default=None)


class ConversionAnalysis(BaseModel):
    """Analysis of what needs to be converted."""
    source_files: list[dict] = Field(default_factory=list, description="Files in source")
    destination_files: list[dict] = Field(default_factory=list, description="Files in destination")
    files_to_convert: list[dict] = Field(default_factory=list, description="Files needing conversion")
    already_converted: list[dict] = Field(default_factory=list, description="Files already converted")
    completion_percentage: float = Field(default=0.0)
    total_source_files: int = Field(default=0)
    total_converted_files: int = Field(default=0)


# ============== Saved Configuration Models ==============

class ConfigFieldDefinition(BaseModel):
    """Definition of a configuration field for UI rendering."""
    name: str = Field(description="Field name/key")
    label: str = Field(description="Human-readable label")
    field_type: str = Field(default="text", description="Field type: text, password, textarea, select")
    required: bool = Field(default=False)
    placeholder: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    options: Optional[list[str]] = Field(default=None, description="Options for select type")


class IntegrationSchema(BaseModel):
    """Schema defining fields for an integration type."""
    type: IntegrationType
    name: str = Field(description="Display name")
    description: Optional[str] = Field(default=None)
    fields: list[ConfigFieldDefinition] = Field(default_factory=list)
    
    class Config:
        use_enum_values = True


# Define schemas for each integration type
LOCAL_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,
    name="Local Filesystem",
    description="Read/write files from/to local disk",
    fields=[
        ConfigFieldDefinition(
            name="path",
            label="Directory Path",
            required=True,
            placeholder="./path/to/directory",
            description="Base directory path on local filesystem"
        )
    ]
)

SHAREPOINT_SCHEMA = IntegrationSchema(
    type=IntegrationType.SHAREPOINT,
    name="SharePoint",
    description="Microsoft SharePoint via Graph API",
    fields=[
        ConfigFieldDefinition(
            name="tenant_id",
            label="Tenant ID",
            required=True,
            placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            description="Azure AD Tenant ID"
        ),
        ConfigFieldDefinition(
            name="client_id",
            label="Client ID",
            required=True,
            placeholder="App Registration Client ID",
            description="Azure AD App Registration Client ID"
        ),
        ConfigFieldDefinition(
            name="client_secret",
            label="Client Secret",
            field_type="password",
            required=True,
            description="Azure AD App Registration Client Secret"
        ),
        ConfigFieldDefinition(
            name="site_id",
            label="Site ID",
            required=True,
            placeholder="your-site-id",
            description="SharePoint Site ID"
        ),
        ConfigFieldDefinition(
            name="drive_id",
            label="Drive ID",
            required=True,
            placeholder="your-drive-id",
            description="SharePoint Drive/Document Library ID"
        ),
        ConfigFieldDefinition(
            name="folder_id",
            label="Folder ID",
            required=False,
            placeholder="Optional folder ID",
            description="Optional: Specific folder to start from"
        ),
        ConfigFieldDefinition(
            name="tags_to_include",
            label="Tags to Include (JSON)",
            field_type="textarea",
            required=False,
            placeholder='[{"columnName": "Category", "columnValues": ["Value1", "Value2"]}]',
            description="JSON array of tag filters. Files must match ALL filters to be included."
        ),
        ConfigFieldDefinition(
            name="tags_to_exclude",
            label="Tags to Exclude (JSON)",
            field_type="textarea",
            required=False,
            placeholder='[{"columnName": "Category", "columnValues": ["Value1", "Value2"]}]',
            description="JSON array of tag filters. Files matching ANY filter will be excluded."
        ),
    ]
)

GOOGLE_DRIVE_SCHEMA = IntegrationSchema(
    type=IntegrationType.GOOGLE_DRIVE,
    name="Google Drive",
    description="Google Drive via Google Drive API",
    fields=[
        ConfigFieldDefinition(
            name="auth_type",
            label="Authentication Type",
            field_type="select",
            required=True,
            options=["service_account", "oauth2"],
            description="How to authenticate with Google"
        ),
        ConfigFieldDefinition(
            name="service_account_json",
            label="Service Account JSON",
            field_type="textarea",
            required=False,
            placeholder="Paste service account JSON or file path",
            description="Service account JSON content or path (for service_account auth)"
        ),
        ConfigFieldDefinition(
            name="access_token",
            label="Access Token",
            field_type="password",
            required=False,
            description="OAuth2 access token (for oauth2 auth)"
        ),
        ConfigFieldDefinition(
            name="refresh_token",
            label="Refresh Token",
            field_type="password",
            required=False,
            description="OAuth2 refresh token (for oauth2 auth)"
        ),
        ConfigFieldDefinition(
            name="client_id",
            label="OAuth Client ID",
            required=False,
            description="OAuth2 client ID (for oauth2 auth)"
        ),
        ConfigFieldDefinition(
            name="client_secret",
            label="OAuth Client Secret",
            field_type="password",
            required=False,
            description="OAuth2 client secret (for oauth2 auth)"
        ),
        ConfigFieldDefinition(
            name="folder_id",
            label="Root Folder ID",
            required=False,
            placeholder="Optional folder ID",
            description="Optional: Root folder to use (default: My Drive root)"
        ),
    ]
)

S3_SCHEMA = IntegrationSchema(
    type=IntegrationType.S3,
    name="Amazon S3",
    description="Amazon S3 or S3-compatible storage",
    fields=[
        ConfigFieldDefinition(
            name="access_key_id",
            label="Access Key ID",
            required=True,
            description="AWS Access Key ID"
        ),
        ConfigFieldDefinition(
            name="secret_access_key",
            label="Secret Access Key",
            field_type="password",
            required=True,
            description="AWS Secret Access Key"
        ),
        ConfigFieldDefinition(
            name="bucket",
            label="Bucket Name",
            required=True,
            placeholder="my-bucket",
            description="S3 bucket name"
        ),
        ConfigFieldDefinition(
            name="region",
            label="Region",
            required=False,
            placeholder="us-east-1",
            description="AWS region (default: us-east-1)"
        ),
        ConfigFieldDefinition(
            name="endpoint_url",
            label="Endpoint URL",
            required=False,
            placeholder="https://s3.amazonaws.com",
            description="Custom endpoint for S3-compatible services"
        ),
        ConfigFieldDefinition(
            name="prefix",
            label="Key Prefix",
            required=False,
            placeholder="folder/subfolder/",
            description="Optional key prefix (folder path)"
        ),
    ]
)

AZURE_BLOB_SCHEMA = IntegrationSchema(
    type=IntegrationType.AZURE_BLOB,
    name="Azure Blob Storage",
    description="Microsoft Azure Blob Storage",
    fields=[
        ConfigFieldDefinition(
            name="connection_string",
            label="Connection String",
            field_type="password",
            required=False,
            description="Azure Storage connection string (alternative to account credentials)"
        ),
        ConfigFieldDefinition(
            name="account_name",
            label="Storage Account Name",
            required=False,
            description="Azure Storage account name"
        ),
        ConfigFieldDefinition(
            name="account_key",
            label="Account Key",
            field_type="password",
            required=False,
            description="Azure Storage account key"
        ),
        ConfigFieldDefinition(
            name="container",
            label="Container Name",
            required=True,
            placeholder="my-container",
            description="Blob container name"
        ),
        ConfigFieldDefinition(
            name="prefix",
            label="Blob Prefix",
            required=False,
            placeholder="folder/subfolder/",
            description="Optional blob prefix (folder path)"
        ),
    ]
)

SFTP_SCHEMA = IntegrationSchema(
    type=IntegrationType.SFTP,
    name="SFTP",
    description="SSH File Transfer Protocol (SFTP) server",
    fields=[
        ConfigFieldDefinition(
            name="host",
            label="Host",
            required=True,
            placeholder="sftp.example.com",
            description="SFTP server hostname or IP address"
        ),
        ConfigFieldDefinition(
            name="port",
            label="Port",
            required=False,
            placeholder="22",
            description="SFTP port (default: 22)"
        ),
        ConfigFieldDefinition(
            name="username",
            label="Username",
            required=True,
            description="SFTP username"
        ),
        ConfigFieldDefinition(
            name="password",
            label="Password",
            field_type="password",
            required=False,
            description="SFTP password (if not using private key)"
        ),
        ConfigFieldDefinition(
            name="private_key",
            label="Private Key",
            field_type="textarea",
            required=False,
            placeholder="Paste private key content or file path",
            description="SSH private key content or path (alternative to password)"
        ),
        ConfigFieldDefinition(
            name="private_key_passphrase",
            label="Private Key Passphrase",
            field_type="password",
            required=False,
            description="Passphrase for encrypted private key"
        ),
        ConfigFieldDefinition(
            name="base_path",
            label="Base Path",
            required=False,
            placeholder="/home/user/files",
            description="Base directory path on SFTP server (default: /)"
        ),
    ]
)

# Registry of all schemas
INTEGRATION_SCHEMAS: dict[IntegrationType, IntegrationSchema] = {
    IntegrationType.LOCAL: LOCAL_SCHEMA,
    IntegrationType.SHAREPOINT: SHAREPOINT_SCHEMA,
    IntegrationType.GOOGLE_DRIVE: GOOGLE_DRIVE_SCHEMA,
    IntegrationType.S3: S3_SCHEMA,
    IntegrationType.AZURE_BLOB: AZURE_BLOB_SCHEMA,
    IntegrationType.SFTP: SFTP_SCHEMA,
}


class SavedConfiguration(BaseModel):
    """A saved source or destination configuration."""
    id: Optional[str] = Field(default=None, alias="_id", description="MongoDB ObjectId")
    name: str = Field(description="User-friendly name for this configuration")
    description: Optional[str] = Field(default=None)
    type: IntegrationType = Field(description="Integration type")
    config: dict[str, Any] = Field(default_factory=dict, description="Configuration values")
    is_source: bool = Field(default=True, description="True if source, False if destination")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_used_at: Optional[datetime] = Field(default=None)
    use_count: int = Field(default=0)
    
    class Config:
        populate_by_name = True
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat() if v else None
        }


class SavedConfigurationCreate(BaseModel):
    """Request to create a saved configuration."""
    name: str = Field(description="User-friendly name")
    description: Optional[str] = Field(default=None)
    type: IntegrationType = Field(description="Integration type")
    config: dict[str, Any] = Field(description="Configuration values")
    is_source: bool = Field(default=True)
    
    class Config:
        use_enum_values = True


class SavedConfigurationUpdate(BaseModel):
    """Request to update a saved configuration."""
    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    config: Optional[dict[str, Any]] = Field(default=None)


class JobFromConfigsRequest(BaseModel):
    """Request to create a job from saved configurations."""
    name: str = Field(description="Job name")
    description: Optional[str] = Field(default=None)
    source_config_id: str = Field(description="ID of saved source configuration")
    destination_config_id: str = Field(description="ID of saved destination configuration")
    source_extensions: list[str] = Field(default=['.pdf', '.docx', '.pptx', '.xlsx', '.csv'])
    source_folder: Optional[str] = Field(default=None)
    destination_folder: Optional[str] = Field(default=None)
    conversion_strategy: ConversionStrategy = Field(default=ConversionStrategy.FAST)
    batch_size: int = Field(default=4, description="Number of files to process concurrently")
    llm_settings: Optional[JobLLMSettings] = Field(default=None)
    schedule_cron: Optional[str] = Field(default=None)
    schedule_enabled: bool = Field(default=False)


class JobExecutionHistory(BaseModel):
    """Record of a single job execution (scheduled or manual)."""
    id: Optional[str] = Field(default=None, alias="_id", description="MongoDB ObjectId")
    job_id: str = Field(description="ID of the conversion job")
    job_name: str = Field(default="", description="Snapshot of the job name")
    trigger: str = Field(default="manual", description="'manual' or 'scheduled'")
    status: JobStatus = Field(default=JobStatus.PENDING)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = Field(default=None)
    total_files: int = Field(default=0)
    completed_files: int = Field(default=0)
    failed_files: int = Field(default=0)
    error_message: Optional[str] = Field(default=None)

    class Config:
        populate_by_name = True
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat() if v else None
        }
