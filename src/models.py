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

SITEMAP_SCHEMA = IntegrationSchema(
    type=IntegrationType.SITEMAP,
    name="Sitemap (Web)",
    description="Scrape pages listed in a sitemap.xml and convert them to Markdown",
    fields=[
        ConfigFieldDefinition(
            name="sitemap_url",
            label="Sitemap URL",
            required=True,
            placeholder="https://example.com/sitemap.xml",
            description="HTTP(S) URL (or local file path) of a sitemap.xml. Sitemap indexes are followed."
        ),
        ConfigFieldDefinition(
            name="filter_urls",
            label="Include URL Patterns",
            field_type="textarea",
            required=False,
            placeholder="^https://example\\.com/docs/\nblog/",
            description="One regex per line. If non-empty, a URL must match at least one to be included."
        ),
        ConfigFieldDefinition(
            name="exclude_urls",
            label="Exclude URL Patterns",
            field_type="textarea",
            required=False,
            placeholder="/tag/\n/author/",
            description="One regex per line. URLs matching any are skipped."
        ),
        ConfigFieldDefinition(
            name="content_selector",
            label="Content CSS Selector",
            required=False,
            placeholder="#main",
            description="Optional CSS selector to narrow content (e.g. '#main', 'main', 'article')."
        ),
        ConfigFieldDefinition(
            name="remove_selectors",
            label="Remove CSS Selectors",
            field_type="textarea",
            required=False,
            placeholder="nav\nheader\nfooter\naside\nscript\nstyle",
            description="One CSS selector per line. Defaults strip nav/header/footer/aside/script/style/form/iframe/noscript."
        ),
        ConfigFieldDefinition(
            name="requests_per_second",
            label="Requests per Second",
            required=False,
            placeholder="2",
            description="Politeness throttle (default: 2). Use 0 to disable."
        ),
        ConfigFieldDefinition(
            name="max_urls",
            label="Max URLs",
            required=False,
            placeholder="500",
            description="Optional cap on number of URLs to process."
        ),
        ConfigFieldDefinition(
            name="user_agent",
            label="User-Agent",
            required=False,
            placeholder="ws-mark-flow/1.0",
            description="HTTP User-Agent header sent with each request."
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
    IntegrationType.SITEMAP: SITEMAP_SCHEMA,
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


# ============== Ingestion (Vectorization) Models ==============

from .vectorstore.base import VectorStoreType  # noqa: E402


class EmbeddingSettings(BaseModel):
    """Per-job embedding configuration."""
    provider: str = Field(default="openai", description="Provider: openai, ollama, google")
    model: str = Field(default="text-embedding-3-small", description="Embedding model name")
    api_key: Optional[str] = Field(default=None, description="API key override (defaults to global LLM key)")
    base_url: Optional[str] = Field(default=None, description="Base URL override (e.g. Ollama endpoint)")
    dimensions: Optional[int] = Field(default=None, description="Optional dimensions hint (must match the vector store schema)")


class ChunkingSettings(BaseModel):
    """Per-job chunking configuration."""
    strategy: str = Field(default="markdown_headers", description="Strategy: fixed | recursive | markdown_headers")
    chunk_size: int = Field(default=1200, description="Max characters per chunk")
    chunk_overlap: int = Field(default=150, description="Characters of overlap between adjacent chunks")


class VectorStoreConfig(BaseModel):
    """Configuration for a vector store integration."""
    type: VectorStoreType = Field(description="Vector store type")
    config: dict[str, Any] = Field(default_factory=dict, description="Backend-specific configuration")
    namespace: str = Field(default="default", description="Logical partition (collection / namespace)")

    class Config:
        use_enum_values = True


class IngestionFileStatus(str, Enum):
    """Status of a single document during ingestion."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    UPSERTING = "upserting"
    EXTRACTING_GRAPH = "extracting_graph"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class IngestionFileResult(BaseModel):
    """Result of ingesting a single markdown document."""
    source_path: str = Field(description="Markdown path in source")
    doc_id: Optional[str] = Field(default=None)
    status: IngestionFileStatus = Field(default=IngestionFileStatus.PENDING)
    chunk_count: int = Field(default=0)
    entity_count: int = Field(default=0)
    relation_count: int = Field(default=0)
    error_message: Optional[str] = Field(default=None)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)


class IngestionStats(BaseModel):
    """Statistics for an ingestion job."""
    total_documents: int = Field(default=0)
    pending_documents: int = Field(default=0)
    completed_documents: int = Field(default=0)
    failed_documents: int = Field(default=0)
    skipped_documents: int = Field(default=0)
    total_chunks: int = Field(default=0)
    total_entities: int = Field(default=0)
    total_relations: int = Field(default=0)
    completion_percentage: float = Field(default=0.0)


class GraphOntologyRelation(BaseModel):
    type: str
    source: list[str] = Field(default_factory=list)
    target: list[str] = Field(default_factory=list)


class GraphOntology(BaseModel):
    node_labels: list[str] = Field(default_factory=list)
    relations: list[GraphOntologyRelation] = Field(default_factory=list)
    node_properties: dict[str, list[str]] = Field(default_factory=dict)


class GraphSettings(BaseModel):
    """Per-job graph extraction configuration."""
    enabled: bool = Field(default=False)
    mode: str = Field(default="lexical", description="lexical | schema_guided")
    ontology: Optional[GraphOntology] = Field(default=None)
    ontology_source: Optional[str] = Field(
        default=None,
        description="YAML/JSON string or filesystem path (alternative to ontology)",
    )
    llm_provider: Optional[str] = Field(default=None)
    llm_model: Optional[str] = Field(default=None)
    llm_api_key: Optional[str] = Field(default=None)
    llm_base_url: Optional[str] = Field(default=None)
    max_entities_per_chunk: int = Field(default=15)
    max_relations_per_chunk: int = Field(default=10)
    chunk_concurrency: int = Field(default=2)


class IngestionJob(BaseModel):
    """
    Vectorization job: read markdown from a SourceIntegration, chunk + embed,
    and upsert into a VectorStoreIntegration.
    """
    id: Optional[str] = Field(default=None, alias="_id")
    name: str
    description: Optional[str] = None

    source: IntegrationConfig = Field(description="Markdown source integration")
    vector_store: VectorStoreConfig = Field(description="Vector store integration")

    source_folder: Optional[str] = None
    source_extensions: list[str] = Field(default=[".md"])

    embedding: EmbeddingSettings = Field(default_factory=EmbeddingSettings)
    chunking: ChunkingSettings = Field(default_factory=ChunkingSettings)
    graph: GraphSettings = Field(default_factory=GraphSettings)

    batch_size: int = Field(default=4, description="Number of documents to process concurrently")
    delete_orphans: bool = Field(default=False, description="Remove vectors for documents missing from source")

    schedule_cron: Optional[str] = None
    schedule_enabled: bool = False

    status: JobStatus = Field(default=JobStatus.PENDING)
    stats: IngestionStats = Field(default_factory=IngestionStats)
    file_results: list[IngestionFileResult] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

    class Config:
        populate_by_name = True
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat() if v else None,
        }

    def update_stats(self) -> None:
        total = len(self.file_results)
        completed = sum(1 for f in self.file_results if f.status == IngestionFileStatus.COMPLETED)
        skipped = sum(1 for f in self.file_results if f.status == IngestionFileStatus.SKIPPED)
        failed = sum(1 for f in self.file_results if f.status == IngestionFileStatus.FAILED)
        pending = sum(1 for f in self.file_results if f.status == IngestionFileStatus.PENDING)
        self.stats = IngestionStats(
            total_documents=total,
            pending_documents=pending,
            completed_documents=completed + skipped,
            failed_documents=failed,
            skipped_documents=skipped,
            total_chunks=sum(f.chunk_count for f in self.file_results),
            total_entities=sum(getattr(f, "entity_count", 0) or 0 for f in self.file_results),
            total_relations=sum(getattr(f, "relation_count", 0) or 0 for f in self.file_results),
            completion_percentage=round(((completed + skipped) / total) * 100, 2) if total else 0.0,
        )
        self.updated_at = datetime.utcnow()


class IngestionJobCreateRequest(BaseModel):
    """Request to create an ingestion job."""
    name: str
    description: Optional[str] = None
    source: IntegrationConfig
    vector_store: VectorStoreConfig
    source_folder: Optional[str] = None
    source_extensions: list[str] = Field(default=[".md"])
    embedding: EmbeddingSettings = Field(default_factory=EmbeddingSettings)
    chunking: ChunkingSettings = Field(default_factory=ChunkingSettings)
    graph: GraphSettings = Field(default_factory=GraphSettings)
    batch_size: int = 4
    delete_orphans: bool = False
    schedule_cron: Optional[str] = None
    schedule_enabled: bool = False


class IngestionJobUpdateRequest(BaseModel):
    """Partial update for an ingestion job."""
    name: Optional[str] = None
    description: Optional[str] = None
    source: Optional[IntegrationConfig] = None
    vector_store: Optional[VectorStoreConfig] = None
    source_folder: Optional[str] = None
    source_extensions: Optional[list[str]] = None
    embedding: Optional[EmbeddingSettings] = None
    chunking: Optional[ChunkingSettings] = None
    graph: Optional[GraphSettings] = None
    batch_size: Optional[int] = None
    delete_orphans: Optional[bool] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None


class IngestionAnalysis(BaseModel):
    """Diff between source markdown and indexed documents."""
    source_documents: int = 0
    indexed_documents: int = 0
    to_ingest: list[dict] = Field(default_factory=list)
    up_to_date: list[dict] = Field(default_factory=list)
    orphans: list[dict] = Field(default_factory=list, description="Indexed but missing from source")
    completion_percentage: float = 0.0


# Schemas describing vector store config fields (for UI rendering).
CHROMA_VS_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,  # placeholder; UI uses .type only as opaque key
    name="Chroma (filesystem)",
    description="Embedded Chroma vector store persisted on local disk",
    fields=[
        ConfigFieldDefinition(
            name="path", label="Storage Path", required=True,
            placeholder="./.data/chroma",
            description="Filesystem path where the vector database is persisted",
        ),
    ],
)

PGVECTOR_VS_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,  # placeholder
    name="pgvector (PostgreSQL)",
    description="Remote PostgreSQL with pgvector extension",
    fields=[
        ConfigFieldDefinition(
            name="dsn", label="DSN", required=True, field_type="password",
            placeholder="postgresql://user:pass@host:5432/db",
            description="PostgreSQL connection string",
        ),
        ConfigFieldDefinition(
            name="table", label="Table", required=False,
            placeholder="ws_mark_flow_chunks",
            description="Table name (auto-created on first use)",
        ),
        ConfigFieldDefinition(
            name="embedding_dim", label="Embedding Dim", required=True,
            placeholder="1536",
            description="Vector dimensionality (must match the embedding model)",
        ),
    ],
)


MONGO_ATLAS_VS_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,  # placeholder
    name="MongoDB Atlas Vector Search",
    description="MongoDB Atlas (or Atlas Local) with $vectorSearch",
    fields=[
        ConfigFieldDefinition(
            name="uri", label="MongoDB URI", required=True, field_type="password",
            placeholder="mongodb+srv://user:pass@cluster.mongodb.net",
            description="MongoDB connection string",
        ),
        ConfigFieldDefinition(
            name="database", label="Database", required=True,
            placeholder="ws_mark_flow",
            description="Database name",
        ),
        ConfigFieldDefinition(
            name="collection", label="Collection", required=False,
            placeholder="ws_mark_flow_chunks",
            description="Collection name (auto-created on first insert)",
        ),
        ConfigFieldDefinition(
            name="embedding_dim", label="Embedding Dim", required=True,
            placeholder="1536",
            description="Vector dimensionality (must match the embedding model)",
        ),
        ConfigFieldDefinition(
            name="index_name", label="Vector Index Name", required=False,
            placeholder="vector_index",
            description="Name of the Atlas vector search index",
        ),
        ConfigFieldDefinition(
            name="similarity", label="Similarity", required=False,
            field_type="select",
            options=["cosine", "euclidean", "dotProduct"],
            placeholder="cosine",
            description="Similarity metric for the vector index (default: cosine)",
        ),
    ],
)


NEO4J_VS_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,  # placeholder
    name="Neo4j (vector + graph)",
    description="Neo4j 5.11+ HNSW vector index with optional knowledge graph",
    fields=[
        ConfigFieldDefinition(
            name="uri", label="Bolt URI", required=True,
            placeholder="bolt://localhost:7687",
            description="bolt://host:7687 or neo4j+s://aura-host",
        ),
        ConfigFieldDefinition(
            name="username", label="Username", required=True,
            placeholder="neo4j",
        ),
        ConfigFieldDefinition(
            name="password", label="Password", required=True,
            field_type="password",
        ),
        ConfigFieldDefinition(
            name="database", label="Database", required=False,
            placeholder="neo4j",
            description="Neo4j database name (default: neo4j)",
        ),
        ConfigFieldDefinition(
            name="embedding_dim", label="Embedding Dim", required=True,
            placeholder="1536",
            description="Vector dimensionality (must match the embedding model)",
        ),
        ConfigFieldDefinition(
            name="index_name", label="Vector Index Name", required=False,
            placeholder="ws_mark_flow_chunk_embeddings",
            description="Name of the Neo4j vector index",
        ),
        ConfigFieldDefinition(
            name="similarity", label="Similarity", required=False,
            field_type="select",
            options=["cosine", "euclidean"],
            placeholder="cosine",
            description="Similarity metric for the vector index (default: cosine)",
        ),
    ],
)


REDIS_VS_SCHEMA = IntegrationSchema(
    type=IntegrationType.LOCAL,  # placeholder
    name="Redis (RediSearch)",
    description="Redis Stack with RediSearch vector index (HNSW or FLAT)",
    fields=[
        ConfigFieldDefinition(
            name="url", label="Redis URL", required=True, field_type="password",
            placeholder="redis://:password@localhost:6379/0",
            description="redis:// or rediss:// connection URL",
        ),
        ConfigFieldDefinition(
            name="index_name", label="Index Name", required=False,
            placeholder="ws_mark_flow_chunks_idx",
            description="RediSearch index name (auto-created on first connect)",
        ),
        ConfigFieldDefinition(
            name="key_prefix", label="Key Prefix", required=False,
            placeholder="ws_mark_flow_chunk",
            description="Prefix for chunk hash keys",
        ),
        ConfigFieldDefinition(
            name="embedding_dim", label="Embedding Dim", required=True,
            placeholder="1536",
            description="Vector dimensionality (must match the embedding model)",
        ),
        ConfigFieldDefinition(
            name="similarity", label="Distance Metric", required=False,
            field_type="select",
            options=["cosine", "l2", "ip"],
            placeholder="cosine",
            description="Distance metric (default: cosine)",
        ),
        ConfigFieldDefinition(
            name="algorithm", label="Index Algorithm", required=False,
            field_type="select",
            options=["HNSW", "FLAT"],
            placeholder="HNSW",
            description="HNSW = approximate, fast; FLAT = exact, slower at scale",
        ),
    ],
)


VECTOR_STORE_SCHEMAS: dict[str, IntegrationSchema] = {
    VectorStoreType.CHROMA.value: CHROMA_VS_SCHEMA,
    VectorStoreType.PGVECTOR.value: PGVECTOR_VS_SCHEMA,
    VectorStoreType.MONGO_ATLAS.value: MONGO_ATLAS_VS_SCHEMA,
    VectorStoreType.NEO4J.value: NEO4J_VS_SCHEMA,
    VectorStoreType.REDIS.value: REDIS_VS_SCHEMA,
}
