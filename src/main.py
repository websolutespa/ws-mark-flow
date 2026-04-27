"""
FastAPI application for the Ws-Mark-Flow AI Converter.
Provides REST API for managing conversion jobs.
"""
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from fastapi import Depends, FastAPI, HTTPException, BackgroundTasks, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.datastores.mongodb import MongoDBDataStore
from apscheduler.triggers.cron import CronTrigger

from .config import get_settings
from .models import (
    ConversionJob,
    ConversionAnalysis,
    JobStatus,
    JobCreateRequest,
    JobUpdateRequest,
    IntegrationConfig,
    SavedConfiguration,
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    JobFromConfigsRequest,
    JobLLMSettings,
    JobExecutionHistory,
    IntegrationSchema,
    INTEGRATION_SCHEMAS,
)
from .storage import JobStorage, ConfigurationStorage, ExecutionHistoryStorage
from .converter import ConversionService
from .factory import (
    create_source,
    create_destination,
    get_supported_sources,
    get_supported_destinations,
)
from .integration import IntegrationType, CONVERTIBLE_EXTENSIONS
from .ui import get_ui_html

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
settings = get_settings()
storage: Optional[JobStorage] = None
config_storage: Optional[ConfigurationStorage] = None
history_storage: Optional[ExecutionHistoryStorage] = None
scheduler: Optional[AsyncScheduler] = None
converter = ConversionService(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global storage, config_storage, history_storage, scheduler
            
    # Initialize job storage
    storage = JobStorage(settings.mongodb_uri, settings.mongodb_database)
    connected = await storage.connect()
    
    if not connected:
        logger.warning("MongoDB not available - job persistence disabled")
        storage = None
    
    # Initialize configuration storage
    config_storage = ConfigurationStorage(settings.mongodb_uri, settings.mongodb_database)
    config_connected = await config_storage.connect()
    
    if not config_connected:
        logger.warning("Configuration storage not available")
        config_storage = None

    # Initialize execution history storage
    history_storage = ExecutionHistoryStorage()
    try:
        await history_storage.connect(settings.mongodb_uri, settings.mongodb_database)
    except Exception as e:
        logger.warning(f"Execution history storage not available: {e}")
        history_storage = None

    # Initialize APScheduler with MongoDB data store
    try:
        data_store = MongoDBDataStore(settings.mongodb_uri, database=settings.mongodb_database)
        scheduler = AsyncScheduler(data_store=data_store)
        await scheduler.__aenter__()

        # Sync schedules BEFORE starting the scheduler loop so that any stale
        # references persisted from a previous run (e.g. under a different
        # module path) are purged before they can fire and crash the loop.
        if storage:
            await _sync_all_schedules()

        await scheduler.start_in_background()
        logger.info("APScheduler started with MongoDB data store")

        # Run history cleanup on startup
        if history_storage:
            deleted = await history_storage.cleanup_old(days=30)
            if deleted:
                logger.info(f"Cleaned up {deleted} old execution history records")
    except Exception as e:
        logger.warning(f"APScheduler not available: {e}")
        scheduler = None
    
    yield
    
    # Cleanup
    if scheduler:
        try:
            await scheduler.__aexit__(None, None, None)
        except Exception:
            pass
    if storage:
        await storage.disconnect()
    if config_storage:
        await config_storage.disconnect()
    converter.cleanup()


async def _sync_all_schedules():
    """Sync all schedule-enabled jobs with APScheduler on startup.

    Also purges stale/broken schedule references that may have been persisted
    by a previous run under a different module path (e.g. `src.app:...`
    when running from source vs. `ws_mark_flow.app:...` when running the
    installed package inside the Docker image). Without this, APScheduler
    would crash on the first tick with `ModuleNotFoundError: No module
    named 'src'` (or similar).
    """
    if storage is None or scheduler is None:
        return

    # 1) Drop any schedules whose stored callable can no longer be imported.
    try:
        existing = await scheduler.get_schedules()
    except Exception as e:
        logger.warning(f"Could not list existing schedules: {e}")
        existing = []

    for sched in existing:
        task_ref = getattr(sched, "task_id", None)
        if not task_ref or ":" not in str(task_ref):
            continue
        module_name = str(task_ref).split(":", 1)[0]
        try:
            __import__(module_name)
        except Exception:
            try:
                await scheduler.remove_schedule(sched.id)
                logger.warning(
                    f"Removed stale schedule {sched.id!r} with unresolvable "
                    f"reference {task_ref!r}"
                )
            except Exception as e:
                logger.error(f"Failed to remove stale schedule {sched.id}: {e}")

    # 2) Re-create schedules from the jobs collection (source of truth).
    jobs = await storage.list_jobs(limit=1000, offset=0)
    for job in jobs:
        if job.schedule_enabled and job.schedule_cron:
            try:
                await _upsert_schedule(str(job.id), job.schedule_cron)
            except Exception as e:
                logger.error(f"Failed to sync schedule for job {job.id}: {e}")


async def _upsert_schedule(job_id: str, cron_expression: str):
    """Add or replace a cron schedule for a job in APScheduler."""
    if scheduler is None:
        return
    trigger = CronTrigger.from_crontab(cron_expression)
    await scheduler.add_schedule(
        _scheduled_job_runner,
        trigger,
        id=f"job-{job_id}",
        args=[job_id],
        conflict_policy=ConflictPolicy.replace,
    )
    logger.info(f"Schedule upserted for job {job_id}: {cron_expression}")


async def _remove_schedule(job_id: str):
    """Remove a cron schedule from APScheduler."""
    if scheduler is None:
        return
    try:
        await scheduler.remove_schedule(f"job-{job_id}")
        logger.info(f"Schedule removed for job {job_id}")
    except Exception:
        pass  # schedule may not exist


async def _scheduled_job_runner(job_id: str):
    """Executed by APScheduler when a cron trigger fires."""
    if storage is None:
        return

    job = await storage.get_job(job_id)
    if job is None:
        logger.warning(f"Scheduled run: job {job_id} not found, removing schedule")
        await _remove_schedule(job_id)
        return

    if job.status == JobStatus.RUNNING:
        logger.info(f"Scheduled run: job {job_id} already running, skipping")
        return

    # Create execution history record
    record = JobExecutionHistory(
        job_id=job_id,
        job_name=job.name,
        trigger="scheduled",
        status=JobStatus.RUNNING,
        started_at=datetime.utcnow(),
    )
    record_id = None
    if history_storage:
        record_id = await history_storage.save(record)

    try:
        await storage.update_job_status(job_id, JobStatus.PENDING)
        await run_job_background(job_id)

        # Re-read job to capture final status
        updated = await storage.get_job(job_id)
        if history_storage and record_id and updated:
            await history_storage.update(
                record_id,
                status=updated.status,
                completed_at=datetime.utcnow(),
                total_files=updated.stats.total_files if updated.stats else 0,
                completed_files=updated.stats.completed_files if updated.stats else 0,
                failed_files=updated.stats.failed_files if updated.stats else 0,
                error_message=updated.error_message,
            )
    except Exception as e:
        logger.error(f"Scheduled run failed for job {job_id}: {e}")
        if history_storage and record_id:
            await history_storage.update(
                record_id,
                status=JobStatus.FAILED,
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )



# ============== Basic Auth ==============

http_basic = HTTPBasic(auto_error=True)


def verify_credentials(credentials: HTTPBasicCredentials = Depends(http_basic)) -> str:
    """Validate HTTP Basic credentials against configured values.

    Auth is bypassed entirely when AUTH_PASSWORD is empty (default dev behaviour).
    """
    password = settings.auth_password
    if not password:
        return credentials.username

    valid_username = secrets.compare_digest(
        credentials.username.encode(), settings.auth_username.encode()
    )
    valid_password = secrets.compare_digest(
        credentials.password.encode(), password.encode()
    )
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Convert files from various sources to Markdown and upload to destinations",
    lifespan=lifespan,
    dependencies=[Depends(verify_credentials)],
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== Health & Info Endpoints ==============

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "mongodb": storage is not None
    }


@app.get("/info")
async def get_info():
    import torch
    """Get application info and supported integrations."""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "supported_sources": get_supported_sources(),
        "supported_destinations": get_supported_destinations(),
        "convertible_extensions": CONVERTIBLE_EXTENSIONS,
        "torch": {
            "version": torch.__version__,
            "cuda_available": torch.cuda.is_available(),
            "cuda_version": torch.version.cuda if torch.cuda.is_available() else None,
            "gpu_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
        }
    }


# ============== Job Management Endpoints ==============

@app.post("/jobs", response_model=ConversionJob)
async def create_job(request: JobCreateRequest):
    """Create a new conversion job."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.create_job(request)

    # Register schedule if enabled
    if job.schedule_enabled and job.schedule_cron:
        try:
            await _upsert_schedule(str(job.id), job.schedule_cron)
        except Exception as e:
            logger.error(f"Failed to create schedule for new job {job.id}: {e}")

    return job


@app.get("/jobs", response_model=list[ConversionJob])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """List all conversion jobs."""
    if storage is None:
        return []
    
    status_enum = JobStatus(status) if status else None
    jobs = await storage.list_jobs(status=status_enum, limit=limit, offset=offset)
    return jobs


@app.get("/jobs/{job_id}", response_model=ConversionJob)
async def get_job(job_id: str):
    """Get a specific job by ID."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.patch("/jobs/{job_id}", response_model=ConversionJob)
async def update_job(job_id: str, update: JobUpdateRequest):
    """Update a job's metadata."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.update_job(job_id, update)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Sync schedule if schedule fields were touched
    if update.schedule_enabled is not None or update.schedule_cron is not None:
        if job.schedule_enabled and job.schedule_cron:
            try:
                await _upsert_schedule(str(job.id), job.schedule_cron)
            except Exception as e:
                logger.error(f"Failed to update schedule for job {job.id}: {e}")
        else:
            await _remove_schedule(str(job.id))

    return job


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    await _remove_schedule(job_id)
    deleted = await storage.delete_job(job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"message": "Job deleted"}


# ============== Analysis Endpoints ==============

@app.post("/analyze")
async def analyze_conversion(
    source: IntegrationConfig,
    destination: IntegrationConfig,
    source_extensions: list[str] = Query(default=[".pdf", ".docx", ".pptx"]),
    source_folder: Optional[str] = None,
    destination_folder: Optional[str] = None
) -> ConversionAnalysis:
    """
    Analyze what files need to be converted between source and destination.
    Returns list of files to convert and completion percentage.
    """
    try:
        # Create integrations
        source_type = IntegrationType(source.type)
        dest_type = IntegrationType(destination.type)
        
        source_integration = create_source(source_type, source.config)
        dest_integration = create_destination(dest_type, destination.config)
        
        # Connect and analyze
        async with source_integration, dest_integration:
            analysis = await converter.analyze_conversion(
                source_integration,
                dest_integration,
                source_extensions,
                source_folder,
                destination_folder
            )
        
        return analysis
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/{job_id}/analyze", response_model=ConversionAnalysis)
async def analyze_job(job_id: str):
    """Analyze what files need to be converted for a specific job."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    try:
        source_type = IntegrationType(job.source.type)
        dest_type = IntegrationType(job.destination.type)
        
        source_integration = create_source(source_type, job.source.config)
        dest_integration = create_destination(dest_type, job.destination.config)
        
        async with source_integration, dest_integration:
            analysis = await converter.analyze_conversion(
                source_integration,
                dest_integration,
                job.source_extensions,
                job.source_folder,
                job.destination_folder
            )
        
        return analysis
        
    except Exception as e:
        logger.error(f"Job analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============== Conversion Endpoints ==============

async def run_job_background(job_id: str, history_record_id: Optional[str] = None):
    """Background task to run a conversion job."""
    if storage is None:
        return
    
    job = await storage.get_job(job_id)
    if job is None:
        return
    
    try:
        source_type = IntegrationType(job.source.type)
        dest_type = IntegrationType(job.destination.type)
        
        source_integration = create_source(source_type, job.source.config)
        dest_integration = create_destination(dest_type, job.destination.config)
        
        async def progress_callback(updated_job, current, total):
            """Save progress to storage."""
            await storage.save_job(updated_job)
        
        async with source_integration, dest_integration:
            updated_job = await converter.run_conversion(
                job,
                source_integration,
                dest_integration,
                progress_callback=progress_callback
            )
        
        await storage.save_job(updated_job)

        # Update execution history for manual runs
        if history_storage and history_record_id:
            await history_storage.update(
                history_record_id,
                status=updated_job.status,
                completed_at=datetime.utcnow(),
                total_files=updated_job.stats.total_files if updated_job.stats else 0,
                completed_files=updated_job.stats.completed_files if updated_job.stats else 0,
                failed_files=updated_job.stats.failed_files if updated_job.stats else 0,
                error_message=updated_job.error_message,
            )
        
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        await storage.update_job_status(job_id, JobStatus.FAILED, str(e))
        if history_storage and history_record_id:
            await history_storage.update(
                history_record_id,
                status=JobStatus.FAILED,
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )


@app.post("/jobs/{job_id}/run")
async def run_job(job_id: str, background_tasks: BackgroundTasks):
    """Start running a conversion job in the background."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status == JobStatus.RUNNING:
        raise HTTPException(status_code=400, detail="Job is already running")
    
    # Update status to pending
    await storage.update_job_status(job_id, JobStatus.PENDING)

    # Create execution history record
    record_id = None
    if history_storage:
        record = JobExecutionHistory(
            job_id=job_id,
            job_name=job.name,
            trigger="manual",
            status=JobStatus.RUNNING,
            started_at=datetime.utcnow(),
        )
        record_id = await history_storage.save(record)
    
    # Run in background
    background_tasks.add_task(run_job_background, job_id, record_id)
    
    return {"message": "Job started", "job_id": job_id}


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    job = await storage.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status != JobStatus.RUNNING:
        raise HTTPException(status_code=400, detail="Job is not running")
    
    await storage.update_job_status(job_id, JobStatus.CANCELLED)
    return {"message": "Job cancelled"}


# ============== Source/Destination Testing ==============

@app.post("/test/source")
async def test_source_connection(source: IntegrationConfig):
    """Test connection to a source and list files."""
    try:
        source_type = IntegrationType(source.type)
        source_integration = create_source(source_type, source.config)
        
        async with source_integration:
            files = await source_integration.list_files(
                extensions=CONVERTIBLE_EXTENSIONS[:5]  # Limit for testing
            )
        
        return {
            "success": True,
            "message": f"Successfully connected. Found {len(files)} files.",
            "sample_files": [
                {"name": f.name, "path": f.path, "modified_at": f.modified_at.isoformat()}
                for f in files[:10]
            ]
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "sample_files": []
        }


@app.post("/test/destination")
async def test_destination_connection(destination: IntegrationConfig):
    """Test connection to a destination and list existing markdown files."""
    try:
        dest_type = IntegrationType(destination.type)
        dest_integration = create_destination(dest_type, destination.config)
        
        async with dest_integration:
            files = await dest_integration.list_files(extensions=[".md"])
        
        return {
            "success": True,
            "message": f"Successfully connected. Found {len(files)} markdown files.",
            "sample_files": [
                {"name": f.name, "path": f.path, "modified_at": f.modified_at.isoformat()}
                for f in files[:10]
            ]
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "sample_files": []
        }


# ============== Configuration Management Endpoints ==============

@app.get("/schemas", response_model=list[IntegrationSchema])
async def get_integration_schemas():
    """Get all integration schemas with field definitions for UI rendering."""
    return list(INTEGRATION_SCHEMAS.values())


@app.get("/schemas/{integration_type}", response_model=IntegrationSchema)
async def get_integration_schema(integration_type: str):
    """Get schema for a specific integration type."""
    try:
        int_type = IntegrationType(integration_type)
        schema = INTEGRATION_SCHEMAS.get(int_type)
        if schema is None:
            raise HTTPException(status_code=404, detail=f"Schema not found for {integration_type}")
        return schema
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid integration type: {integration_type}")


@app.post("/configurations", response_model=SavedConfiguration)
async def create_configuration(request: SavedConfigurationCreate):
    """Create a new saved configuration."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")
    
    config = await config_storage.create(request)
    return config


@app.get("/configurations", response_model=list[SavedConfiguration])
async def list_configurations(
    is_source: Optional[bool] = Query(None, description="Filter by source (true) or destination (false)"),
    integration_type: Optional[str] = Query(None, description="Filter by integration type"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List all saved configurations."""
    if config_storage is None:
        return []
    
    configs = await config_storage.list_all(
        is_source=is_source,
        integration_type=integration_type,
        limit=limit,
        offset=offset
    )
    return configs


@app.get("/configurations/sources", response_model=list[SavedConfiguration])
async def list_source_configurations(
    integration_type: Optional[str] = Query(None)
):
    """List all source configurations."""
    if config_storage is None:
        return []
    return await config_storage.list_sources(integration_type=integration_type)


@app.get("/configurations/destinations", response_model=list[SavedConfiguration])
async def list_destination_configurations(
    integration_type: Optional[str] = Query(None)
):
    """List all destination configurations."""
    if config_storage is None:
        return []
    return await config_storage.list_destinations(integration_type=integration_type)


# ============== Configuration Import / Export ==============

@app.get("/configurations/export")
async def export_configurations(
    is_source: Optional[bool] = Query(None, description="True for sources, False for destinations, omit for all")
):
    """Export configurations as a JSON array (suitable for download and later import)."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")

    configs = await config_storage.list_all(is_source=is_source, limit=1000, offset=0)
    return [
        {
            "name": c.name,
            "description": c.description,
            "type": c.type,
            "config": c.config,
            "is_source": c.is_source,
        }
        for c in configs
    ]


class _ConfigImportItem(BaseModel):
    name: str
    description: Optional[str] = None
    type: str
    config: dict
    is_source: bool = True


@app.post("/configurations/import")
async def import_configurations(items: list[_ConfigImportItem]):
    """
    Import configurations from a JSON array.
    Configurations whose name already exists (for the same is_source value) are skipped.
    Returns counts of created, skipped, and any per-item errors.
    """
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")

    existing = await config_storage.list_all(limit=10000, offset=0)
    existing_keys = {(c.name, c.is_source) for c in existing}

    created = 0
    skipped = 0
    errors: list[dict] = []

    for item in items:
        if (item.name, item.is_source) in existing_keys:
            skipped += 1
            continue
        try:
            int_type = IntegrationType(item.type)
            await config_storage.create(
                SavedConfigurationCreate(
                    name=item.name,
                    description=item.description,
                    type=int_type,
                    config=item.config,
                    is_source=item.is_source,
                )
            )
            existing_keys.add((item.name, item.is_source))
            created += 1
        except Exception as e:
            errors.append({"name": item.name, "error": str(e)})

    return {"created": created, "skipped": skipped, "errors": errors}


@app.get("/configurations/{config_id}", response_model=SavedConfiguration)
async def get_configuration(config_id: str):
    """Get a specific configuration by ID."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")
    
    config = await config_storage.get(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Configuration not found")
    return config


@app.patch("/configurations/{config_id}", response_model=SavedConfiguration)
async def update_configuration(config_id: str, update: SavedConfigurationUpdate):
    """Update a configuration."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")
    
    config = await config_storage.update(config_id, update)
    if config is None:
        raise HTTPException(status_code=404, detail="Configuration not found")
    return config


@app.delete("/configurations/{config_id}")
async def delete_configuration(config_id: str):
    """Delete a configuration."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")
    
    deleted = await config_storage.delete(config_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Configuration not found")
    return {"message": "Configuration deleted"}


@app.post("/configurations/{config_id}/test")
async def test_configuration(config_id: str):
    """Test a saved configuration by connecting and listing files."""
    if config_storage is None:
        raise HTTPException(status_code=503, detail="Configuration storage not available")
    
    config = await config_storage.get(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Configuration not found")
    
    try:
        int_type = IntegrationType(config.type)
        
        if config.is_source:
            integration = create_source(int_type, config.config)
            async with integration:
                files = await integration.list_files(extensions=CONVERTIBLE_EXTENSIONS[:5])
            return {
                "success": True,
                "message": f"Successfully connected. Found {len(files)} files.",
                "sample_files": [
                    {"name": f.name, "path": f.path, "modified_at": f.modified_at.isoformat()}
                    for f in files[:10]
                ]
            }
        else:
            integration = create_destination(int_type, config.config)
            async with integration:
                files = await integration.list_files(extensions=[".md"])
            return {
                "success": True,
                "message": f"Successfully connected. Found {len(files)} markdown files.",
                "sample_files": [
                    {"name": f.name, "path": f.path, "modified_at": f.modified_at.isoformat()}
                    for f in files[:10]
                ]
            }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "sample_files": []
        }


# ============== Job from Configurations ==============

@app.post("/jobs/from-configs", response_model=ConversionJob)
async def create_job_from_configurations(request: JobFromConfigsRequest):
    """Create a new job using saved source and destination configurations."""
    if storage is None or config_storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    
    # Get source configuration
    source_config = await config_storage.get(request.source_config_id)
    if source_config is None:
        raise HTTPException(status_code=404, detail="Source configuration not found")
    if not source_config.is_source:
        raise HTTPException(status_code=400, detail="Selected configuration is not a source")
    
    # Get destination configuration
    dest_config = await config_storage.get(request.destination_config_id)
    if dest_config is None:
        raise HTTPException(status_code=404, detail="Destination configuration not found")
    if dest_config.is_source:
        raise HTTPException(status_code=400, detail="Selected configuration is not a destination")
    
    # Create job
    job_request = JobCreateRequest(
        name=request.name,
        description=request.description,
        source=IntegrationConfig(type=source_config.type, config=source_config.config),
        destination=IntegrationConfig(type=dest_config.type, config=dest_config.config),
        source_extensions=request.source_extensions,
        source_folder=request.source_folder,
        destination_folder=request.destination_folder,
        conversion_strategy=request.conversion_strategy,
        batch_size=request.batch_size,
        llm_settings=request.llm_settings,
        schedule_cron=request.schedule_cron,
        schedule_enabled=request.schedule_enabled,
    )
    
    job = await storage.create_job(job_request)

    # Register schedule if enabled
    if job.schedule_enabled and job.schedule_cron:
        try:
            await _upsert_schedule(str(job.id), job.schedule_cron)
        except Exception as e:
            logger.error(f"Failed to create schedule for new job {job.id}: {e}")
    
    # Increment use counts
    await config_storage.increment_use_count(request.source_config_id)
    await config_storage.increment_use_count(request.destination_config_id)
    
    return job


# ============== Schedule & Execution History Endpoints ==============

class _ScheduleUpdate(BaseModel):
    schedule_cron: Optional[str] = Field(default=None, description="Cron expression (5-part)")
    schedule_enabled: Optional[bool] = Field(default=None)


@app.put("/jobs/{job_id}/schedule")
async def update_job_schedule(job_id: str, body: _ScheduleUpdate):
    """Enable, disable, or change the cron schedule for a job."""
    if storage is None:
        raise HTTPException(status_code=503, detail="Storage not available")

    job = await storage.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    update_fields = JobUpdateRequest()
    if body.schedule_cron is not None:
        update_fields.schedule_cron = body.schedule_cron
    if body.schedule_enabled is not None:
        update_fields.schedule_enabled = body.schedule_enabled

    updated = await storage.update_job(job_id, update_fields)
    if updated is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if updated.schedule_enabled and updated.schedule_cron:
        try:
            await _upsert_schedule(job_id, updated.schedule_cron)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron expression: {e}")
    else:
        await _remove_schedule(job_id)

    return {
        "job_id": job_id,
        "schedule_cron": updated.schedule_cron,
        "schedule_enabled": updated.schedule_enabled,
    }


@app.get("/jobs/{job_id}/history")
async def get_job_history(job_id: str, limit: int = Query(50, ge=1, le=200)):
    """Get execution history for a specific job."""
    if history_storage is None:
        return []
    return await history_storage.list_by_job(job_id, limit=limit)


@app.get("/history")
async def get_recent_history(limit: int = Query(100, ge=1, le=500)):
    """Get recent execution history across all jobs."""
    if history_storage is None:
        return []
    return await history_storage.list_recent(limit=limit)


@app.delete("/history/cleanup")
async def cleanup_history(days: int = Query(30, ge=1, le=365)):
    """Delete execution history older than N days."""
    if history_storage is None:
        raise HTTPException(status_code=503, detail="History storage not available")
    deleted = await history_storage.cleanup_old(days=days)
    return {"deleted": deleted, "days": days}


# ============== Settings Endpoints ==============

@app.get("/settings/llm")
async def get_llm_settings():
    """Get current LLM configuration (API key masked)."""
    return {
        "llm_provider": settings.llm_provider,
        "llm_model": settings.llm_model,
        "llm_base_url": settings.llm_base_url,
        "llm_api_key_set": bool(settings.llm_api_key),
    }

class _LLMSettingsUpdate(BaseModel):
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_base_url: Optional[str] = None
    llm_api_key: Optional[str] = None


@app.put("/settings/llm")
async def update_llm_settings(body: _LLMSettingsUpdate):
    """Update LLM settings at runtime."""
    if body.llm_provider is not None:
        settings.llm_provider = body.llm_provider
    if body.llm_model is not None:
        settings.llm_model = body.llm_model
    if body.llm_base_url is not None:
        settings.llm_base_url = body.llm_base_url
    if body.llm_api_key is not None:
        settings.llm_api_key = body.llm_api_key
    return await get_llm_settings()


# ============== UI Endpoints ==============

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    """Serve the main UI."""
    return get_ui_html()
