"""
MongoDB storage service for conversion jobs.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional
from bson import ObjectId

from pymongo import AsyncMongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from .models import (
    ConversionJob, JobStatus, JobCreateRequest, JobUpdateRequest,
    SavedConfiguration, SavedConfigurationCreate, SavedConfigurationUpdate,
    JobExecutionHistory
)

logger = logging.getLogger(__name__)


class JobStorage:
    """
    MongoDB-based storage for conversion jobs.
    Provides CRUD operations and query methods for jobs.
    """
    
    COLLECTION_NAME = "conversion_jobs"
    
    def __init__(self, mongodb_uri: str, database_name: str = "converter"):
        """
        Initialize job storage.
        
        Args:
            mongodb_uri: MongoDB connection URI
            database_name: Database name to use
        """
        self._client: Optional[AsyncMongoClient] = None
        self._db: Optional[Database] = None
        self._mongodb_uri = mongodb_uri
        self._database_name = database_name
    
    async def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            self._client = AsyncMongoClient(self._mongodb_uri)
            self._db = self._client[self._database_name]
            
            # Test connection
            await self._client.admin.command('ping')
            
            # Create indexes
            await self._create_indexes()
            
            logger.info(f"Connected to MongoDB: {self._database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
    
    async def _create_indexes(self) -> None:
        """Create necessary indexes for performance."""
        collection = self._get_collection()
        
        # Index on status for filtering
        await collection.create_index("status")
        
        # Index on created_at for sorting
        await collection.create_index("created_at")
        
        # Compound index for common queries
        await collection.create_index([("status", 1), ("created_at", -1)])
    
    def _get_collection(self) -> Collection:
        """Get the jobs collection."""
        if self._db is None:
            raise RuntimeError("Not connected to MongoDB")
        return self._db[self.COLLECTION_NAME]
    
    async def create_job(self, request: JobCreateRequest) -> ConversionJob:
        """
        Create a new conversion job.
        
        Args:
            request: Job creation request
            
        Returns:
            Created ConversionJob with ID
        """
        job = ConversionJob(
            name=request.name,
            description=request.description,
            source=request.source,
            destination=request.destination,
            source_extensions=request.source_extensions,
            source_folder=request.source_folder,
            destination_folder=request.destination_folder,
            conversion_strategy=request.conversion_strategy,
            schedule_cron=request.schedule_cron,
            schedule_enabled=request.schedule_enabled
        )
        
        # Convert to dict and insert
        job_dict = job.model_dump(by_alias=True, exclude={"id"})
        
        collection = self._get_collection()
        result = await collection.insert_one(job_dict)
        
        job.id = str(result.inserted_id)
        return job
    
    async def get_job(self, job_id: str) -> Optional[ConversionJob]:
        """
        Get a job by ID.
        
        Args:
            job_id: Job ID string
            
        Returns:
            ConversionJob or None if not found
        """
        try:
            collection = self._get_collection()
            doc = await collection.find_one({"_id": ObjectId(job_id)})
            
            if doc:
                doc["_id"] = str(doc["_id"])
                return ConversionJob.model_validate(doc)
            return None
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return None
    
    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        limit: int = 50,
        offset: int = 0
    ) -> list[ConversionJob]:
        """
        List jobs with optional filtering.
        
        Args:
            status: Optional status filter
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip
            
        Returns:
            List of ConversionJob objects
        """
        collection = self._get_collection()
        
        # Build query
        query = {}
        if status:
            query["status"] = status.value if isinstance(status, JobStatus) else status
        
        # Execute query
        cursor = collection.find(query).sort("created_at", -1).skip(offset).limit(limit)
        
        jobs = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            jobs.append(ConversionJob.model_validate(doc))
        
        return jobs
    
    async def update_job(self, job_id: str, update: JobUpdateRequest) -> Optional[ConversionJob]:
        """
        Update a job's metadata.
        
        Args:
            job_id: Job ID to update
            update: Update request with new values
            
        Returns:
            Updated ConversionJob or None
        """
        collection = self._get_collection()
        
        # Build update dict (exclude None values)
        update_dict = {k: v for k, v in update.model_dump().items() if v is not None}
        update_dict["updated_at"] = datetime.utcnow()
        
        if not update_dict:
            return await self.get_job(job_id)
        
        result = await collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_dict}
        )
        
        if result.modified_count > 0:
            return await self.get_job(job_id)
        return None
    
    async def save_job(self, job: ConversionJob) -> ConversionJob:
        """
        Save (upsert) a job.
        
        Args:
            job: Job to save
            
        Returns:
            Saved ConversionJob
        """
        collection = self._get_collection()
        
        job.updated_at = datetime.utcnow()
        job_dict = job.model_dump(by_alias=True, exclude={"id"})
        
        if job.id:
            # Update existing
            await collection.replace_one(
                {"_id": ObjectId(job.id)},
                job_dict
            )
        else:
            # Insert new
            result = await collection.insert_one(job_dict)
            job.id = str(result.inserted_id)
        
        return job
    
    async def delete_job(self, job_id: str) -> bool:
        """
        Delete a job.
        
        Args:
            job_id: Job ID to delete
            
        Returns:
            True if deleted, False if not found
        """
        collection = self._get_collection()
        
        result = await collection.delete_one({"_id": ObjectId(job_id)})
        return result.deleted_count > 0
    
    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update job status.
        
        Args:
            job_id: Job ID to update
            status: New status
            error_message: Optional error message for failed status
            
        Returns:
            True if updated
        """
        collection = self._get_collection()
        
        update_dict = {
            "status": status.value if isinstance(status, JobStatus) else status,
            "updated_at": datetime.utcnow()
        }
        
        if status == JobStatus.RUNNING:
            update_dict["started_at"] = datetime.utcnow()
        elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            update_dict["completed_at"] = datetime.utcnow()
        
        if error_message:
            update_dict["error_message"] = error_message
        
        result = await collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_dict}
        )
        
        return result.modified_count > 0
    
    async def get_pending_jobs(self) -> list[ConversionJob]:
        """Get all pending jobs."""
        return await self.list_jobs(status=JobStatus.PENDING)
    
    async def get_running_jobs(self) -> list[ConversionJob]:
        """Get all running jobs."""
        return await self.list_jobs(status=JobStatus.RUNNING)
    
    async def count_jobs(self, status: Optional[JobStatus] = None) -> int:
        """Count jobs with optional status filter."""
        collection = self._get_collection()
        
        query = {}
        if status:
            query["status"] = status.value if isinstance(status, JobStatus) else status
        
        return await collection.count_documents(query)


class ConfigurationStorage:
    """
    MongoDB-based storage for saved source/destination configurations.
    """
    
    COLLECTION_NAME = "configurations"
    
    def __init__(self, mongodb_uri: str, database_name: str = "converter"):
        """Initialize configuration storage."""
        self._client: Optional[AsyncMongoClient] = None
        self._db: Optional[Database] = None
        self._mongodb_uri = mongodb_uri
        self._database_name = database_name
    
    async def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            self._client = AsyncMongoClient(self._mongodb_uri)
            self._db = self._client[self._database_name]
            
            await self._client.admin.command('ping')
            await self._create_indexes()
            
            logger.info(f"ConfigurationStorage connected to MongoDB: {self._database_name}")
            return True
        except Exception as e:
            logger.error(f"ConfigurationStorage failed to connect: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
    
    async def _create_indexes(self) -> None:
        """Create indexes for configuration queries."""
        collection = self._get_collection()
        
        await collection.create_index("type")
        await collection.create_index("is_source")
        await collection.create_index("name")
        await collection.create_index([("is_source", 1), ("type", 1)])
        await collection.create_index([("is_source", 1), ("created_at", -1)])
    
    def _get_collection(self) -> Collection:
        """Get the configurations collection."""
        if self._db is None:
            raise RuntimeError("Not connected to MongoDB")
        return self._db[self.COLLECTION_NAME]
    
    async def create(self, request: SavedConfigurationCreate) -> SavedConfiguration:
        """Create a new saved configuration."""
        config = SavedConfiguration(
            name=request.name,
            description=request.description,
            type=request.type,
            config=request.config,
            is_source=request.is_source
        )
        
        config_dict = config.model_dump(by_alias=True, exclude={"id"})
        
        collection = self._get_collection()
        result = await collection.insert_one(config_dict)
        
        config.id = str(result.inserted_id)
        return config
    
    async def get(self, config_id: str) -> Optional[SavedConfiguration]:
        """Get a configuration by ID."""
        try:
            collection = self._get_collection()
            doc = await collection.find_one({"_id": ObjectId(config_id)})
            
            if doc:
                doc["_id"] = str(doc["_id"])
                return SavedConfiguration.model_validate(doc)
            return None
        except Exception as e:
            logger.error(f"Error getting config {config_id}: {e}")
            return None
    
    async def list_all(
        self,
        is_source: Optional[bool] = None,
        integration_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[SavedConfiguration]:
        """List configurations with optional filtering."""
        collection = self._get_collection()
        
        query = {}
        if is_source is not None:
            query["is_source"] = is_source
        if integration_type:
            query["type"] = integration_type
        
        cursor = collection.find(query).sort("created_at", -1).skip(offset).limit(limit)
        
        configs = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            configs.append(SavedConfiguration.model_validate(doc))
        
        return configs
    
    async def list_sources(self, integration_type: Optional[str] = None) -> list[SavedConfiguration]:
        """List all source configurations."""
        return await self.list_all(is_source=True, integration_type=integration_type)
    
    async def list_destinations(self, integration_type: Optional[str] = None) -> list[SavedConfiguration]:
        """List all destination configurations."""
        return await self.list_all(is_source=False, integration_type=integration_type)
    
    async def update(self, config_id: str, update: SavedConfigurationUpdate) -> Optional[SavedConfiguration]:
        """Update a configuration."""
        collection = self._get_collection()
        
        update_dict = {k: v for k, v in update.model_dump().items() if v is not None}
        update_dict["updated_at"] = datetime.utcnow()
        
        if not update_dict or len(update_dict) == 1:  # Only updated_at
            return await self.get(config_id)
        
        result = await collection.update_one(
            {"_id": ObjectId(config_id)},
            {"$set": update_dict}
        )
        
        if result.modified_count > 0:
            return await self.get(config_id)
        return await self.get(config_id)  # Return existing even if not modified
    
    async def delete(self, config_id: str) -> bool:
        """Delete a configuration."""
        collection = self._get_collection()
        result = await collection.delete_one({"_id": ObjectId(config_id)})
        return result.deleted_count > 0
    
    async def increment_use_count(self, config_id: str) -> bool:
        """Increment the use count and update last_used_at."""
        collection = self._get_collection()
        
        result = await collection.update_one(
            {"_id": ObjectId(config_id)},
            {
                "$inc": {"use_count": 1},
                "$set": {"last_used_at": datetime.utcnow()}
            }
        )
        
        return result.modified_count > 0
    
    async def count(self, is_source: Optional[bool] = None) -> int:
        """Count configurations."""
        collection = self._get_collection()
        
        query = {}
        if is_source is not None:
            query["is_source"] = is_source
        
        return await collection.count_documents(query)


# ============== Execution History Storage ==============

class ExecutionHistoryStorage:
    """MongoDB storage for job execution history."""

    COLLECTION_NAME = "job_execution_history"

    def __init__(self):
        self._db: Optional[Database] = None

    async def connect(self, mongodb_uri: str, mongodb_database: str):
        client: AsyncMongoClient = AsyncMongoClient(mongodb_uri)
        self._db = client[mongodb_database]
        collection = self._get_collection()
        await collection.create_index("job_id")
        await collection.create_index("started_at")

    def _get_collection(self) -> Collection:
        if self._db is None:
            raise RuntimeError("ExecutionHistoryStorage not connected")
        return self._db[self.COLLECTION_NAME]

    async def save(self, record: JobExecutionHistory) -> str:
        doc = record.model_dump(by_alias=True, exclude={"id"})
        result = await self._get_collection().insert_one(doc)
        return str(result.inserted_id)

    async def update(self, record_id: str, **fields):
        await self._get_collection().update_one(
            {"_id": ObjectId(record_id)},
            {"$set": fields}
        )

    async def list_by_job(self, job_id: str, limit: int = 50) -> list[dict]:
        cursor = self._get_collection().find(
            {"job_id": job_id}
        ).sort("started_at", -1).limit(limit)
        results = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)
        return results

    async def list_recent(self, limit: int = 100) -> list[dict]:
        cursor = self._get_collection().find().sort("started_at", -1).limit(limit)
        results = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)
        return results

    async def cleanup_old(self, days: int = 30) -> int:
        cutoff = datetime.utcnow() - timedelta(days=days)
        result = await self._get_collection().delete_many({"started_at": {"$lt": cutoff}})
        return result.deleted_count
