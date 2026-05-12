"""
Vector store factory — mirrors src/factory.py for sources/destinations.
"""
from typing import Any, Type

from .vectorstore import (
    ChromaVectorStore,
    MongoAtlasVectorStore,
    Neo4jVectorStore,
    PgVectorStore,
    RedisVectorStore,
    VectorStoreIntegration,
    VectorStoreType,
)

VECTOR_STORE_REGISTRY: dict[VectorStoreType, Type[VectorStoreIntegration]] = {
    VectorStoreType.CHROMA: ChromaVectorStore,
    VectorStoreType.PGVECTOR: PgVectorStore,
    VectorStoreType.MONGO_ATLAS: MongoAtlasVectorStore,
    VectorStoreType.NEO4J: Neo4jVectorStore,
    VectorStoreType.REDIS: RedisVectorStore,
}


def create_vector_store(
    store_type: VectorStoreType, config: dict[str, Any]
) -> VectorStoreIntegration:
    """Create a vector store integration instance."""
    cls = VECTOR_STORE_REGISTRY.get(store_type)
    if cls is None:
        raise ValueError(f"Unsupported vector store type: {store_type}")
    return cls(config)


def get_supported_vector_stores() -> list[str]:
    """Return supported vector store type identifiers."""
    return [t.value for t in VECTOR_STORE_REGISTRY.keys()]
