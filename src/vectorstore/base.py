"""
Base interfaces for VectorStore integrations.

Vector stores are a third integration role alongside Sources and Destinations.
They expose vector-native operations (upsert/delete by document id, query)
and own the indexing concerns: embedding model, chunking strategy, namespace.

Idempotency contract
--------------------
Each document upsert carries:
  - doc_id            : stable identifier (typically the markdown file path)
  - source_hash       : sha256 of the markdown content
  - embedding_model   : model identifier used to produce vectors
  - chunking_version  : opaque tag identifying chunker config

Implementations MUST persist these in chunk metadata so callers can diff
"already-indexed" vs "needs-reindex" using `list_documents()`.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class VectorStoreType(str, Enum):
    """Supported vector store backends."""
    CHROMA = "chroma"           # filesystem-embedded
    PGVECTOR = "pgvector"       # remote (PostgreSQL + pgvector)
    MONGO_ATLAS = "mongo_atlas" # remote (MongoDB Atlas Vector Search)
    NEO4J = "neo4j"             # remote (Neo4j vector + graph)
    REDIS = "redis"             # remote (Redis Stack / RediSearch)
    # future: QDRANT, FAISS, WEAVIATE, ...


class Chunk(BaseModel):
    """A single chunk of a document, optionally with its embedding."""
    chunk_id: str = Field(description="Unique chunk id (deterministic, stable)")
    doc_id: str = Field(description="Parent document id")
    text: str = Field(description="Chunk text content")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Chunk metadata")
    embedding: Optional[list[float]] = Field(default=None, description="Embedding vector")


class DocumentRecord(BaseModel):
    """Index-side view of a document (without chunks/vectors)."""
    doc_id: str
    source_hash: Optional[str] = None
    embedding_model: Optional[str] = None
    chunking_version: Optional[str] = None
    chunk_count: int = 0


def make_doc_id(path: str) -> str:
    """Stable document id from a path (lowercased, no leading slash)."""
    return path.lstrip("/").lower()


def hash_text(text: str) -> str:
    """sha256 hex digest of text content."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class VectorStoreIntegration(ABC):
    """Abstract base for vector store backends."""

    @property
    @abstractmethod
    def store_type(self) -> VectorStoreType:
        ...

    @abstractmethod
    async def connect(self) -> bool:
        """Open backend connection / open files."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close backend connection."""

    async def __aenter__(self):
        if not await self.connect():
            raise RuntimeError(f"Failed to connect to vector store {self.store_type.value}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    @abstractmethod
    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        """Return existing documents keyed by `doc_id` for the given namespace."""

    @abstractmethod
    async def upsert_document(
        self,
        namespace: str,
        doc_id: str,
        chunks: list[Chunk],
        source_hash: str,
        embedding_model: str,
        chunking_version: str,
    ) -> None:
        """Replace all chunks of `doc_id` with `chunks`. Atomic per-doc."""

    @abstractmethod
    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        """Remove all chunks of a document. Returns True if any were removed."""

    @abstractmethod
    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        """Top-k similarity search. Returns (chunk, score) pairs."""


# ---- Graph capability (optional, opt-in by backend) ----

class Entity(BaseModel):
    """A graph entity extracted from one or more chunks."""
    id: str = Field(description="Stable canonical id (e.g. slug of name+label)")
    label: str = Field(description="Ontology label (e.g. Person, Organization)")
    name: str = Field(description="Display name")
    properties: dict[str, Any] = Field(default_factory=dict)


class Relation(BaseModel):
    """A graph relation between two entities."""
    source_id: str = Field(description="Source entity id")
    target_id: str = Field(description="Target entity id")
    type: str = Field(description="Relation type (e.g. WORKS_AT)")
    properties: dict[str, Any] = Field(default_factory=dict)


class ChunkMention(BaseModel):
    """Link from a chunk to an entity it mentions."""
    chunk_id: str
    entity_id: str
    span: Optional[str] = Field(default=None, description="Optional surface form / context")


class GraphCapableVectorStore(ABC):
    """
    Optional capability mixin. Backends implementing this expose a graph
    structure on top of the vector chunks.

    Conventions:
      - `Document(doc_id, namespace)` — one node per source markdown file.
      - `Chunk(chunk_id, namespace, doc_id, ...)` — already created by `upsert_document`.
      - `Entity(id, namespace, label, name, ...)` — extracted by an LLM.
      - `(:Document)-[:HAS_CHUNK]->(:Chunk)`
      - `(:Chunk)-[:MENTIONS]->(:Entity)`
      - `(:Entity)-[<TYPE>]->(:Entity)` — typed relations.

    All operations MUST be idempotent and scoped by `namespace + doc_id`.
    """

    @abstractmethod
    async def upsert_graph(
        self,
        namespace: str,
        doc_id: str,
        entities: list[Entity],
        relations: list[Relation],
        mentions: list[ChunkMention],
    ) -> None:
        """Replace all graph artifacts for `doc_id` with the provided ones."""

    @abstractmethod
    async def delete_graph(self, namespace: str, doc_id: str) -> bool:
        """Remove all entities/relations/mentions tied to `doc_id`."""

    @abstractmethod
    async def expand_chunks(
        self,
        namespace: str,
        chunk_ids: list[str],
        hops: int = 1,
        limit: int = 25,
    ) -> list[tuple[Entity, list[Entity]]]:
        """
        Given seed chunks, return entities mentioned by them and the entities
        reachable within `hops` graph hops. Returns list of (entity, neighbors).
        """
