"""
Neo4j vector + graph store.

Implements both VectorStoreIntegration (HNSW vector index on :Chunk(embedding))
and GraphCapableVectorStore (Document/Chunk/Entity nodes + typed relations).

Schema (auto-created on connect):

    (:Document {namespace, doc_id, source_hash, embedding_model, chunking_version})
    (:Chunk    {chunk_id PK, namespace, doc_id, text, embedding, metadata, ...})
    (:Entity   {id PK, namespace, label, name, properties})

    (:Document)-[:HAS_CHUNK {order}]->(:Chunk)
    (:Chunk)-[:NEXT]->(:Chunk)               -- adjacency
    (:Chunk)-[:MENTIONS {span?}]->(:Entity)
    (:Entity)-[<dynamic relation type>]->(:Entity)

Vector search uses `db.index.vector.queryNodes`. Filtering by `namespace`
is applied as a post-filter (Neo4j vector index does not have first-class
filter fields like Atlas).

Requires Neo4j 5.11+ (5.20+ recommended).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from pydantic import BaseModel, Field

from .base import (
    Chunk,
    ChunkMention,
    DocumentRecord,
    Entity,
    GraphCapableVectorStore,
    Relation,
    VectorStoreIntegration,
    VectorStoreType,
)

logger = logging.getLogger(__name__)

# Neo4j label/relation-type validation: alnum + underscore, must start with letter.
_IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _safe_ident(name: str, fallback: str) -> str:
    """Return name if it is a valid Neo4j identifier, else fallback."""
    return name if _IDENT_RE.match(name or "") else fallback


class Neo4jConfig(BaseModel):
    """Configuration for Neo4j backend."""
    uri: str = Field(description="bolt://host:7687 or neo4j+s://aura-host")
    username: str = Field(default="neo4j")
    password: str = Field(description="Neo4j password")
    database: str = Field(default="neo4j", description="Database name")
    embedding_dim: int = Field(default=1536, description="Embedding vector dimensionality")
    index_name: str = Field(default="ws_mark_flow_chunk_embeddings",
                            description="Vector index name")
    similarity: str = Field(default="cosine",
                            description="Similarity: cosine | euclidean")

    class Config:
        extra = "ignore"


class Neo4jVectorStore(VectorStoreIntegration, GraphCapableVectorStore):
    """Neo4j-backed vector + graph store."""

    LBL_DOC = "Document"
    LBL_CHUNK = "Chunk"
    LBL_ENTITY = "Entity"

    def __init__(self, config: dict[str, Any]):
        self._config = Neo4jConfig.model_validate(config)
        self._driver: Any = None

    @property
    def store_type(self) -> VectorStoreType:
        return VectorStoreType.NEO4J

    # ---------- connection / schema ----------

    async def connect(self) -> bool:
        try:
            from neo4j import AsyncGraphDatabase  # type: ignore
        except ImportError as e:
            logger.error(f"neo4j driver not installed: {e}")
            return False

        try:
            self._driver = AsyncGraphDatabase.driver(
                self._config.uri,
                auth=(self._config.username, self._config.password),
            )
            await self._driver.verify_connectivity()
            await self._ensure_schema()
            return True
        except Exception as e:
            logger.error(f"Neo4j connect failed: {e}")
            return False

    async def disconnect(self) -> None:
        if self._driver is not None:
            try:
                await self._driver.close()
            except Exception:
                pass
            self._driver = None

    async def _ensure_schema(self) -> None:
        constraints = [
            f"CREATE CONSTRAINT chunk_id_unique IF NOT EXISTS "
            f"FOR (c:{self.LBL_CHUNK}) REQUIRE c.chunk_id IS UNIQUE",
            f"CREATE CONSTRAINT doc_unique IF NOT EXISTS "
            f"FOR (d:{self.LBL_DOC}) REQUIRE (d.namespace, d.doc_id) IS UNIQUE",
            f"CREATE CONSTRAINT entity_unique IF NOT EXISTS "
            f"FOR (e:{self.LBL_ENTITY}) REQUIRE (e.namespace, e.id) IS UNIQUE",
        ]
        indexes = [
            f"CREATE INDEX chunk_ns_doc IF NOT EXISTS "
            f"FOR (c:{self.LBL_CHUNK}) ON (c.namespace, c.doc_id)",
            f"CREATE INDEX entity_ns_label IF NOT EXISTS "
            f"FOR (e:{self.LBL_ENTITY}) ON (e.namespace, e.label)",
        ]
        vector_index = (
            f"CREATE VECTOR INDEX {self._config.index_name} IF NOT EXISTS "
            f"FOR (c:{self.LBL_CHUNK}) ON (c.embedding) "
            f"OPTIONS {{ indexConfig: {{ "
            f"`vector.dimensions`: $dim, "
            f"`vector.similarity_function`: $sim "
            f"}} }}"
        )
        async with self._driver.session(database=self._config.database) as s:
            for q in constraints + indexes:
                await s.run(q)
            await s.run(
                vector_index,
                dim=self._config.embedding_dim,
                sim=self._config.similarity,
            )

    # ---------- vector store ----------

    async def list_documents(self, namespace: str) -> dict[str, DocumentRecord]:
        cypher = f"""
        MATCH (c:{self.LBL_CHUNK} {{namespace: $ns}})
        RETURN c.doc_id          AS doc_id,
               head(collect(c.source_hash))      AS source_hash,
               head(collect(c.embedding_model))  AS embedding_model,
               head(collect(c.chunking_version)) AS chunking_version,
               count(c)          AS chunk_count
        """
        out: dict[str, DocumentRecord] = {}
        async with self._driver.session(database=self._config.database) as s:
            res = await s.run(cypher, ns=namespace)
            async for row in res:
                doc_id = row["doc_id"]
                if not doc_id:
                    continue
                out[doc_id] = DocumentRecord(
                    doc_id=doc_id,
                    source_hash=row["source_hash"],
                    embedding_model=row["embedding_model"],
                    chunking_version=row["chunking_version"],
                    chunk_count=int(row["chunk_count"] or 0),
                )
        return out

    async def upsert_document(
        self,
        namespace: str,
        doc_id: str,
        chunks: list[Chunk],
        source_hash: str,
        embedding_model: str,
        chunking_version: str,
    ) -> None:
        if not chunks:
            return

        for c in chunks:
            if c.embedding is None:
                raise ValueError(f"Missing embedding on chunk {c.chunk_id}")

        rows = [
            {
                "chunk_id": c.chunk_id,
                "text": c.text,
                "metadata": c.metadata or {},
                "embedding": list(c.embedding),
                "order": i,
            }
            for i, c in enumerate(chunks)
        ]

        # Per-doc replace: drop old chunks (and incident MENTIONS edges), then
        # rebuild Document + Chunks + HAS_CHUNK + NEXT chain in one tx.
        delete_old = f"""
        MATCH (c:{self.LBL_CHUNK} {{namespace: $ns, doc_id: $doc_id}})
        DETACH DELETE c
        """
        upsert_doc = f"""
        MERGE (d:{self.LBL_DOC} {{namespace: $ns, doc_id: $doc_id}})
        SET d.source_hash = $source_hash,
            d.embedding_model = $embedding_model,
            d.chunking_version = $chunking_version,
            d.updated_at = timestamp()
        """
        # Use db.create.setNodeVectorProperty (Neo4j helper) for the embedding
        # so values get stored in the binary vector format expected by the index.
        create_chunks = f"""
        MATCH (d:{self.LBL_DOC} {{namespace: $ns, doc_id: $doc_id}})
        UNWIND $rows AS row
        CREATE (c:{self.LBL_CHUNK} {{
            chunk_id: row.chunk_id,
            namespace: $ns,
            doc_id: $doc_id,
            text: row.text,
            metadata: row.metadata,
            chunk_index: row.order,
            source_hash: $source_hash,
            embedding_model: $embedding_model,
            chunking_version: $chunking_version
        }})
        MERGE (d)-[:HAS_CHUNK {{order: row.order}}]->(c)
        WITH c, row
        CALL db.create.setNodeVectorProperty(c, 'embedding', row.embedding)
        RETURN count(*) AS n
        """
        chain_next = f"""
        MATCH (c:{self.LBL_CHUNK} {{namespace: $ns, doc_id: $doc_id}})
        WITH c ORDER BY c.chunk_index
        WITH collect(c) AS cs
        UNWIND range(0, size(cs) - 2) AS i
        WITH cs[i] AS a, cs[i+1] AS b
        MERGE (a)-[:NEXT]->(b)
        """

        async with self._driver.session(database=self._config.database) as s:
            async with await s.begin_transaction() as tx:
                await tx.run(delete_old, ns=namespace, doc_id=doc_id)
                await tx.run(
                    upsert_doc,
                    ns=namespace, doc_id=doc_id,
                    source_hash=source_hash,
                    embedding_model=embedding_model,
                    chunking_version=chunking_version,
                )
                await tx.run(
                    create_chunks,
                    ns=namespace, doc_id=doc_id, rows=rows,
                    source_hash=source_hash,
                    embedding_model=embedding_model,
                    chunking_version=chunking_version,
                )
                if len(rows) > 1:
                    await tx.run(chain_next, ns=namespace, doc_id=doc_id)
                await tx.commit()

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        cypher = f"""
        MATCH (n)
        WHERE (n:{self.LBL_CHUNK} OR n:{self.LBL_DOC})
          AND n.namespace = $ns AND n.doc_id = $doc_id
        WITH count(n) AS deleted
        CALL {{
            WITH deleted
            MATCH (n)
            WHERE (n:{self.LBL_CHUNK} OR n:{self.LBL_DOC})
              AND n.namespace = $ns AND n.doc_id = $doc_id
            DETACH DELETE n
            RETURN count(*) AS _ignored
        }}
        RETURN deleted
        """
        async with self._driver.session(database=self._config.database) as s:
            res = await s.run(cypher, ns=namespace, doc_id=doc_id)
            row = await res.single()
            return bool(row and (row["deleted"] or 0) > 0)

    async def query(
        self,
        namespace: str,
        embedding: list[float],
        k: int = 5,
        filter: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Chunk, float]]:
        # Over-fetch then post-filter by namespace (and optional metadata keys).
        # Neo4j vector indexes don't natively support filter fields.
        over = max(k * 4, 20)

        cypher = f"""
        CALL db.index.vector.queryNodes($index, $over, $vec)
        YIELD node, score
        WHERE node.namespace = $ns
        RETURN node.chunk_id AS chunk_id,
               node.doc_id   AS doc_id,
               node.text     AS text,
               node.metadata AS metadata,
               score
        """

        out: list[tuple[Chunk, float]] = []
        async with self._driver.session(database=self._config.database) as s:
            res = await s.run(
                cypher,
                index=self._config.index_name,
                over=over,
                vec=list(embedding),
                ns=namespace,
            )
            async for row in res:
                meta = row["metadata"] or {}
                # Apply post-filter on metadata (and doc_id) if provided.
                if filter:
                    skip = False
                    for fk, fv in filter.items():
                        if fk == "doc_id":
                            if row["doc_id"] != fv:
                                skip = True
                                break
                        else:
                            if str(meta.get(fk)) != str(fv):
                                skip = True
                                break
                    if skip:
                        continue
                # score is similarity (higher=better) → distance (lower=better)
                # to match Chroma/pgvector cosine convention.
                score = float(row["score"] or 0.0)
                distance = (1.0 - score) if self._config.similarity == "cosine" else -score
                out.append((
                    Chunk(
                        chunk_id=row["chunk_id"],
                        doc_id=row["doc_id"] or "",
                        text=row["text"] or "",
                        metadata=meta if isinstance(meta, dict) else {},
                    ),
                    distance,
                ))
                if len(out) >= k:
                    break
        return out

    # ---------- graph capability ----------

    async def upsert_graph(
        self,
        namespace: str,
        doc_id: str,
        entities: list[Entity],
        relations: list[Relation],
        mentions: list[ChunkMention],
    ) -> None:
        """Per-doc replace of graph artifacts."""
        # 1. Drop old MENTIONS originating from this doc's chunks.
        # 2. MERGE entities (canonical id within namespace).
        # 3. MERGE typed relations (one query per distinct rel type — Cypher
        #    doesn't allow parameterised relationship types).
        # 4. Re-create MENTIONS edges from chunk_id → entity_id.
        # We do NOT auto-delete entities even if they become orphaned, because
        # they may be referenced by other documents.

        drop_mentions = f"""
        MATCH (c:{self.LBL_CHUNK} {{namespace: $ns, doc_id: $doc_id}})
              -[m:MENTIONS]->(:{self.LBL_ENTITY})
        DELETE m
        """

        merge_entities = f"""
        UNWIND $rows AS row
        MERGE (e:{self.LBL_ENTITY} {{namespace: $ns, id: row.id}})
        ON CREATE SET e.label = row.label,
                      e.name = row.name,
                      e.properties = row.properties,
                      e.created_at = timestamp()
        ON MATCH  SET e.label = coalesce(e.label, row.label),
                      e.name  = coalesce(e.name,  row.name),
                      e.properties = apoc.map.merge(coalesce(e.properties, {{}}), row.properties)
        """
        # Fallback if APOC is not installed: overwrite properties.
        merge_entities_no_apoc = f"""
        UNWIND $rows AS row
        MERGE (e:{self.LBL_ENTITY} {{namespace: $ns, id: row.id}})
        ON CREATE SET e.label = row.label,
                      e.name = row.name,
                      e.properties = row.properties,
                      e.created_at = timestamp()
        ON MATCH  SET e.label = coalesce(e.label, row.label),
                      e.name  = coalesce(e.name,  row.name),
                      e.properties = row.properties
        """

        # Group relations by type because Cypher needs the type literal.
        rels_by_type: dict[str, list[dict[str, Any]]] = {}
        for r in relations:
            rt = _safe_ident(r.type, "RELATED_TO")
            rels_by_type.setdefault(rt, []).append({
                "src": r.source_id, "tgt": r.target_id,
                "props": r.properties or {},
            })

        merge_mentions = f"""
        UNWIND $rows AS row
        MATCH (c:{self.LBL_CHUNK} {{chunk_id: row.chunk_id}})
        MATCH (e:{self.LBL_ENTITY} {{namespace: $ns, id: row.entity_id}})
        MERGE (c)-[m:MENTIONS]->(e)
        SET m.span = row.span
        """

        ent_rows = [
            {
                "id": e.id, "label": _safe_ident(e.label, "Entity"),
                "name": e.name, "properties": e.properties or {},
            }
            for e in entities
        ]
        men_rows = [
            {"chunk_id": m.chunk_id, "entity_id": m.entity_id, "span": m.span}
            for m in mentions
        ]

        async with self._driver.session(database=self._config.database) as s:
            async with await s.begin_transaction() as tx:
                await tx.run(drop_mentions, ns=namespace, doc_id=doc_id)

                if ent_rows:
                    try:
                        await tx.run(merge_entities, ns=namespace, rows=ent_rows)
                    except Exception:
                        # APOC not available — fall back.
                        await tx.run(merge_entities_no_apoc, ns=namespace, rows=ent_rows)

                for rt, rows in rels_by_type.items():
                    cy = (
                        f"UNWIND $rows AS row "
                        f"MATCH (a:{self.LBL_ENTITY} {{namespace: $ns, id: row.src}}) "
                        f"MATCH (b:{self.LBL_ENTITY} {{namespace: $ns, id: row.tgt}}) "
                        f"MERGE (a)-[r:{rt}]->(b) "
                        f"SET r += row.props"
                    )
                    await tx.run(cy, ns=namespace, rows=rows)

                if men_rows:
                    await tx.run(merge_mentions, ns=namespace, rows=men_rows)

                await tx.commit()

    async def delete_graph(self, namespace: str, doc_id: str) -> bool:
        """Drop only MENTIONS originating from this doc; keep entities."""
        cypher = f"""
        MATCH (c:{self.LBL_CHUNK} {{namespace: $ns, doc_id: $doc_id}})
              -[m:MENTIONS]->(:{self.LBL_ENTITY})
        WITH count(m) AS n
        CALL {{
            WITH n
            MATCH (c:{self.LBL_CHUNK} {{namespace: $ns, doc_id: $doc_id}})
                  -[m:MENTIONS]->(:{self.LBL_ENTITY})
            DELETE m
            RETURN count(*) AS _ignored
        }}
        RETURN n
        """
        async with self._driver.session(database=self._config.database) as s:
            res = await s.run(cypher, ns=namespace, doc_id=doc_id)
            row = await res.single()
            return bool(row and (row["n"] or 0) > 0)

    async def expand_chunks(
        self,
        namespace: str,
        chunk_ids: list[str],
        hops: int = 1,
        limit: int = 25,
    ) -> list[tuple[Entity, list[Entity]]]:
        if not chunk_ids:
            return []
        hops = max(1, min(int(hops), 3))
        # APOC-free variable-length expansion. We collect entities mentioned
        # by the seed chunks, then their neighbors up to `hops`.
        cypher = f"""
        UNWIND $chunk_ids AS cid
        MATCH (c:{self.LBL_CHUNK} {{chunk_id: cid, namespace: $ns}})
              -[:MENTIONS]->(seed:{self.LBL_ENTITY})
        WITH DISTINCT seed
        LIMIT $limit
        OPTIONAL MATCH (seed)-[*1..{hops}]-(nbr:{self.LBL_ENTITY})
        RETURN seed.id AS s_id, seed.label AS s_label, seed.name AS s_name,
               seed.properties AS s_props,
               collect(DISTINCT {{id: nbr.id, label: nbr.label,
                                  name: nbr.name, properties: nbr.properties}}) AS neighbors
        """
        out: list[tuple[Entity, list[Entity]]] = []
        async with self._driver.session(database=self._config.database) as s:
            res = await s.run(
                cypher, ns=namespace, chunk_ids=chunk_ids, limit=limit,
            )
            async for row in res:
                if not row["s_id"]:
                    continue
                seed = Entity(
                    id=row["s_id"],
                    label=row["s_label"] or "Entity",
                    name=row["s_name"] or row["s_id"],
                    properties=row["s_props"] or {},
                )
                neighbors: list[Entity] = []
                for nb in row["neighbors"] or []:
                    if not nb or not nb.get("id"):
                        continue
                    neighbors.append(Entity(
                        id=nb["id"],
                        label=nb.get("label") or "Entity",
                        name=nb.get("name") or nb["id"],
                        properties=nb.get("properties") or {},
                    ))
                out.append((seed, neighbors))
        return out
