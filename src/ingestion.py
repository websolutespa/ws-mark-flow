"""
Ingestion service: drives the markdown → chunks → embeddings → vector store pipeline.

Mirrors the structure of `ConversionService` but writes to a `VectorStoreIntegration`
instead of a `DestinationIntegration`. Idempotent: re-running an ingestion job only
re-embeds documents whose `source_hash`, `embedding_model`, or `chunking_version`
has changed since the last run.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from .chunking import ChunkingParams, chunk_markdown, chunking_version
from .config import Settings
from .embedding import EmbeddingService
from .graph_extraction import (
    GraphExtractionSettings,
    GraphExtractor,
    Ontology as ExtractorOntology,
    OntologyRelation as ExtractorOntologyRelation,
)
from .integration import FileInfo, SourceIntegration
from .models import (
    GraphSettings,
    IngestionAnalysis,
    IngestionFileResult,
    IngestionFileStatus,
    IngestionJob,
    JobStatus,
)
from .vectorstore.base import (
    Chunk,
    GraphCapableVectorStore,
    VectorStoreIntegration,
    hash_text,
    make_doc_id,
)

logger = logging.getLogger(__name__)


def _new_chunk_id(doc_id: str, idx: int, text: str) -> str:
    """Deterministic chunk id (stable for identical content)."""
    return f"{doc_id}#{idx:04d}#{hash_text(text)[:12]}"


class IngestionService:
    """Orchestrates ingestion jobs."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._embedder = EmbeddingService(settings)
        self._graph_extractor = GraphExtractor(settings)
        self._temp_dir: Optional[Path] = Path(settings.temp_dir) / "ingestion"
    def _ensure_temp_dir(self) -> Path:
        if self._temp_dir is None or not self._temp_dir.exists():
            self._temp_dir = Path(tempfile.mkdtemp(prefix="ingestion_"))
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        return self._temp_dir

    def cleanup(self) -> None:
        if self._temp_dir and self._temp_dir.exists():
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass
            self._temp_dir = None

    # ---- analysis ----

    async def analyze(
        self,
        source: SourceIntegration,
        vector_store: VectorStoreIntegration,
        namespace: str,
        embedding_model: str,
        chunk_version: str,
        source_extensions: list[str],
        source_folder: Optional[str] = None,
    ) -> tuple[IngestionAnalysis, list[FileInfo]]:
        """
        Diff source markdown vs indexed docs.

        Returns the analysis and the list of source FileInfo objects (so callers
        don't need to re-list).
        """
        files = await source.list_files(
            extensions=source_extensions, folder_path=source_folder
        )
        existing = await vector_store.list_documents(namespace)

        to_ingest: list[dict] = []
        up_to_date: list[dict] = []
        seen_doc_ids: set[str] = set()

        for f in files:
            doc_id = make_doc_id(f.path)
            seen_doc_ids.add(doc_id)
            entry = {
                "doc_id": doc_id,
                "path": f.path,
                "name": f.name,
                "size": f.size,
                "modified_at": f.modified_at.isoformat(),
            }
            rec = existing.get(doc_id)
            # We can't compare source_hash without downloading content; instead
            # mark "needs check" unless model/chunking version mismatch is already
            # enough to require re-ingest. The executor performs the final
            # source_hash comparison after downloading the file.
            if (
                rec
                and rec.embedding_model == embedding_model
                and rec.chunking_version == chunk_version
            ):
                up_to_date.append(entry)
            else:
                to_ingest.append(entry)

        orphans = [
            {"doc_id": doc_id, "chunk_count": rec.chunk_count}
            for doc_id, rec in existing.items()
            if doc_id not in seen_doc_ids
        ]

        total = len(files)
        completion = round((len(up_to_date) / total) * 100, 2) if total else 0.0
        analysis = IngestionAnalysis(
            source_documents=total,
            indexed_documents=len(existing),
            to_ingest=to_ingest,
            up_to_date=up_to_date,
            orphans=orphans,
            completion_percentage=completion,
        )
        return analysis, files

    # ---- single document ----

    @staticmethod
    def _to_extractor_settings(g: GraphSettings) -> GraphExtractionSettings:
        """Translate the storage-side GraphSettings into the runtime extractor's."""
        onto = None
        if g.ontology is not None:
            onto = ExtractorOntology(
                node_labels=list(g.ontology.node_labels or []),
                relations=[
                    ExtractorOntologyRelation(
                        type=r.type, source=list(r.source or []), target=list(r.target or []),
                    )
                    for r in (g.ontology.relations or [])
                ],
                node_properties=dict(g.ontology.node_properties or {}),
            )
        return GraphExtractionSettings(
            enabled=bool(g.enabled),
            mode=g.mode or "lexical",
            ontology=onto,
            ontology_source=g.ontology_source,
            llm_provider=g.llm_provider,
            llm_model=g.llm_model,
            llm_api_key=g.llm_api_key,
            llm_base_url=g.llm_base_url,
            max_entities_per_chunk=g.max_entities_per_chunk,
            max_relations_per_chunk=g.max_relations_per_chunk,
            chunk_concurrency=g.chunk_concurrency,
        )

    async def _ingest_one(
        self,
        result: IngestionFileResult,
        file_info: FileInfo,
        source: SourceIntegration,
        vector_store: VectorStoreIntegration,
        namespace: str,
        chunking: ChunkingParams,
        chunk_version: str,
        embedding_model: str,
        embedding_provider: str,
        embedding_api_key: Optional[str],
        embedding_base_url: Optional[str],
        existing_hash_by_doc: dict[str, Optional[str]],
        temp_dir: Path,
        graph_settings: Optional[GraphExtractionSettings] = None,
    ) -> None:
        result.started_at = datetime.utcnow()
        try:
            doc_id = make_doc_id(file_info.path)
            result.doc_id = doc_id

            # 1. Download
            result.status = IngestionFileStatus.DOWNLOADING
            local_path = temp_dir / file_info.path.lstrip("/")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if not await source.download_file(file_info, local_path):
                result.status = IngestionFileStatus.FAILED
                result.error_message = "Download failed"
                return

            text = local_path.read_text(encoding="utf-8", errors="replace")
            source_hash = hash_text(text)

            # Skip if unchanged.
            if existing_hash_by_doc.get(doc_id) == source_hash:
                result.status = IngestionFileStatus.SKIPPED
                return

            # 2. Chunk
            result.status = IngestionFileStatus.CHUNKING
            pieces = chunk_markdown(text, chunking)
            if not pieces:
                result.status = IngestionFileStatus.SKIPPED
                result.error_message = "No content to chunk"
                return

            # 3. Embed
            result.status = IngestionFileStatus.EMBEDDING
            texts = [t for t, _ in pieces]
            embeddings = await self._embedder.embed(
                texts,
                provider=embedding_provider,
                model=embedding_model,
                api_key=embedding_api_key,
                base_url=embedding_base_url,
            )
            if len(embeddings) != len(texts):
                result.status = IngestionFileStatus.FAILED
                result.error_message = "Embedding count mismatch"
                return

            chunks = [
                Chunk(
                    chunk_id=_new_chunk_id(doc_id, i, t),
                    doc_id=doc_id,
                    text=t,
                    metadata={
                        **meta,
                        "source_path": file_info.path,
                        "chunk_index": i,
                    },
                    embedding=embeddings[i],
                )
                for i, ((t, meta)) in enumerate(pieces)
            ]

            # 4. Upsert
            result.status = IngestionFileStatus.UPSERTING
            await vector_store.upsert_document(
                namespace=namespace,
                doc_id=doc_id,
                chunks=chunks,
                source_hash=source_hash,
                embedding_model=embedding_model,
                chunking_version=chunk_version,
            )

            result.chunk_count = len(chunks)

            # 5. Graph extraction (optional, only if store supports it)
            if (
                graph_settings is not None
                and graph_settings.enabled
                and isinstance(vector_store, GraphCapableVectorStore)
            ):
                result.status = IngestionFileStatus.EXTRACTING_GRAPH
                try:
                    graph = await self._graph_extractor.extract(chunks, graph_settings)
                    if graph.entities or graph.mentions:
                        await vector_store.upsert_graph(
                            namespace=namespace,
                            doc_id=doc_id,
                            entities=graph.entities,
                            relations=graph.relations,
                            mentions=graph.mentions,
                        )
                    result.entity_count = len(graph.entities)
                    result.relation_count = len(graph.relations)
                except Exception as ge:
                    logger.warning(f"Graph extraction failed for {file_info.path}: {ge}")

            result.status = IngestionFileStatus.COMPLETED
            result.completed_at = datetime.utcnow()

        except Exception as e:
            result.status = IngestionFileStatus.FAILED
            result.error_message = str(e)
            logger.exception(f"Ingestion failed for {file_info.path}")

    # ---- job runner ----

    async def run(
        self,
        job: IngestionJob,
        source: SourceIntegration,
        vector_store: VectorStoreIntegration,
        progress_callback=None,
    ) -> IngestionJob:
        """Execute the ingestion job end-to-end."""
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()

        chunking = ChunkingParams(
            strategy=job.chunking.strategy,
            chunk_size=job.chunking.chunk_size,
            chunk_overlap=job.chunking.chunk_overlap,
        )
        chunk_ver = chunking_version(chunking)

        # `embedding` may have been deserialized as dict or model
        embedding = job.embedding

        # Translate stored graph settings → runtime extractor settings.
        graph_runtime = self._to_extractor_settings(job.graph) if job.graph else None
        if graph_runtime and graph_runtime.enabled and not isinstance(
            # Defer the isinstance check to per-document; here just warn.
            None, type(None),
        ):
            pass
        if (
            graph_runtime and graph_runtime.enabled
            and not hasattr(vector_store, "upsert_graph")
        ):
            logger.warning(
                "Graph extraction enabled but vector store %s is not graph-capable; "
                "extraction will be skipped.",
                vector_store.store_type.value,
            )

        temp_dir = self._ensure_temp_dir()

        try:
            analysis, files = await self.analyze(
                source=source,
                vector_store=vector_store,
                namespace=job.vector_store.namespace,
                embedding_model=embedding.model,
                chunk_version=chunk_ver,
                source_extensions=job.source_extensions,
                source_folder=job.source_folder,
            )

            # Map of indexed source_hash per doc_id (only for docs whose
            # model/chunker already match — we'll skip if file content is also
            # identical).
            existing_docs = await vector_store.list_documents(job.vector_store.namespace)
            existing_hashes: dict[str, Optional[str]] = {}
            for did, rec in existing_docs.items():
                if (
                    rec.embedding_model == embedding.model
                    and rec.chunking_version == chunk_ver
                ):
                    existing_hashes[did] = rec.source_hash

            # Build per-file results: include up_to_date as "candidate skip".
            job.file_results = []
            files_by_path = {f.path: f for f in files}
            for entry in analysis.to_ingest + analysis.up_to_date:
                fi = files_by_path[entry["path"]]
                job.file_results.append(
                    IngestionFileResult(
                        source_path=fi.path,
                        doc_id=entry["doc_id"],
                        status=IngestionFileStatus.PENDING,
                    )
                )
            job.update_stats()

            semaphore = asyncio.Semaphore(max(1, job.batch_size))
            processed = 0
            total = len(job.file_results)

            async def _run_one(result: IngestionFileResult) -> None:
                nonlocal processed
                async with semaphore:
                    await self._ingest_one(
                        result=result,
                        file_info=files_by_path[result.source_path],
                        source=source,
                        vector_store=vector_store,
                        namespace=job.vector_store.namespace,
                        chunking=chunking,
                        chunk_version=chunk_ver,
                        embedding_model=embedding.model,
                        embedding_provider=embedding.provider,
                        embedding_api_key=embedding.api_key,
                        embedding_base_url=embedding.base_url,
                        existing_hash_by_doc=existing_hashes,
                        temp_dir=temp_dir,
                        graph_settings=graph_runtime,
                    )
                processed += 1
                job.update_stats()
                if progress_callback:
                    await progress_callback(job, processed, total)

            await asyncio.gather(*[_run_one(r) for r in job.file_results])

            # Optional: delete orphans
            if job.delete_orphans:
                for orphan in analysis.orphans:
                    try:
                        await vector_store.delete_document(
                            job.vector_store.namespace, orphan["doc_id"]
                        )
                    except Exception as e:
                        logger.warning(f"Orphan delete failed for {orphan['doc_id']}: {e}")

            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()

        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            logger.exception("Ingestion job failed")

        finally:
            self.cleanup()

        return job
