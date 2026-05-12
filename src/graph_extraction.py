"""
Schema-guided graph extraction from markdown chunks.

Strategy
--------
1. The user defines an `Ontology` (allowed node labels + relation types). It
   may be loaded from a YAML/JSON file or supplied inline.
2. For each chunk we call an LLM with a strict JSON schema response format,
   constrained to the ontology. The LLM may NOT invent labels or relation
   types.
3. Returned entities are canonicalised with a deterministic id
   (slug of label + name) so re-runs deduplicate via `MERGE`.
4. The extractor is *idempotent and stateless*: it never writes to the store,
   it only returns `(entities, relations, mentions)`.

If `Ontology` is empty / `mode == "lexical"`, the extractor falls back to a
generic NER-style call with a fixed coarse type set
(`Person, Organization, Product, Place, Concept, Event, Other`) and emits
only `MENTIONS` (no typed relations).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Optional

from pydantic import BaseModel, Field

from .config import Settings
from .vectorstore.base import Chunk, ChunkMention, Entity, Relation

logger = logging.getLogger(__name__)


# ---------------- Ontology ----------------

LEXICAL_LABELS = ["Person", "Organization", "Product", "Place", "Concept", "Event", "Other"]


class OntologyRelation(BaseModel):
    type: str = Field(description="Relation type (e.g. WORKS_AT)")
    source: list[str] = Field(default_factory=list, description="Allowed source labels (empty = any)")
    target: list[str] = Field(default_factory=list, description="Allowed target labels (empty = any)")


class Ontology(BaseModel):
    """Allowed labels + relation types. Empty fields ⇒ unrestricted."""
    node_labels: list[str] = Field(default_factory=list)
    relations: list[OntologyRelation] = Field(default_factory=list)
    node_properties: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Optional per-label allowed property keys",
    )

    def fingerprint(self) -> str:
        """Stable id for cache invalidation."""
        import hashlib
        payload = self.model_dump_json(exclude_none=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def load_ontology(path_or_inline: Optional[str | dict]) -> Ontology:
    """Load an Ontology from a file path, JSON/YAML string, or dict."""
    if not path_or_inline:
        return Ontology()
    if isinstance(path_or_inline, dict):
        return Ontology.model_validate(path_or_inline)
    s = str(path_or_inline).strip()
    # File path?
    from pathlib import Path
    p = Path(s)
    if p.exists() and p.is_file():
        text = p.read_text(encoding="utf-8")
    else:
        text = s
    # Try JSON first, then YAML.
    try:
        return Ontology.model_validate(json.loads(text))
    except Exception:
        pass
    try:
        import yaml  # type: ignore
        return Ontology.model_validate(yaml.safe_load(text))
    except Exception as e:
        raise ValueError(f"Could not parse ontology: {e}")


# ---------------- Canonicalisation ----------------

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def canonical_id(label: str, name: str) -> str:
    """Deterministic entity id: label::slug(name)."""
    slug = _SLUG_RE.sub("-", (name or "").strip().lower()).strip("-")
    return f"{(label or 'entity').lower()}::{slug or 'unknown'}"


# ---------------- Settings ----------------

class GraphExtractionSettings(BaseModel):
    """Per-job graph extraction configuration."""
    enabled: bool = Field(default=False)
    mode: str = Field(
        default="lexical",
        description="lexical | schema_guided",
    )
    ontology: Optional[Ontology] = Field(
        default=None,
        description="Ontology object (used when mode='schema_guided')",
    )
    ontology_source: Optional[str] = Field(
        default=None,
        description="Ontology YAML/JSON string or filesystem path",
    )
    llm_provider: Optional[str] = Field(default=None, description="Override LLM provider")
    llm_model: Optional[str] = Field(default=None, description="Override LLM model")
    llm_api_key: Optional[str] = Field(default=None)
    llm_base_url: Optional[str] = Field(default=None)
    max_entities_per_chunk: int = Field(default=15)
    max_relations_per_chunk: int = Field(default=10)
    chunk_concurrency: int = Field(default=2)


# ---------------- Extractor ----------------

class _ExtractionResult(BaseModel):
    entities: list[Entity]
    relations: list[Relation]
    mentions: list[ChunkMention]


def extractor_version(s: GraphExtractionSettings) -> str:
    """Stable id for skipping unchanged extraction work."""
    onto_fp = (s.ontology.fingerprint() if s.ontology else "none")
    return f"{s.mode}-{s.llm_model or 'default'}-{onto_fp}"


class GraphExtractor:
    """Stateless LLM-driven entity + relation extractor."""

    def __init__(self, settings: Settings):
        self._settings = settings

    async def extract(
        self,
        chunks: list[Chunk],
        cfg: GraphExtractionSettings,
    ) -> _ExtractionResult:
        if not cfg.enabled or not chunks:
            return _ExtractionResult(entities=[], relations=[], mentions=[])

        provider = (cfg.llm_provider or self._settings.llm_provider or "openai").lower()
        model = cfg.llm_model or self._settings.llm_model
        api_key = cfg.llm_api_key or self._settings.llm_api_key or ""
        base_url = cfg.llm_base_url or self._settings.llm_base_url or ""

        ontology = cfg.ontology
        if ontology is None and cfg.ontology_source:
            try:
                ontology = load_ontology(cfg.ontology_source)
            except Exception as e:
                logger.error(f"Ontology load failed: {e}")
                ontology = None

        sem = asyncio.Semaphore(max(1, cfg.chunk_concurrency))

        async def _one(c: Chunk) -> tuple[list[Entity], list[Relation], list[ChunkMention]]:
            async with sem:
                return await asyncio.to_thread(
                    self._extract_chunk_sync,
                    c, cfg, ontology, provider, model, api_key, base_url,
                )

        results = await asyncio.gather(*[_one(c) for c in chunks])

        # Deduplicate entities by canonical id; merge property dicts.
        ent_by_id: dict[str, Entity] = {}
        relations: list[Relation] = []
        mentions: list[ChunkMention] = []
        seen_rel: set[tuple[str, str, str]] = set()

        for ents, rels, mens in results:
            for e in ents:
                existing = ent_by_id.get(e.id)
                if existing is None:
                    ent_by_id[e.id] = e
                else:
                    merged = {**(existing.properties or {}), **(e.properties or {})}
                    existing.properties = merged
                    if not existing.name:
                        existing.name = e.name
            for r in rels:
                key = (r.source_id, r.type, r.target_id)
                if key in seen_rel:
                    continue
                seen_rel.add(key)
                relations.append(r)
            mentions.extend(mens)

        return _ExtractionResult(
            entities=list(ent_by_id.values()),
            relations=relations,
            mentions=mentions,
        )

    # ---------- per-chunk LLM call ----------

    def _extract_chunk_sync(
        self,
        chunk: Chunk,
        cfg: GraphExtractionSettings,
        ontology: Optional[Ontology],
        provider: str,
        model: str,
        api_key: str,
        base_url: str,
    ) -> tuple[list[Entity], list[Relation], list[ChunkMention]]:
        try:
            allowed_labels, allowed_rel_types, prompt = self._build_prompt(chunk, cfg, ontology)
            raw = self._call_llm(prompt, provider, model, api_key, base_url)
            data = self._parse_json(raw)
        except Exception as e:
            logger.warning(f"Graph extraction failed on chunk {chunk.chunk_id}: {e}")
            return [], [], []

        return self._coerce(data, chunk, allowed_labels, allowed_rel_types, cfg)

    @staticmethod
    def _build_prompt(
        chunk: Chunk,
        cfg: GraphExtractionSettings,
        ontology: Optional[Ontology],
    ) -> tuple[set[str], set[str], str]:
        if cfg.mode == "schema_guided" and ontology and ontology.node_labels:
            allowed_labels = set(ontology.node_labels)
            allowed_rel_types = {r.type for r in ontology.relations}
            ont_block = json.dumps(
                {
                    "node_labels": ontology.node_labels,
                    "relations": [r.model_dump() for r in ontology.relations],
                    "node_properties": ontology.node_properties,
                },
                indent=2,
            )
            instructions = (
                "Extract entities and typed relations from the TEXT below using "
                "ONLY the allowed labels and relation types from the ONTOLOGY. "
                "Do NOT invent new labels or relation types. If a candidate "
                "does not fit, omit it.\n\n"
                f"ONTOLOGY:\n{ont_block}\n\n"
            )
        else:
            allowed_labels = set(LEXICAL_LABELS)
            allowed_rel_types = set()  # lexical mode: no typed relations
            instructions = (
                "Extract named entities from the TEXT below. Each entity must "
                f"be classified with one of: {', '.join(LEXICAL_LABELS)}. "
                "Do NOT extract relations.\n\n"
            )

        schema = (
            '{"entities":[{"name": str, "label": str, "properties": {<str>: <str>}}],'
            '"relations":[{"source_name": str, "source_label": str, '
            '"target_name": str, "target_label": str, "type": str, '
            '"properties": {<str>: <str>}}]}'
        )

        prompt = (
            f"{instructions}"
            f"Constraints:\n"
            f"- Output STRICT JSON matching this shape: {schema}\n"
            f"- At most {cfg.max_entities_per_chunk} entities and "
            f"{cfg.max_relations_per_chunk} relations.\n"
            f"- All property values must be strings.\n"
            f"- Return ONLY the JSON object, no commentary, no markdown fences.\n\n"
            f"TEXT:\n\"\"\"\n{chunk.text}\n\"\"\""
        )
        return allowed_labels, allowed_rel_types, prompt

    @staticmethod
    def _call_llm(
        prompt: str, provider: str, model: str, api_key: str, base_url: str,
    ) -> str:
        if provider in ("openai", "ollama"):
            from openai import OpenAI
            kwargs: dict[str, Any] = {
                "api_key": api_key or ("ollama" if provider == "ollama" else "")
            }
            if base_url:
                kwargs["base_url"] = base_url
            client = OpenAI(**kwargs)
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system",
                     "content": "You extract knowledge graphs as strict JSON."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            return resp.choices[0].message.content or "{}"

        if provider == "anthropic":
            from anthropic import Anthropic
            client = Anthropic(api_key=api_key)
            resp = client.messages.create(
                model=model,
                max_tokens=2048,
                system="You extract knowledge graphs as strict JSON. "
                       "Respond ONLY with the JSON object.",
                messages=[{"role": "user", "content": prompt}],
            )
            parts = []
            for block in resp.content:
                if getattr(block, "type", "") == "text":
                    parts.append(getattr(block, "text", ""))
            return "".join(parts) or "{}"

        if provider == "google":
            from google import genai
            client = genai.Client(api_key=api_key)
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config={"response_mime_type": "application/json"},
            )
            return getattr(resp, "text", "") or "{}"

        raise ValueError(f"Unsupported LLM provider for extraction: {provider}")

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        s = (raw or "").strip()
        # Strip ``` fences if a model added them despite instructions.
        if s.startswith("```"):
            s = s.strip("`")
            # remove leading "json" tag if present
            if s.lower().startswith("json"):
                s = s[4:].lstrip()
            # trim trailing fence
            if s.endswith("```"):
                s = s[:-3]
        try:
            data = json.loads(s)
            if not isinstance(data, dict):
                return {}
            return data
        except Exception:
            # last-ditch: find first {...} block
            m = re.search(r"\{.*\}", s, re.DOTALL)
            if not m:
                return {}
            try:
                return json.loads(m.group(0))
            except Exception:
                return {}

    @staticmethod
    def _coerce(
        data: dict[str, Any],
        chunk: Chunk,
        allowed_labels: set[str],
        allowed_rel_types: set[str],
        cfg: GraphExtractionSettings,
    ) -> tuple[list[Entity], list[Relation], list[ChunkMention]]:
        entities: list[Entity] = []
        mentions: list[ChunkMention] = []
        seen_ent: set[str] = set()
        # name → canonical id, used to resolve relation endpoints.
        name_to_id: dict[tuple[str, str], str] = {}

        raw_ents = data.get("entities") or []
        for raw in raw_ents[: cfg.max_entities_per_chunk]:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or "").strip()
            label = str(raw.get("label") or "").strip()
            if not name or not label:
                continue
            if allowed_labels and label not in allowed_labels:
                # Schema-guided: reject. Lexical: should always be allowed.
                continue
            cid = canonical_id(label, name)
            props_raw = raw.get("properties") or {}
            props = {
                str(k): str(v) for k, v in props_raw.items()
                if isinstance(props_raw, dict) and v is not None
            }
            if cid not in seen_ent:
                seen_ent.add(cid)
                entities.append(Entity(
                    id=cid, label=label, name=name, properties=props,
                ))
            name_to_id[(name.lower(), label)] = cid
            # Also key by name alone for relation resolution fuzziness.
            name_to_id.setdefault((name.lower(), ""), cid)
            mentions.append(ChunkMention(
                chunk_id=chunk.chunk_id, entity_id=cid, span=name,
            ))

        relations: list[Relation] = []
        if allowed_rel_types or cfg.mode == "schema_guided":
            for raw in (data.get("relations") or [])[: cfg.max_relations_per_chunk]:
                if not isinstance(raw, dict):
                    continue
                rtype = str(raw.get("type") or "").strip()
                if not rtype:
                    continue
                if allowed_rel_types and rtype not in allowed_rel_types:
                    continue
                s_name = str(raw.get("source_name") or "").strip().lower()
                t_name = str(raw.get("target_name") or "").strip().lower()
                s_label = str(raw.get("source_label") or "").strip()
                t_label = str(raw.get("target_label") or "").strip()
                src_id = name_to_id.get((s_name, s_label)) or name_to_id.get((s_name, ""))
                tgt_id = name_to_id.get((t_name, t_label)) or name_to_id.get((t_name, ""))
                if not src_id or not tgt_id or src_id == tgt_id:
                    continue
                props_raw = raw.get("properties") or {}
                props = {
                    str(k): str(v) for k, v in props_raw.items()
                    if isinstance(props_raw, dict) and v is not None
                }
                relations.append(Relation(
                    source_id=src_id, target_id=tgt_id,
                    type=rtype, properties=props,
                ))

        return entities, relations, mentions
