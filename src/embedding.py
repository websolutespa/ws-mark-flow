"""
Embedding service abstraction.

Uniform interface over multiple providers:
  - openai  : OpenAI Embeddings API
  - ollama  : OpenAI-compatible local server (http://host:port/v1)
  - google  : Gemini text-embedding models

Selection is driven by `EmbeddingSettings` (see models.py). Falls back to
the global Settings for any unset field.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .config import Settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Provider-agnostic embedding generator."""

    def __init__(self, settings: Settings):
        self._settings = settings

    # ---- public API ----

    async def embed(
        self,
        texts: list[str],
        provider: str,
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> list[list[float]]:
        """Compute embeddings for a list of texts."""
        if not texts:
            return []
        provider = (provider or "openai").lower()
        api_key = api_key or self._settings.llm_api_key or ""
        base_url = base_url or self._settings.llm_base_url or ""

        if provider in ("openai", "ollama"):
            return await asyncio.to_thread(
                self._openai_compat, texts, model, api_key, base_url, provider
            )
        if provider == "google":
            return await asyncio.to_thread(self._google, texts, model, api_key)

        raise ValueError(f"Unsupported embedding provider: {provider}")

    # ---- providers ----

    @staticmethod
    def _openai_compat(
        texts: list[str],
        model: str,
        api_key: str,
        base_url: str,
        provider: str,
    ) -> list[list[float]]:
        from openai import OpenAI

        kwargs = {"api_key": api_key or ("ollama" if provider == "ollama" else "")}
        if base_url:
            kwargs["base_url"] = base_url
        client = OpenAI(**kwargs)

        resp = client.embeddings.create(model=model, input=texts)
        return [d.embedding for d in resp.data]

    @staticmethod
    def _google(texts: list[str], model: str, api_key: str) -> list[list[float]]:
        from google import genai

        client = genai.Client(api_key=api_key)
        out: list[list[float]] = []
        # Gemini SDK supports batched input via `contents=[...]` for embed_content.
        resp = client.models.embed_content(model=model, contents=texts)
        for emb in resp.embeddings:
            out.append(list(emb.values))
        return out
