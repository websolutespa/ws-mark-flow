"""Tests for ``src/embedding.py``: provider dispatch and the real SDKs driven via FakeWire."""
import base64
import struct

import pytest

from src.config import Settings
from src.embedding import EmbeddingService
from tests.helpers import FakeWire


def _openai_embeddings(*vectors):
    # The OpenAI SDK requests ``encoding_format=base64`` and decodes float32 LE.
    return {
        "object": "list",
        "model": "text-embedding-3-small",
        "data": [
            {
                "object": "embedding",
                "index": i,
                "embedding": base64.b64encode(struct.pack(f"<{len(v)}f", *v)).decode(),
            }
            for i, v in enumerate(vectors)
        ],
        "usage": {"prompt_tokens": 2, "total_tokens": 2},
    }


@pytest.fixture
def wire(monkeypatch):
    # Base-URL overrides from the shell would redirect the SDKs.
    for var in ("OPENAI_BASE_URL", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_GENAI_USE_VERTEXAI"):
        monkeypatch.delenv(var, raising=False)
    return FakeWire(monkeypatch)


@pytest.fixture
def service(settings):
    return EmbeddingService(settings)


async def test_empty_input_makes_no_request(service, wire):
    assert await service.embed([], "openai", "text-embedding-3-small", api_key="k") == []
    assert wire.requests == []


async def test_openai_embeddings_round_trip(service, wire):
    wire.reply("POST", "/v1/embeddings", json=_openai_embeddings([0.5, 0.25], [-1.0, 2.0]))

    vectors = await service.embed(
        ["first", "second"], "openai", "text-embedding-3-small", api_key="sk-test",
    )

    assert vectors == [[0.5, 0.25], [-1.0, 2.0]]
    [request] = wire.requests
    assert request.url == "https://api.openai.com/v1/embeddings"
    assert request.headers["authorization"] == "Bearer sk-test"
    assert request.json["model"] == "text-embedding-3-small"
    assert request.json["input"] == ["first", "second"]


@pytest.mark.parametrize("provider", [None, "", "OpenAI"])
async def test_provider_defaults_to_openai_case_insensitively(service, wire, provider):
    wire.reply("POST", "/v1/embeddings", json=_openai_embeddings([1.0]))
    assert await service.embed(["x"], provider, "m", api_key="k") == [[1.0]]
    assert wire.requests[0].url.startswith("https://api.openai.com/")


async def test_ollama_uses_placeholder_key_and_settings_base_url(tmp_path, wire):
    settings = Settings(temp_dir=str(tmp_path), llm_base_url="http://localhost:11434/v1")
    wire.reply("POST", "/v1/embeddings", json=_openai_embeddings([0.5]))

    vectors = await EmbeddingService(settings).embed(["x"], "ollama", "nomic-embed-text")

    assert vectors == [[0.5]]
    [request] = wire.requests
    assert request.url == "http://localhost:11434/v1/embeddings"
    # The OpenAI SDK refuses an empty key; Ollama ignores whatever is sent.
    assert request.headers["authorization"] == "Bearer ollama"


async def test_settings_credentials_are_the_fallback(tmp_path, wire):
    settings = Settings(
        temp_dir=str(tmp_path), llm_api_key="settings-key", llm_base_url="http://127.0.0.1:9000/v1",
    )
    wire.reply("POST", "/v1/embeddings", json=_openai_embeddings([0.5]))

    await EmbeddingService(settings).embed(["x"], "openai", "m")

    [request] = wire.requests
    assert request.url == "http://127.0.0.1:9000/v1/embeddings"
    assert request.headers["authorization"] == "Bearer settings-key"


async def test_explicit_credentials_override_settings(tmp_path, wire):
    settings = Settings(
        temp_dir=str(tmp_path), llm_api_key="settings-key", llm_base_url="http://127.0.0.1:9000/v1",
    )
    wire.reply("POST", "/v1/embeddings", json=_openai_embeddings([0.5]))

    await EmbeddingService(settings).embed(
        ["x"], "openai", "m", api_key="job-key", base_url="http://localhost:8080/v1",
    )

    [request] = wire.requests
    assert request.url == "http://localhost:8080/v1/embeddings"
    assert request.headers["authorization"] == "Bearer job-key"


async def test_google_batch_embeddings(service, wire):
    wire.reply(
        "POST",
        "text-embedding-004:batchEmbedContents",
        json={"embeddings": [{"values": [0.1, 0.2]}, {"values": [0.3, 0.4]}]},
    )

    vectors = await service.embed(["a", "b"], "Google", "text-embedding-004", api_key="g-key")

    assert vectors == [[0.1, 0.2], [0.3, 0.4]]
    [request] = wire.requests
    assert request.headers["x-goog-api-key"] == "g-key"
    assert [r["content"]["parts"][0]["text"] for r in request.json["requests"]] == ["a", "b"]


async def test_unsupported_provider_is_rejected(service, wire):
    with pytest.raises(ValueError, match="Unsupported embedding provider: cohere"):
        await service.embed(["x"], "cohere", "embed-v3", api_key="k")
    assert wire.requests == []
