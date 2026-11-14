"""Tests for ``src/factory.py`` and ``src/vectorstore_factory.py``: type -> integration dispatch."""
import pytest

from src import factory, vectorstore_factory
from src.factory import (
    create_destination,
    create_source,
    get_supported_destinations,
    get_supported_sources,
)
from src.integration import FilesystemDestination, FilesystemSource, IntegrationType
from src.vectorstore import ChromaVectorStore, VectorStoreType
from src.vectorstore_factory import create_vector_store, get_supported_vector_stores


class _Recorder:
    """Stand-in integration class that records the config it was built with."""

    def __init__(self, config):
        self.config = config


# --- sources / destinations --------------------------------------------------

def test_create_source_builds_a_configured_filesystem_source(tmp_path):
    source = create_source(IntegrationType.LOCAL, {"path": str(tmp_path)})
    assert isinstance(source, FilesystemSource)
    assert source.integration_type is IntegrationType.LOCAL


def test_create_destination_builds_a_configured_filesystem_destination(tmp_path):
    destination = create_destination(IntegrationType.LOCAL, {"path": str(tmp_path)})
    assert isinstance(destination, FilesystemDestination)


@pytest.mark.parametrize("integration_type", list(factory.SOURCE_REGISTRY))
def test_create_source_passes_config_to_the_registered_class(monkeypatch, integration_type):
    monkeypatch.setitem(factory.SOURCE_REGISTRY, integration_type, _Recorder)
    config = {"anything": integration_type.value}
    source = create_source(integration_type, config)
    assert isinstance(source, _Recorder)
    assert source.config == config


def test_create_source_rejects_unknown_type():
    with pytest.raises(ValueError, match="Unsupported source type: ftp"):
        create_source("ftp", {})


def test_sitemap_is_a_source_only_integration():
    assert "sitemap" in get_supported_sources()
    assert "sitemap" not in get_supported_destinations()
    with pytest.raises(ValueError, match="Unsupported destination type"):
        create_destination(IntegrationType.SITEMAP, {})


def test_supported_lists_are_creatable_type_identifiers():
    # The UI offers exactly these values; each must parse back to a registered type.
    for value in get_supported_sources():
        assert IntegrationType(value) in factory.SOURCE_REGISTRY
    for value in get_supported_destinations():
        assert IntegrationType(value) in factory.DESTINATION_REGISTRY


# --- vector stores -----------------------------------------------------------

def test_create_vector_store_builds_chroma_without_connecting(tmp_path):
    store = create_vector_store(VectorStoreType.CHROMA, {"path": str(tmp_path / "chroma")})
    assert isinstance(store, ChromaVectorStore)
    assert store.store_type is VectorStoreType.CHROMA
    # Construction must be side-effect free: connecting happens explicitly later.
    assert not (tmp_path / "chroma").exists()


@pytest.mark.parametrize("store_type", list(VectorStoreType))
def test_create_vector_store_passes_config_to_the_registered_class(monkeypatch, store_type):
    monkeypatch.setitem(vectorstore_factory.VECTOR_STORE_REGISTRY, store_type, _Recorder)
    store = create_vector_store(store_type, {"k": "v"})
    assert isinstance(store, _Recorder)
    assert store.config == {"k": "v"}


@pytest.mark.parametrize("store_type", ["qdrant", None])
def test_create_vector_store_rejects_unknown_type(store_type):
    with pytest.raises(ValueError, match="Unsupported vector store type"):
        create_vector_store(store_type, {})


def test_every_vector_store_backend_is_supported():
    assert sorted(get_supported_vector_stores()) == sorted(t.value for t in VectorStoreType)
