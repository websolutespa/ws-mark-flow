"""Tests for ``src/chunking.py``: chunker config versioning and the three strategies."""
import re

import pytest

from src.chunking import ChunkingParams, chunk_markdown, chunking_version


def _texts(chunks):
    return [text for text, _ in chunks]


def _words(text):
    return re.findall(r"\w+", text)


# --- chunking_version --------------------------------------------------------

def test_chunking_version_is_stable_for_equal_params():
    a = ChunkingParams(strategy="recursive", chunk_size=500, chunk_overlap=50)
    b = ChunkingParams(strategy="recursive", chunk_size=500, chunk_overlap=50)
    assert chunking_version(a) == chunking_version(b)
    assert chunking_version(a).startswith("recursive-")


@pytest.mark.parametrize(
    "changed",
    [
        ChunkingParams(strategy="fixed"),
        ChunkingParams(chunk_size=1201),
        ChunkingParams(chunk_overlap=0),
    ],
)
def test_chunking_version_changes_when_any_param_changes(changed):
    # The version is the re-index cache key: any config change must invalidate it.
    assert chunking_version(changed) != chunking_version(ChunkingParams())


# --- fixed -------------------------------------------------------------------

@pytest.mark.parametrize(
    ("text", "size", "overlap", "expected"),
    [
        ("abcdefghij", 4, 1, ["abcd", "defg", "ghij"]),
        ("abcdefghij", 5, 0, ["abcde", "fghij"]),
        ("abcdefghij", 4, 0, ["abcd", "efgh", "ij"]),
        ("abc", 10, 3, ["abc"]),
        # Overlap >= size is clamped to size - 1 so the window always advances.
        ("abcde", 2, 5, ["ab", "bc", "cd", "de"]),
        ("", 4, 1, []),
    ],
)
def test_fixed_windows(text, size, overlap, expected):
    params = ChunkingParams(strategy="fixed", chunk_size=size, chunk_overlap=overlap)
    chunks = chunk_markdown(text, params)
    assert _texts(chunks) == expected
    assert all(meta == {} for _, meta in chunks)


@pytest.mark.parametrize("size", [0, -5])
def test_fixed_non_positive_size_returns_whole_text(size):
    params = ChunkingParams(strategy="fixed", chunk_size=size, chunk_overlap=2)
    assert _texts(chunk_markdown("whole text", params)) == ["whole text"]


# --- recursive ---------------------------------------------------------------

def _recursive(size, overlap=0):
    return ChunkingParams(strategy="recursive", chunk_size=size, chunk_overlap=overlap)


@pytest.mark.parametrize("text", ["", "   \n\n  ", None])
def test_recursive_blank_input_yields_no_chunks(text):
    assert chunk_markdown(text, _recursive(10)) == []


def test_recursive_short_text_is_one_stripped_chunk():
    assert chunk_markdown("  hello world \n", _recursive(50)) == [("hello world", {})]


def test_recursive_merges_paragraphs_greedily():
    text = "aaa\n\nbbb\n  \nccc"
    # "aaa\n\nbbb" is exactly 8 characters: it fits, adding "ccc" would not.
    assert _texts(chunk_markdown(text, _recursive(8))) == ["aaa\n\nbbb", "ccc"]


def test_recursive_splits_on_sentences_when_there_is_one_paragraph():
    text = "One two. Three four! Five six? Seven."
    # "One two.\n\nThree four!" is exactly 21 characters.
    assert _texts(chunk_markdown(text, _recursive(21))) == [
        "One two.\n\nThree four!",
        "Five six?\n\nSeven.",
    ]


def test_recursive_falls_back_to_fixed_windows_without_boundaries():
    assert _texts(chunk_markdown("abcdefghij", _recursive(4))) == ["abcd", "efgh", "ij"]


def test_recursive_recurses_into_oversized_paragraphs():
    text = "intro\n\nFirst long sentence. Second long sentence.\n\noutro"
    assert _texts(chunk_markdown(text, _recursive(25))) == [
        "intro",
        "First long sentence.",
        "Second long sentence.",
        "outro",
    ]


def test_recursive_overlap_carries_tail_of_previous_chunk():
    text = "aaaa\n\nbbbb\n\ncccc"
    assert _texts(chunk_markdown(text, _recursive(5, overlap=2))) == [
        "aaaa",
        "aa\n\nbbbb",
        "bb\n\ncccc",
    ]


LONG_TEXT = "\n\n".join(
    [
        "Alpha beta gamma delta. Epsilon zeta eta theta! Iota kappa lambda?",
        "Mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega",
        "Short para.",
        "x" * 130,
        "Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Sed do eiusmod.",
    ]
)


@pytest.mark.parametrize("size", [20, 40, 64, 300])
def test_recursive_without_overlap_respects_size_and_preserves_content(size):
    chunks = _texts(chunk_markdown(LONG_TEXT, _recursive(size)))
    assert chunks
    assert all(0 < len(c) <= size for c in chunks)
    # Nothing is lost or reordered (fixed windows may cut a word in two).
    assert "".join(_words(" ".join(chunks))) == "".join(_words(LONG_TEXT))


@pytest.mark.xfail(
    strict=True,
    reason="BUG: chunk_overlap is prepended after merging, so chunks exceed chunk_size",
)
def test_recursive_with_overlap_still_respects_chunk_size():
    chunks = _texts(chunk_markdown(LONG_TEXT, _recursive(40, overlap=10)))
    assert all(len(c) <= 40 for c in chunks)


# --- markdown_headers --------------------------------------------------------

def _headers(size=1200, overlap=0):
    return ChunkingParams(strategy="markdown_headers", chunk_size=size, chunk_overlap=overlap)


def test_markdown_headers_tracks_heading_breadcrumbs():
    text = (
        "Preamble text.\n"
        "# Guide\n"
        "Intro.\n"
        "## Install\n"
        "Run pip.\n"
        "### Windows\n"
        "Use py.\n"
        "## Usage\n"
        "Call it.\n"
        "# Appendix\n"
        "Extra.\n"
    )
    assert chunk_markdown(text, _headers()) == [
        ("Preamble text.", {}),
        ("Intro.", {"headings": "Guide"}),
        ("Run pip.", {"headings": "Guide > Install"}),
        ("Use py.", {"headings": "Guide > Install > Windows"}),
        ("Call it.", {"headings": "Guide > Usage"}),
        ("Extra.", {"headings": "Appendix"}),
    ]


def test_markdown_headers_skips_empty_sections_but_keeps_their_path():
    text = "# Title\n\n## Empty\n## Filled\nBody here.\n"
    assert chunk_markdown(text, _headers()) == [
        ("Body here.", {"headings": "Title > Filled"}),
    ]


def test_markdown_headers_without_headers_behaves_like_recursive():
    text = "no headers\n\njust text"
    assert chunk_markdown(text, _headers(size=12)) == chunk_markdown(text, _recursive(12))
    assert chunk_markdown(text, _headers(size=12)) == [("no headers", {}), ("just text", {})]


def test_markdown_headers_splits_long_sections_and_repeats_metadata():
    text = "## Section\n" + "\n\n".join(["para one", "para two", "para three"])
    assert chunk_markdown(text, _headers(size=10)) == [
        ("para one", {"headings": "Section"}),
        ("para two", {"headings": "Section"}),
        ("para three", {"headings": "Section"}),
    ]


def test_hash_without_space_is_not_a_header():
    assert chunk_markdown("#hashtag\ntext", _headers()) == [("#hashtag\ntext", {})]


def test_markdown_headers_is_the_default_strategy():
    assert chunk_markdown("# H\nbody", ChunkingParams()) == [("body", {"headings": "H"})]


def test_unknown_strategy_is_rejected():
    with pytest.raises(ValueError, match="Unknown chunking strategy: semantic"):
        chunk_markdown("text", ChunkingParams(strategy="semantic"))
