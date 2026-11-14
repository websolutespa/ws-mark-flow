"""Tests for ``release.py``: the version bump that precedes ``dist:check``."""
import io
import json
import urllib.error

import pytest

import release

SETUP_PY = '''setup(
    name="ws_mark_flow",
    version="0.0.17",
    python_requires=">=3.12",
)
'''


@pytest.mark.parametrize("version, expected", [("0.0.17", "0.0.18"), ("1.2.9", "1.2.10"), ("3", "4")])
def test_bump_patch(version, expected):
    assert release.bump_patch(version) == expected


@pytest.mark.parametrize("version", ["1.0.0rc1", "1.0.post1", ""])
def test_bump_patch_rejects_non_numeric_versions(version):
    with pytest.raises(ValueError):
        release.bump_patch(version)


def test_published_version_is_bumped_past_every_release(tmp_path):
    setup_py = tmp_path / "setup.py"
    setup_py.write_text(SETUP_PY, encoding="utf-8")

    assert release.update_setup_version(setup_py, {"0.0.16", "0.0.17", "0.0.18"}) == ("0.0.17", "0.0.19")
    assert setup_py.read_text(encoding="utf-8") == SETUP_PY.replace('"0.0.17"', '"0.0.19"')


def test_unpublished_version_is_kept(tmp_path):
    setup_py = tmp_path / "setup.py"
    setup_py.write_text(SETUP_PY, encoding="utf-8")

    assert release.update_setup_version(setup_py, {"0.0.16"}) == ("0.0.17", "0.0.17")
    assert setup_py.read_text(encoding="utf-8") == SETUP_PY


def test_dry_run_does_not_write(tmp_path):
    setup_py = tmp_path / "setup.py"
    setup_py.write_text(SETUP_PY, encoding="utf-8")

    assert release.update_setup_version(setup_py, {"0.0.17"}, dry_run=True) == ("0.0.17", "0.0.18")
    assert setup_py.read_text(encoding="utf-8") == SETUP_PY


def test_missing_version_is_an_error():
    with pytest.raises(ValueError, match="version"):
        release.read_version('setup(name="x")')


def test_published_versions_reads_pypi_releases(monkeypatch):
    payload = json.dumps({"releases": {"0.0.1": [], "0.0.2": []}}).encode()
    monkeypatch.setattr(release.urllib.request, "urlopen", lambda url, timeout: io.BytesIO(payload))

    assert release.published_versions("pkg") == {"0.0.1", "0.0.2"}


def test_never_published_project_has_no_versions(monkeypatch):
    def not_found(url, timeout):
        raise urllib.error.HTTPError(url, 404, "Not Found", None, None)

    monkeypatch.setattr(release.urllib.request, "urlopen", not_found)

    assert release.published_versions("pkg") == set()


def test_other_http_errors_are_not_mistaken_for_unpublished(monkeypatch):
    def unavailable(url, timeout):
        raise urllib.error.HTTPError(url, 503, "Service Unavailable", None, None)

    monkeypatch.setattr(release.urllib.request, "urlopen", unavailable)

    with pytest.raises(urllib.error.HTTPError):
        release.published_versions("pkg")
