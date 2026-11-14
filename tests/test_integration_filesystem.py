"""
Tests for ``src.integration.filesystem`` (local source + destination).

Everything runs against the real filesystem under ``tmp_path``; mocking is
used only to force error branches that cannot be produced portably
(PermissionError on a directory, a failing ``Path.resolve``).
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.integration.base import (
    DestinationIntegration,
    FileInfo,
    IntegrationType,
    SourceIntegration,
)
from src.integration.filesystem import (
    FilesystemConfig,
    FilesystemDestination,
    FilesystemSource,
)

WINDOWS = sys.platform == "win32"
LOGGER = "src.integration.filesystem"
MTIME = 1_700_000_000  # fixed timestamp applied to every file of the sample tree

NOT_CONNECTED = "Not connected"


def posix(path: str) -> str:
    """FileInfo.path with OS separators normalised (see the xfail test below)."""
    return path.replace(os.sep, "/")


def write(path: Path, content: bytes = b"data") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    os.utime(path, (MTIME, MTIME))
    return path


@pytest.fixture
def tree(tmp_path) -> Path:
    """
    root/
      a.pdf            (3 bytes)
      B.PDF
      notes.txt
      README
      docs/c.docx
      docs/deep/d.pdf
      docs/deep/e.md
      empty/
      .git/hidden.pdf
    """
    root = tmp_path / "root"
    write(root / "a.pdf", b"AAA")
    write(root / "B.PDF", b"BBBB")
    write(root / "notes.txt", b"notes")
    write(root / "README", b"readme")
    write(root / "docs" / "c.docx", b"docx")
    write(root / "docs" / "deep" / "d.pdf", b"deep pdf")
    write(root / "docs" / "deep" / "e.md", b"# md")
    (root / "empty").mkdir()
    write(root / ".git" / "hidden.pdf", b"hidden")
    return root


@pytest.fixture
async def source(tree) -> FilesystemSource:
    src = FilesystemSource({"path": str(tree)})
    assert await src.connect() is True
    return src


@pytest.fixture
async def destination(tmp_path) -> FilesystemDestination:
    dest = FilesystemDestination({"path": str(tmp_path / "out")})
    assert await dest.connect() is True
    return dest


def by_path(files: list[FileInfo]) -> dict[str, FileInfo]:
    return {posix(f.path): f for f in files}


# --- Config ------------------------------------------------------------------

class TestConfig:
    def test_path_is_required(self):
        with pytest.raises(ValidationError) as exc:
            FilesystemConfig.model_validate({})
        assert exc.value.errors()[0]["loc"] == ("path",)

    def test_unknown_keys_are_ignored(self):
        config = FilesystemConfig.model_validate({"path": "/data", "bucket": "x", "type": "local"})
        assert config.model_dump() == {"path": "/data"}

    @pytest.mark.parametrize("cls", [FilesystemSource, FilesystemDestination])
    def test_invalid_config_is_rejected_by_the_integration(self, cls):
        with pytest.raises(ValidationError):
            cls({"folder": "/data"})


class TestIdentity:
    def test_source(self, tmp_path):
        src = FilesystemSource({"path": str(tmp_path)})
        assert src.integration_type is IntegrationType.LOCAL
        assert isinstance(src, SourceIntegration)
        assert not isinstance(src, DestinationIntegration)

    def test_destination(self, tmp_path):
        dest = FilesystemDestination({"path": str(tmp_path)})
        assert dest.integration_type is IntegrationType.LOCAL
        assert isinstance(dest, DestinationIntegration)


# --- connect / disconnect ----------------------------------------------------

class TestSourceConnect:
    async def test_existing_directory(self, tree):
        src = FilesystemSource({"path": str(tree)})

        assert await src.connect() is True
        assert src._base_path == tree.resolve()

    async def test_relative_path_is_resolved_against_cwd(self, tree, monkeypatch):
        monkeypatch.chdir(tree.parent)
        src = FilesystemSource({"path": "root"})

        assert await src.connect() is True
        assert src._base_path == tree.resolve()
        assert src._base_path.is_absolute()

    async def test_missing_directory_fails_and_is_not_created(self, tmp_path):
        missing = tmp_path / "nope"
        src = FilesystemSource({"path": str(missing)})

        assert await src.connect() is False
        assert not missing.exists()

    async def test_path_to_a_file_fails(self, tree):
        src = FilesystemSource({"path": str(tree / "a.pdf")})

        assert await src.connect() is False

    async def test_unexpected_error_is_reported_as_failure(self, tree, monkeypatch, caplog):
        def boom(self, *args, **kwargs):
            raise OSError("disk on fire")

        monkeypatch.setattr(Path, "resolve", boom)
        src = FilesystemSource({"path": str(tree)})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await src.connect() is False
        assert "disk on fire" in caplog.text

    async def test_context_manager_raises_when_directory_is_missing(self, tmp_path):
        with pytest.raises(RuntimeError, match="Failed to connect to local"):
            async with FilesystemSource({"path": str(tmp_path / "nope")}):
                pytest.fail("must not enter")  # pragma: no cover

    async def test_context_manager_connects_and_disconnects(self, tree):
        src = FilesystemSource({"path": str(tree)})

        async with src as entered:
            assert entered is src
            assert len(await src.list_files(extensions=[".docx"])) == 1

        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await src.list_files()


class TestDestinationConnect:
    async def test_existing_directory(self, tmp_path):
        dest = FilesystemDestination({"path": str(tmp_path)})

        assert await dest.connect() is True
        assert dest._base_path == tmp_path.resolve()

    async def test_missing_directory_is_created_with_parents(self, tmp_path):
        target = tmp_path / "a" / "b" / "out"
        dest = FilesystemDestination({"path": str(target)})

        assert await dest.connect() is True
        assert target.is_dir()
        assert dest._base_path == target.resolve()

    async def test_path_to_a_file_fails(self, tmp_path):
        blocker = write(tmp_path / "file.txt")
        dest = FilesystemDestination({"path": str(blocker)})

        assert await dest.connect() is False
        assert blocker.is_file()

    async def test_directory_that_cannot_be_created_fails(self, tmp_path, caplog):
        blocker = write(tmp_path / "file.txt")
        dest = FilesystemDestination({"path": str(blocker / "sub")})

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await dest.connect() is False
        assert "Failed to connect to filesystem destination" in caplog.text


@pytest.mark.parametrize("cls", [FilesystemSource, FilesystemDestination])
class TestDisconnect:
    async def test_disconnect_without_connect_is_a_noop(self, cls, tmp_path):
        integration = cls({"path": str(tmp_path)})

        await integration.disconnect()

        assert integration._base_path is None

    async def test_disconnect_requires_reconnect(self, cls, tmp_path):
        integration = cls({"path": str(tmp_path)})
        assert await integration.connect() is True

        await integration.disconnect()
        await integration.disconnect()  # idempotent

        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await integration.list_files()

        assert await integration.connect() is True
        assert await integration.list_files() == []


# --- not-connected guards ----------------------------------------------------

class TestNotConnectedGuards:
    async def test_source_operations(self, tree, file_info, tmp_path):
        src = FilesystemSource({"path": str(tree)})

        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await src.list_files()
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await src.download_file(file_info("/a.pdf"), tmp_path / "a.pdf")
        assert not (tmp_path / "a.pdf").exists()

    async def test_destination_operations(self, tmp_path, file_info):
        local = write(tmp_path / "local.md")
        target = tmp_path / "out"
        dest = FilesystemDestination({"path": str(target)})

        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.list_files()
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.download_file(file_info("/a.md"), tmp_path / "a.md")
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.upload_file(local, "/a.md")
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.file_exists("/a.md")
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.delete_file("/a.md")
        with pytest.raises(RuntimeError, match=NOT_CONNECTED):
            await dest.create_folder("/docs")
        assert not target.exists()


# --- list_files --------------------------------------------------------------

class TestListFiles:
    async def test_lists_recursively_and_skips_hidden_directories(self, source):
        files = by_path(await source.list_files())

        assert set(files) == {
            "/a.pdf",
            "/B.PDF",
            "/notes.txt",
            "/README",
            "/docs/c.docx",
            "/docs/deep/d.pdf",
            "/docs/deep/e.md",
        }

    async def test_file_metadata(self, source):
        files = by_path(await source.list_files())

        a = files["/a.pdf"]
        assert a.name == "a.pdf"
        assert a.size == 3
        assert a.modified_at == datetime.fromtimestamp(MTIME)
        assert a.content_type is None

        deep = files["/docs/deep/d.pdf"]
        assert deep.name == "d.pdf"
        assert deep.size == len(b"deep pdf")
        assert Path(deep.parent_path) == Path("/docs/deep")

    @pytest.mark.xfail(
        WINDOWS,
        strict=True,
        reason="BUG: filesystem.py:74 builds FileInfo.path with str(Path), "
        "yielding '/docs\\\\c.docx' on Windows instead of a POSIX-style path",
    )
    async def test_paths_use_forward_slashes_on_every_platform(self, source):
        paths = {f.path for f in await source.list_files(extensions=[".docx", ".md"])}

        assert paths == {"/docs/c.docx", "/docs/deep/e.md"}

    @pytest.mark.parametrize(
        "extensions, expected",
        [
            ([".pdf"], {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}),
            ([".PDF"], {"/a.pdf", "/B.PDF", "/docs/deep/d.pdf"}),
            ([".docx", ".TXT"], {"/notes.txt", "/docs/c.docx"}),
            ([".md"], {"/docs/deep/e.md"}),
            ([".xlsx"], set()),
            (None, {"/a.pdf", "/B.PDF", "/notes.txt", "/README", "/docs/c.docx",
                    "/docs/deep/d.pdf", "/docs/deep/e.md"}),
        ],
    )
    async def test_extension_filter_is_case_insensitive(self, source, extensions, expected):
        files = await source.list_files(extensions=extensions)

        assert set(by_path(files)) == expected

    @pytest.mark.parametrize("folder", ["docs", "/docs", "docs/", "/docs/"])
    async def test_folder_path_limits_the_walk_but_paths_stay_relative_to_base(self, source, folder):
        files = await source.list_files(folder_path=folder)

        assert set(by_path(files)) == {"/docs/c.docx", "/docs/deep/d.pdf", "/docs/deep/e.md"}

    async def test_nested_folder_path_with_extension_filter(self, source):
        files = await source.list_files(extensions=[".pdf"], folder_path="/docs/deep")

        assert set(by_path(files)) == {"/docs/deep/d.pdf"}

    async def test_missing_folder_yields_nothing(self, source):
        assert await source.list_files(folder_path="/does/not/exist") == []

    async def test_empty_folder_yields_nothing(self, source):
        assert await source.list_files(folder_path="empty") == []

    async def test_folder_path_pointing_to_a_file_yields_nothing(self, source, caplog):
        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.list_files(folder_path="/a.pdf") == []
        assert "Error listing" in caplog.text

    async def test_unreadable_directory_is_skipped(self, source, tree, monkeypatch, caplog):
        real_iterdir = Path.iterdir

        def iterdir(self):
            if self.name == "deep":
                raise PermissionError("access denied")
            return real_iterdir(self)

        monkeypatch.setattr(Path, "iterdir", iterdir)

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            files = await source.list_files()

        assert set(by_path(files)) == {"/a.pdf", "/B.PDF", "/notes.txt", "/README", "/docs/c.docx"}
        assert "Permission denied" in caplog.text
        assert "deep" in caplog.text

    async def test_destination_lists_its_own_files(self, destination, tmp_path):
        base = tmp_path / "out"
        write(base / "docs" / "a.md", b"# a")
        write(base / "docs" / "a.pdf", b"pdf")

        files = await destination.list_files(extensions=[".md"])

        assert [posix(f.path) for f in files] == ["/docs/a.md"]
        assert files[0].name == "a.md"
        assert files[0].size == 3

    async def test_listing_reflects_changes_between_calls(self, source, tree):
        before = set(by_path(await source.list_files(extensions=[".txt"])))
        write(tree / "new" / "added.txt")
        (tree / "notes.txt").unlink()

        after = set(by_path(await source.list_files(extensions=[".txt"])))

        assert before == {"/notes.txt"}
        assert after == {"/new/added.txt"}


# --- download_file -----------------------------------------------------------

class TestDownloadFile:
    async def test_copies_content_and_creates_parent_folders(self, source, file_info, tmp_path):
        local = tmp_path / "work" / "nested" / "d.pdf"

        assert await source.download_file(file_info("/docs/deep/d.pdf"), local) is True

        assert local.read_bytes() == b"deep pdf"
        # shutil.copy2 keeps the modification time.
        assert int(local.stat().st_mtime) == MTIME

    async def test_every_listed_file_can_be_downloaded(self, source, tree, tmp_path):
        files = await source.list_files()
        assert len(files) == 7

        for info in files:
            local = tmp_path / "dl" / posix(info.path).lstrip("/")
            assert await source.download_file(info, local) is True
            assert local.read_bytes() == (tree / posix(info.path).lstrip("/")).read_bytes()

    async def test_path_without_leading_slash(self, source, file_info, tmp_path):
        local = tmp_path / "a.pdf"

        assert await source.download_file(file_info("a.pdf"), local) is True
        assert local.read_bytes() == b"AAA"

    async def test_overwrites_existing_local_file(self, source, file_info, tmp_path):
        local = write(tmp_path / "a.pdf", b"stale content")

        assert await source.download_file(file_info("/a.pdf"), local) is True
        assert local.read_bytes() == b"AAA"

    async def test_missing_source_file(self, source, file_info, tmp_path, caplog):
        local = tmp_path / "work" / "ghost.pdf"

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await source.download_file(file_info("/ghost.pdf"), local) is False

        assert not local.exists()
        assert "Source file not found" in caplog.text

    async def test_copy_error_is_reported_as_failure(self, source, file_info, tmp_path, caplog):
        blocker = write(tmp_path / "blocker.txt")

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            # The parent of the target is a regular file: mkdir must fail.
            assert await source.download_file(file_info("/a.pdf"), blocker / "a.pdf") is False

        assert blocker.read_bytes() == b"data"
        assert "Failed to copy file /a.pdf" in caplog.text

    async def test_destination_can_download_too(self, destination, file_info, tmp_path):
        write(tmp_path / "out" / "docs" / "a.md", b"# a")
        local = tmp_path / "dl" / "a.md"

        assert await destination.download_file(file_info("/docs/a.md"), local) is True
        assert local.read_bytes() == b"# a"

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: filesystem.py:96 joins FileInfo.path onto the base directory without "
        "confinement, so '..' segments read files outside the configured path",
    )
    async def test_cannot_read_outside_the_base_directory(self, source, tree, file_info, tmp_path):
        write(tree.parent / "secret.txt", b"top secret")
        local = tmp_path / "dl" / "secret.txt"

        ok = await source.download_file(file_info("/../secret.txt"), local)

        assert ok is False
        assert not local.exists()


# --- upload_file -------------------------------------------------------------

class TestUploadFile:
    @pytest.mark.parametrize("remote", ["/docs/sub/a.md", "docs/sub/a.md"])
    async def test_copies_file_and_creates_parent_folders(self, destination, tmp_path, remote):
        local = write(tmp_path / "local.md", b"# hello")

        assert await destination.upload_file(local, remote) is True

        uploaded = tmp_path / "out" / "docs" / "sub" / "a.md"
        assert uploaded.read_bytes() == b"# hello"
        assert int(uploaded.stat().st_mtime) == MTIME
        assert local.exists()  # copied, not moved

    async def test_file_at_the_root(self, destination, tmp_path):
        local = write(tmp_path / "local.md", b"root")

        assert await destination.upload_file(local, "/a.md") is True
        assert (tmp_path / "out" / "a.md").read_bytes() == b"root"

    async def test_overwrites_existing_file(self, destination, tmp_path):
        write(tmp_path / "out" / "a.md", b"old")
        local = write(tmp_path / "local.md", b"new")

        assert await destination.upload_file(local, "/a.md") is True
        assert (tmp_path / "out" / "a.md").read_bytes() == b"new"

    async def test_accepts_local_path_as_string(self, destination, tmp_path):
        local = write(tmp_path / "local.md", b"str path")

        assert await destination.upload_file(str(local), "/a.md") is True
        assert (tmp_path / "out" / "a.md").read_bytes() == b"str path"

    async def test_missing_local_file(self, destination, tmp_path, caplog):
        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await destination.upload_file(tmp_path / "ghost.md", "/docs/a.md") is False

        assert not (tmp_path / "out" / "docs" / "a.md").exists()
        assert "Failed to upload file to /docs/a.md" in caplog.text

    async def test_uploaded_file_is_listed_and_exists(self, destination, tmp_path):
        local = write(tmp_path / "local.md", b"# hello")

        assert await destination.upload_file(local, "/docs/a.md") is True

        assert await destination.file_exists("/docs/a.md") is True
        listed = await destination.list_files(extensions=[".md"])
        assert [posix(f.path) for f in listed] == ["/docs/a.md"]

    async def test_upload_files_helper_creates_folders_and_uploads(self, destination, tmp_path):
        a = write(tmp_path / "a.md", b"A")
        b = write(tmp_path / "b.md", b"B")

        result = await destination.upload_files({a: "/x/a.md", b: "/x/y/b.md"})

        assert result == {"/x/a.md": True, "/x/y/b.md": True}
        assert (tmp_path / "out" / "x" / "a.md").read_bytes() == b"A"
        assert (tmp_path / "out" / "x" / "y" / "b.md").read_bytes() == b"B"

    @pytest.mark.xfail(
        strict=True,
        reason="BUG: filesystem.py:158 joins remote_path onto the base directory without "
        "confinement, so '..' segments write files outside the configured path",
    )
    async def test_cannot_write_outside_the_base_directory(self, destination, tmp_path):
        local = write(tmp_path / "local.md", b"escaped")

        ok = await destination.upload_file(local, "/../escaped.md")

        assert not (tmp_path / "escaped.md").exists()
        assert ok is False


# --- file_exists / delete_file / create_folder -------------------------------

class TestFileExists:
    async def test_existing_and_missing(self, destination, tmp_path):
        write(tmp_path / "out" / "docs" / "a.md")

        assert await destination.file_exists("/docs/a.md") is True
        assert await destination.file_exists("docs/a.md") is True
        assert await destination.file_exists("/docs/b.md") is False
        assert await destination.file_exists("/nope/a.md") is False


class TestDeleteFile:
    async def test_deletes_existing_file(self, destination, tmp_path):
        target = write(tmp_path / "out" / "docs" / "a.md")
        sibling = write(tmp_path / "out" / "docs" / "b.md")

        assert await destination.delete_file("/docs/a.md") is True

        assert not target.exists()
        assert sibling.exists()
        assert await destination.file_exists("/docs/a.md") is False

    async def test_missing_file_counts_as_deleted(self, destination):
        assert await destination.delete_file("/docs/ghost.md") is True

    async def test_failure_is_reported(self, destination, tmp_path, caplog):
        folder = tmp_path / "out" / "docs"
        folder.mkdir()

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            # unlink() cannot remove a directory.
            assert await destination.delete_file("/docs") is False

        assert folder.is_dir()
        assert "Failed to delete file /docs" in caplog.text


class TestCreateFolder:
    @pytest.mark.parametrize("folder", ["/docs/sub/deep", "docs/sub/deep"])
    async def test_creates_nested_folders(self, destination, tmp_path, folder):
        assert await destination.create_folder(folder) is True

        assert (tmp_path / "out" / "docs" / "sub" / "deep").is_dir()

    async def test_existing_folder_is_kept(self, destination, tmp_path):
        existing = write(tmp_path / "out" / "docs" / "a.md")

        assert await destination.create_folder("/docs") is True
        assert await destination.create_folder("/docs") is True

        assert existing.read_bytes() == b"data"

    async def test_root_folder(self, destination, tmp_path):
        assert await destination.create_folder("/") is True
        assert (tmp_path / "out").is_dir()

    @pytest.mark.parametrize("folder", ["/file.txt", "/file.txt/sub"])
    async def test_failure_is_reported(self, destination, tmp_path, caplog, folder):
        blocker = write(tmp_path / "out" / "file.txt")

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            assert await destination.create_folder(folder) is False

        assert blocker.is_file()
        assert f"Failed to create folder {folder}" in caplog.text
