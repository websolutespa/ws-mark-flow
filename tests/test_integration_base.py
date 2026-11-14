"""
Tests for ``src.integration.base``: FileInfo, IntegrationType, the abstract
contracts and the concrete helpers shared by every integration
(``__aenter__`` / ``__aexit__`` / ``download_files`` / ``upload_files``).

The helpers are exercised through the in-memory fakes from ``tests.helpers``,
which subclass the real ABCs without overriding the methods under test.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.integration.base import (
    CONVERTIBLE_EXTENSIONS,
    MARKDOWN_EXTENSIONS,
    BaseIntegration,
    DestinationIntegration,
    FileInfo,
    IntegrationType,
    SourceIntegration,
)

WINDOWS = sys.platform == "win32"

# FileInfo.path is documented as a remote-style path ("/folder/sub/file.pdf"),
# yet base.py derives parents with pathlib.Path, so the separator follows the
# host OS. The strict-POSIX expectations below therefore fail on Windows only.
posix_only = pytest.mark.xfail(
    WINDOWS,
    strict=True,
    reason="BUG: base.py builds remote-style paths with pathlib.Path, "
    "so they contain backslashes on Windows (should be PurePosixPath)",
)


# --- FileInfo ----------------------------------------------------------------

class TestFileInfo:
    def test_required_and_optional_fields(self):
        mtime = datetime(2026, 1, 15, 12, 30)
        info = FileInfo(name="report.pdf", path="/docs/report.pdf", modified_at=mtime)

        assert info.name == "report.pdf"
        assert info.path == "/docs/report.pdf"
        assert info.modified_at == mtime
        assert info.size is None
        assert info.content_type is None

    def test_all_fields_round_trip_through_model_dump(self):
        mtime = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)
        info = FileInfo(
            name="a.pdf", path="/a.pdf", modified_at=mtime, size=42, content_type="application/pdf"
        )

        dumped = info.model_dump()

        assert dumped == {
            "name": "a.pdf",
            "path": "/a.pdf",
            "modified_at": mtime,
            "size": 42,
            "content_type": "application/pdf",
        }
        assert FileInfo.model_validate(dumped) == info

    def test_field_order_is_stable(self):
        # src.main builds CSV headers from FileInfo.model_fields.
        assert list(FileInfo.model_fields) == ["name", "path", "modified_at", "size", "content_type"]

    def test_modified_at_is_parsed_from_iso_string(self):
        info = FileInfo(name="a.pdf", path="/a.pdf", modified_at="2026-01-15T12:30:00Z")

        assert info.modified_at == datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)

    @pytest.mark.parametrize("missing", ["name", "path", "modified_at"])
    def test_missing_required_field_is_rejected(self, missing):
        data = {"name": "a.pdf", "path": "/a.pdf", "modified_at": datetime(2026, 1, 1)}
        del data[missing]

        with pytest.raises(ValidationError) as exc:
            FileInfo(**data)

        assert exc.value.errors()[0]["loc"] == (missing,)

    def test_invalid_size_is_rejected(self):
        with pytest.raises(ValidationError):
            FileInfo(name="a.pdf", path="/a.pdf", modified_at=datetime(2026, 1, 1), size="big")

    @pytest.mark.parametrize(
        "name, stem, suffix",
        [
            ("report.pdf", "report", ".pdf"),
            ("REPORT.PDF", "REPORT", ".pdf"),
            ("Mixed.DocX", "Mixed", ".docx"),
            ("archive.tar.gz", "archive.tar", ".gz"),
            ("README", "README", ""),
            (".env", ".env", ""),
            ("my file (1).pdf", "my file (1)", ".pdf"),
        ],
    )
    def test_stem_and_suffix(self, file_info, name, stem, suffix):
        info = file_info(f"/docs/{name}", name=name)

        assert info.stem == stem
        assert info.suffix == suffix

    @pytest.mark.parametrize(
        "path, parent",
        [
            ("/docs/sub/report.pdf", "/docs/sub"),
            ("/report.pdf", "/"),
            ("report.pdf", "."),
            ("docs/report.pdf", "docs"),
        ],
    )
    def test_parent_path_points_to_the_containing_folder(self, file_info, path, parent):
        # OS-agnostic: compare as paths, not as strings.
        assert Path(file_info(path).parent_path) == Path(parent)

    @posix_only
    @pytest.mark.parametrize(
        "path, parent",
        [("/docs/sub/report.pdf", "/docs/sub"), ("/report.pdf", "/")],
    )
    def test_parent_path_is_posix_on_every_platform(self, file_info, path, parent):
        assert file_info(path).parent_path == parent

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/docs/sub/report.pdf", "/docs/sub/report.md"),
            ("/report.pdf", "/report.md"),
            ("report.pdf", "report.md"),
            ("/docs/archive.tar.gz", "/docs/archive.tar.md"),
            ("/docs/README", "/docs/README.md"),
            ("/docs/REPORT.PDF", "/docs/REPORT.md"),
        ],
    )
    def test_to_markdown_path_swaps_the_extension(self, file_info, path, expected):
        assert Path(file_info(path).to_markdown_path()) == Path(expected)

    @posix_only
    def test_to_markdown_path_is_posix_on_every_platform(self, file_info):
        assert file_info("/docs/sub/report.pdf").to_markdown_path() == "/docs/sub/report.md"

    def test_to_markdown_path_uses_name_for_the_stem(self):
        # The stem comes from ``name``, the folder from ``path``.
        info = FileInfo(name="Display Name.docx", path="/docs/ignored.docx", modified_at=datetime(2026, 1, 1))

        assert Path(info.to_markdown_path()) == Path("/docs/Display Name.md")


# --- IntegrationType / module constants --------------------------------------

def test_integration_type_values_are_stable():
    # These strings are persisted in job configurations: renaming one is a
    # breaking change.
    assert {t.name: t.value for t in IntegrationType} == {
        "SHAREPOINT": "sharepoint",
        "S3": "s3",
        "GCS": "gcs",
        "AZURE_BLOB": "azure_blob",
        "GOOGLE_DRIVE": "google_drive",
        "SFTP": "sftp",
        "LOCAL": "local",
        "SITEMAP": "sitemap",
    }


def test_integration_type_is_a_str_enum():
    assert IntegrationType("s3") is IntegrationType.S3
    assert IntegrationType.S3 == "s3"
    assert isinstance(IntegrationType.S3, str)
    with pytest.raises(ValueError):
        IntegrationType("dropbox")


def test_convertible_extensions_are_normalised():
    assert all(ext.startswith(".") and ext == ext.lower() for ext in CONVERTIBLE_EXTENSIONS)
    assert len(set(CONVERTIBLE_EXTENSIONS)) == len(CONVERTIBLE_EXTENSIONS)
    assert {".pdf", ".docx", ".pptx", ".xlsx", ".html", ".txt", ".png", ".jpg"} <= set(CONVERTIBLE_EXTENSIONS)
    # Markdown is the output format: converting it again would loop.
    assert ".md" not in CONVERTIBLE_EXTENSIONS


def test_markdown_extensions():
    assert MARKDOWN_EXTENSIONS == [".md"]


# --- Abstract contracts ------------------------------------------------------

class TestAbstractContracts:
    @pytest.mark.parametrize("cls", [BaseIntegration, SourceIntegration, DestinationIntegration])
    def test_abstract_classes_cannot_be_instantiated(self, cls):
        with pytest.raises(TypeError, match="abstract"):
            cls()

    def test_source_must_implement_every_abstract_member(self):
        class Incomplete(SourceIntegration):
            @property
            def integration_type(self):
                return IntegrationType.LOCAL

            async def connect(self):
                return True

            async def disconnect(self):
                return None

            async def list_files(self, extensions=None, folder_path=None):
                return []

        with pytest.raises(TypeError, match="download_file"):
            Incomplete()

    def test_destination_additionally_requires_upload_and_create_folder(self, fake_source):
        # FakeSource implements exactly the members a source needs.
        source_members = {
            name: getattr(fake_source, name)
            for name in ("integration_type", "connect", "disconnect", "list_files", "download_file")
        }
        NotADestination = type("NotADestination", (DestinationIntegration,), source_members)

        with pytest.raises(TypeError) as exc:
            NotADestination()

        assert "upload_file" in str(exc.value)
        assert "create_folder" in str(exc.value)

    def test_abstract_method_sets(self):
        common = {"integration_type", "connect", "disconnect", "list_files", "download_file"}
        assert BaseIntegration.__abstractmethods__ == common
        assert SourceIntegration.__abstractmethods__ == common
        assert DestinationIntegration.__abstractmethods__ == common | {"upload_file", "create_folder"}

    def test_source_and_destination_share_the_base(self):
        assert issubclass(SourceIntegration, BaseIntegration)
        assert issubclass(DestinationIntegration, BaseIntegration)
        assert not issubclass(SourceIntegration, DestinationIntegration)


# --- async context manager ---------------------------------------------------

class TestAsyncContextManager:
    async def test_enter_connects_and_exit_disconnects(self, fake_source):
        source = fake_source()

        async with source as entered:
            assert entered is source
            assert source.connected is True
            assert source.disconnect_calls == 0

        assert source.connect_calls == 1
        assert source.disconnect_calls == 1
        assert source.connected is False

    async def test_failed_connect_raises_and_names_the_integration(self, fake_destination):
        destination = fake_destination(connect_ok=False)

        with pytest.raises(RuntimeError, match="Failed to connect to local"):
            async with destination:
                pytest.fail("body must not run when connect() fails")  # pragma: no cover

        # __aexit__ is not invoked when __aenter__ raises.
        assert destination.disconnect_calls == 0

    async def test_exit_disconnects_and_propagates_body_errors(self, fake_source):
        source = fake_source()

        with pytest.raises(KeyError, match="boom"):
            async with source:
                raise KeyError("boom")

        assert source.disconnect_calls == 1


# --- SourceIntegration.download_files ----------------------------------------

class TestDownloadFiles:
    async def test_preserves_folder_structure_by_default(self, fake_source, file_info, tmp_path):
        source = fake_source({"/docs/a.pdf": b"AAA", "/docs/sub/b.docx": b"BB", "/c.txt": b"C"})
        files = [file_info(p) for p in source.files]

        result = await source.download_files(files, tmp_path)

        assert result == {
            "/docs/a.pdf": tmp_path / "docs" / "a.pdf",
            "/docs/sub/b.docx": tmp_path / "docs" / "sub" / "b.docx",
            "/c.txt": tmp_path / "c.txt",
        }
        assert (tmp_path / "docs" / "a.pdf").read_bytes() == b"AAA"
        assert (tmp_path / "docs" / "sub" / "b.docx").read_bytes() == b"BB"
        assert (tmp_path / "c.txt").read_bytes() == b"C"
        assert source.downloaded == ["/docs/a.pdf", "/docs/sub/b.docx", "/c.txt"]

    async def test_flat_layout_when_structure_is_not_preserved(self, fake_source, file_info, tmp_path):
        source = fake_source({"/docs/a.pdf": b"AAA", "/docs/sub/b.docx": b"BB"})
        files = [file_info(p) for p in source.files]

        result = await source.download_files(files, tmp_path, preserve_structure=False)

        assert result == {
            "/docs/a.pdf": tmp_path / "a.pdf",
            "/docs/sub/b.docx": tmp_path / "b.docx",
        }
        assert sorted(p.name for p in tmp_path.iterdir()) == ["a.pdf", "b.docx"]

    async def test_flat_layout_uses_file_name_not_path(self, fake_source, tmp_path):
        source = fake_source({"/docs/remote-id-123": b"X"})
        info = FileInfo(name="Pretty Name.pdf", path="/docs/remote-id-123", modified_at=datetime(2026, 1, 1))

        result = await source.download_files([info], tmp_path, preserve_structure=False)

        assert result == {"/docs/remote-id-123": tmp_path / "Pretty Name.pdf"}

    async def test_failed_downloads_are_left_out_of_the_result(self, fake_source, file_info, tmp_path):
        source = fake_source(
            {"/docs/ok.pdf": b"OK", "/broken/bad.pdf": b"BAD"},
            fail_download=["/broken/bad.pdf"],
        )
        files = [file_info("/docs/ok.pdf"), file_info("/broken/bad.pdf"), file_info("/gone/missing.pdf")]

        result = await source.download_files(files, tmp_path)

        assert result == {"/docs/ok.pdf": tmp_path / "docs" / "ok.pdf"}
        assert not (tmp_path / "broken" / "bad.pdf").exists()
        # The target folder is prepared before the download is attempted.
        assert (tmp_path / "broken").is_dir()

    async def test_creates_missing_local_directory(self, fake_source, file_info, tmp_path):
        source = fake_source({"/a.pdf": b"A"})
        target = tmp_path / "not" / "yet" / "there"

        result = await source.download_files([file_info("/a.pdf")], target)

        assert result == {"/a.pdf": target / "a.pdf"}
        assert (target / "a.pdf").read_bytes() == b"A"

    async def test_no_files_is_a_noop(self, fake_source, tmp_path):
        source = fake_source({"/a.pdf": b"A"})

        assert await source.download_files([], tmp_path) == {}
        assert list(tmp_path.iterdir()) == []
        assert source.downloaded == []


# --- DestinationIntegration.upload_files -------------------------------------

@pytest.fixture
def recording_destination(fake_destination):
    """A FakeDestination that records folder creations and uploads in order."""

    class Recording(fake_destination):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.events: list[tuple[str, str]] = []

        async def create_folder(self, folder_path: str) -> bool:
            self.events.append(("folder", folder_path))
            return await super().create_folder(folder_path)

        async def upload_file(self, local_path: Path, remote_path: str) -> bool:
            self.events.append(("upload", remote_path))
            return await super().upload_file(local_path, remote_path)

    return Recording


def _local_files(tmp_path: Path, *names: str) -> list[Path]:
    paths = []
    for name in names:
        path = tmp_path / name
        path.write_text(f"content of {name}", encoding="utf-8")
        paths.append(path)
    return paths


class TestUploadFiles:
    async def test_uploads_every_file_and_reports_status(self, recording_destination, tmp_path):
        a, b, c = _local_files(tmp_path, "a.md", "b.md", "c.md")
        destination = recording_destination(fail_upload=["/docs/b.md"])

        result = await destination.upload_files(
            {a: "/docs/a.md", b: "/docs/b.md", c: "/other/c.md"}
        )

        assert result == {"/docs/a.md": True, "/docs/b.md": False, "/other/c.md": True}
        assert destination.uploaded == ["/docs/a.md", "/other/c.md"]
        assert destination.files["/docs/a.md"] == b"content of a.md"
        assert destination.files["/other/c.md"] == b"content of c.md"
        assert "/docs/b.md" not in destination.files

    async def test_creates_each_parent_folder_once_sorted_before_uploading(
        self, recording_destination, tmp_path
    ):
        a, b, c, d = _local_files(tmp_path, "a.md", "b.md", "c.md", "d.md")
        destination = recording_destination()

        await destination.upload_files(
            {a: "/zeta/a.md", b: "/alpha/sub/b.md", c: "/alpha/c.md", d: "/alpha/sub/d.md"}
        )

        kinds = [kind for kind, _ in destination.events]
        assert kinds == ["folder"] * 3 + ["upload"] * 4
        # Parents come before their children; each folder is requested once.
        assert [Path(p) for p in destination.folders] == [
            Path("/alpha"),
            Path("/alpha/sub"),
            Path("/zeta"),
        ]
        assert [p for kind, p in destination.events if kind == "upload"] == [
            "/zeta/a.md",
            "/alpha/sub/b.md",
            "/alpha/c.md",
            "/alpha/sub/d.md",
        ]

    @posix_only
    async def test_folder_paths_are_posix_on_every_platform(self, recording_destination, tmp_path):
        (a,) = _local_files(tmp_path, "a.md")
        destination = recording_destination()

        await destination.upload_files({a: "/docs/sub/a.md"})

        assert destination.folders == ["/docs/sub"]

    async def test_files_without_a_parent_folder_need_no_folder(self, recording_destination, tmp_path):
        (a,) = _local_files(tmp_path, "a.md")
        destination = recording_destination()

        result = await destination.upload_files({a: "a.md"})

        assert result == {"a.md": True}
        assert destination.folders == []

    async def test_create_folders_can_be_disabled(self, recording_destination, tmp_path):
        a, b = _local_files(tmp_path, "a.md", "b.md")
        destination = recording_destination()

        result = await destination.upload_files(
            {a: "/docs/a.md", b: "/docs/sub/b.md"}, create_folders=False
        )

        assert result == {"/docs/a.md": True, "/docs/sub/b.md": True}
        assert destination.folders == []
        assert destination.events == [("upload", "/docs/a.md"), ("upload", "/docs/sub/b.md")]

    async def test_no_files_is_a_noop(self, recording_destination):
        destination = recording_destination()

        assert await destination.upload_files({}) == {}
        assert destination.events == []
