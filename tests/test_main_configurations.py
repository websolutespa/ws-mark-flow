"""API tests: integration schemas, saved configurations, exports and job-from-config."""
import csv
import io
import logging
import zipfile
from pathlib import Path
from urllib.parse import quote

import pytest

from src import main
from src.integration import IntegrationType
from src.models import (
    INTEGRATION_SCHEMAS,
    JobCreateRequest,
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
)
from tests.helpers import FakeDestination, FakeSource
from tests.main_support import CONFIG_ID, DEST_CONFIG_ID, JOB_ID, make_config, make_job

CONFIG_BODY = {"name": "My Source", "type": "local", "config": {"path": "/data/in"}}


def make_dest_config(**overrides):
    data = {"_id": DEST_CONFIG_ID, "name": "My Dest", "is_source": False, "config": {"path": "/data/out"}}
    data.update(overrides)
    return make_config(**data)


# -------------------------------------------------------------------- schemas

def test_list_schemas(client):
    body = client.get("/schemas").json()

    assert {s["type"] for s in body} == {t.value for t in INTEGRATION_SCHEMAS}
    local = next(s for s in body if s["type"] == "local")
    assert local["name"] == "Local Filesystem"
    assert local["fields"][0]["name"] == "path"
    assert local["fields"][0]["required"] is True


def test_get_schema(client):
    body = client.get("/schemas/sharepoint").json()

    assert body["type"] == "sharepoint"
    required = {f["name"] for f in body["fields"] if f["required"]}
    assert required == {"tenant_id", "client_id", "client_secret", "site_id", "drive_id"}
    secret = next(f for f in body["fields"] if f["name"] == "client_secret")
    assert secret["field_type"] == "password"


def test_get_schema_invalid_type_is_400(client):
    response = client.get("/schemas/ftp")
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid integration type: ftp"}


def test_get_schema_for_type_without_schema_is_404(client, monkeypatch):
    monkeypatch.delitem(INTEGRATION_SCHEMAS, IntegrationType.SFTP)
    response = client.get("/schemas/sftp")
    assert response.status_code == 404
    assert response.json() == {"detail": "Schema not found for sftp"}


@pytest.mark.xfail(strict=True, reason="BUG: GCS is a supported source/destination but has no UI schema")
def test_every_supported_integration_has_a_schema(client):
    supported = set(client.get("/info").json()["supported_sources"])
    with_schema = {s["type"] for s in client.get("/schemas").json()}
    assert supported <= with_schema


# ------------------------------------------------------- configuration CRUD

def test_create_configuration(client, config_storage):
    config_storage.create.return_value = make_config()

    response = client.post("/configurations", json=CONFIG_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["_id"] == CONFIG_ID
    assert (body["name"], body["type"], body["is_source"]) == ("My Source", "local", True)
    request = config_storage.create.await_args.args[0]
    assert isinstance(request, SavedConfigurationCreate)
    assert request.config == {"path": "/data/in"}
    assert request.is_source is True


def test_create_configuration_validates_the_body(client, config_storage):
    assert client.post("/configurations", json={"name": "x", "type": "local"}).status_code == 422
    assert client.post("/configurations", json={**CONFIG_BODY, "type": "ftp"}).status_code == 422
    config_storage.create.assert_not_awaited()


def test_create_configuration_requires_storage(client):
    response = client.post("/configurations", json=CONFIG_BODY)
    assert response.status_code == 503
    assert response.json() == {"detail": "Configuration storage not available"}


def test_list_configurations_is_empty_without_storage(client):
    assert client.get("/configurations").json() == []
    assert client.get("/configurations/sources").json() == []
    assert client.get("/configurations/destinations").json() == []


def test_list_configurations_defaults(client, config_storage):
    config_storage.list_all.return_value = [make_config(), make_dest_config()]

    body = client.get("/configurations").json()

    assert [c["name"] for c in body] == ["My Source", "My Dest"]
    config_storage.list_all.assert_awaited_once_with(
        is_source=None, integration_type=None, limit=100, offset=0
    )


def test_list_configurations_with_filters(client, config_storage):
    config_storage.list_all.return_value = []

    client.get(
        "/configurations",
        params={"is_source": "false", "integration_type": "s3", "limit": 7, "offset": 3},
    )

    config_storage.list_all.assert_awaited_once_with(
        is_source=False, integration_type="s3", limit=7, offset=3
    )


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 501}, {"offset": -1}])
def test_list_configurations_rejects_out_of_range_paging(client, config_storage, params):
    assert client.get("/configurations", params=params).status_code == 422


def test_list_source_and_destination_configurations(client, config_storage):
    config_storage.list_sources.return_value = [make_config()]
    config_storage.list_destinations.return_value = [make_dest_config()]

    sources = client.get("/configurations/sources", params={"integration_type": "local"}).json()
    destinations = client.get("/configurations/destinations").json()

    assert [c["_id"] for c in sources] == [CONFIG_ID]
    assert [c["_id"] for c in destinations] == [DEST_CONFIG_ID]
    config_storage.list_sources.assert_awaited_once_with(integration_type="local")
    config_storage.list_destinations.assert_awaited_once_with(integration_type=None)


def test_get_configuration(client, config_storage):
    config_storage.get.return_value = make_config()

    response = client.get(f"/configurations/{CONFIG_ID}")

    assert response.status_code == 200
    assert response.json()["_id"] == CONFIG_ID
    config_storage.get.assert_awaited_once_with(CONFIG_ID)


def test_get_configuration_not_found(client, config_storage):
    config_storage.get.return_value = None
    response = client.get(f"/configurations/{CONFIG_ID}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Configuration not found"}


def test_update_configuration(client, config_storage):
    config_storage.update.return_value = make_config(name="Renamed")

    response = client.patch(f"/configurations/{CONFIG_ID}", json={"name": "Renamed"})

    assert response.status_code == 200
    assert response.json()["name"] == "Renamed"
    config_id, update = config_storage.update.await_args.args
    assert config_id == CONFIG_ID
    assert isinstance(update, SavedConfigurationUpdate)
    assert update.model_dump(exclude_none=True) == {"name": "Renamed"}


def test_update_configuration_not_found(client, config_storage):
    config_storage.update.return_value = None
    assert client.patch(f"/configurations/{CONFIG_ID}", json={"name": "x"}).status_code == 404


def test_delete_configuration(client, config_storage):
    config_storage.delete.return_value = True

    response = client.delete(f"/configurations/{CONFIG_ID}")

    assert response.status_code == 200
    assert response.json() == {"message": "Configuration deleted"}
    config_storage.delete.assert_awaited_once_with(CONFIG_ID)


def test_delete_configuration_not_found(client, config_storage):
    config_storage.delete.return_value = False
    assert client.delete(f"/configurations/{CONFIG_ID}").status_code == 404


@pytest.mark.parametrize(
    "method, path, body",
    [
        ("GET", f"/configurations/{CONFIG_ID}", None),
        ("PATCH", f"/configurations/{CONFIG_ID}", {"name": "x"}),
        ("DELETE", f"/configurations/{CONFIG_ID}", None),
        ("POST", f"/configurations/{CONFIG_ID}/test", None),
        ("GET", f"/configurations/{CONFIG_ID}/test/download?format=csv", None),
        ("GET", "/configurations/export", None),
        ("POST", "/configurations/import", []),
    ],
)
def test_configuration_endpoints_require_storage(client, method, path, body):
    response = client.request(method, path, json=body)
    assert response.status_code == 503
    assert response.json() == {"detail": "Configuration storage not available"}


# ------------------------------------------------------------ import / export

def test_export_configurations(client, config_storage):
    config_storage.list_all.return_value = [
        make_config(description="primary"),
        make_dest_config(),
    ]

    response = client.get("/configurations/export", params={"is_source": "true"})

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "My Source",
            "description": "primary",
            "type": "local",
            "config": {"path": "/data/in"},
            "is_source": True,
        },
        {
            "name": "My Dest",
            "description": None,
            "type": "local",
            "config": {"path": "/data/out"},
            "is_source": False,
        },
    ]
    config_storage.list_all.assert_awaited_once_with(is_source=True, limit=1000, offset=0)


def test_export_is_not_shadowed_by_the_id_route(client, config_storage):
    """`/configurations/export` must not be captured by `/configurations/{config_id}`."""
    config_storage.list_all.return_value = []
    assert client.get("/configurations/export").json() == []
    config_storage.get.assert_not_awaited()


def test_import_creates_new_and_skips_existing_and_reports_errors(client, config_storage):
    config_storage.list_all.return_value = [make_config(name="Existing", is_source=True)]
    config_storage.create.side_effect = [make_config(), make_config(), RuntimeError("write failed")]

    response = client.post(
        "/configurations/import",
        json=[
            {"name": "Existing", "type": "local", "config": {"path": "/x"}},  # skipped
            {"name": "Existing", "type": "local", "config": {"path": "/x"}, "is_source": False},
            {"name": "New", "description": "d", "type": "s3", "config": {"bucket": "b"}},
            {"name": "New", "type": "s3", "config": {}},  # duplicate inside the payload
            {"name": "Bad type", "type": "ftp", "config": {}},
            {"name": "Storage error", "type": "local", "config": {}},
        ],
    )

    assert response.status_code == 200
    body = response.json()
    assert body["created"] == 2
    assert body["skipped"] == 2
    assert [e["name"] for e in body["errors"]] == ["Bad type", "Storage error"]
    assert "ftp" in body["errors"][0]["error"]
    assert body["errors"][1]["error"] == "write failed"

    config_storage.list_all.assert_awaited_once_with(limit=10000, offset=0)
    created = [call.args[0] for call in config_storage.create.await_args_list]
    assert [(c.name, c.is_source) for c in created] == [
        ("Existing", False),
        ("New", True),
        ("Storage error", True),
    ]
    assert (created[1].type, created[1].description, created[1].config) == ("s3", "d", {"bucket": "b"})


def test_import_validates_items(client, config_storage):
    assert client.post("/configurations/import", json=[{"name": "no type"}]).status_code == 422
    config_storage.create.assert_not_awaited()


# --------------------------------------------------------- test a saved config

def test_test_configuration_for_a_source(client, config_storage, integrations):
    config_storage.get.return_value = make_config()
    files = {f"/in/f{i:02d}.pdf": b"abc" for i in range(11)}
    files["/in/ignored.md"] = b"-"
    integrations.source = FakeSource(files)

    body = client.post(f"/configurations/{CONFIG_ID}/test").json()

    assert body["success"] is True
    assert body["message"] == "Successfully connected. Found 11 files."
    assert len(body["sample_files"]) == 10
    assert body["sample_files"][0] == {
        "name": "f00.pdf",
        "path": "/in/f00.pdf",
        "modified_at": "2026-01-15T12:30:00",
        "size": 3,
        "content_type": None,
    }
    assert integrations.calls == [("source", IntegrationType.LOCAL, {"path": "/data/in"})]


def test_test_configuration_for_a_destination_lists_markdown(client, config_storage, integrations):
    config_storage.get.return_value = make_dest_config()
    integrations.destination = FakeDestination({"/out/a.md": b"#", "/out/raw.pdf": b"x"})

    body = client.post(f"/configurations/{DEST_CONFIG_ID}/test").json()

    assert body["success"] is True
    assert [f["path"] for f in body["sample_files"]] == ["/out/a.md"]
    assert integrations.calls[0][0] == "destination"


def test_test_configuration_reports_failure_in_the_payload(client, config_storage, integrations):
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource(connect_ok=False)

    response = client.post(f"/configurations/{CONFIG_ID}/test")

    assert response.status_code == 200
    assert response.json() == {
        "success": False,
        "message": "Failed to connect to local",
        "sample_files": [],
    }


def test_test_configuration_not_found(client, config_storage):
    config_storage.get.return_value = None
    assert client.post(f"/configurations/{CONFIG_ID}/test").status_code == 404


# ------------------------------------------------------------- CSV download

def _download(client, fmt, config_id=CONFIG_ID):
    return client.get(f"/configurations/{config_id}/test/download", params={"format": fmt})


def test_download_csv(client, config_storage, integrations):
    config_storage.get.return_value = make_config(name="dhl fs CSTEAMS")
    integrations.source = FakeSource(
        {"/in/sub/Report.PDF": b"12345", "/top.docx": b"1", "/in/notes.md": b"skip"}
    )

    response = _download(client, "csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    # whitespace is stripped from the connection name
    assert response.headers["content-disposition"] == 'attachment; filename="dhlfsCSTEAMS_fileinfo.csv"'
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert list(rows[0]) == [
        "name", "path", "modified_at", "size", "content_type", "stem", "suffix", "parent_path",
    ]
    assert [r["path"] for r in rows] == ["/in/sub/Report.PDF", "/top.docx"]
    first = rows[0]
    assert first["name"] == "Report.PDF"
    assert first["modified_at"] == "2026-01-15T12:30:00"
    assert first["size"] == "5"
    assert first["content_type"] == ""  # missing optional value -> empty cell
    assert first["stem"] == "Report"
    assert first["suffix"] == ".pdf"
    assert first["parent_path"] == str(Path("/in/sub"))
    assert integrations.source.disconnect_calls == 1


def test_download_csv_for_empty_listing_still_has_a_header(client, config_storage, integrations):
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource()

    response = _download(client, "csv")

    assert response.status_code == 200
    assert response.text.strip() == "name,path,modified_at,size,content_type,stem,suffix,parent_path"


def test_download_csv_for_destination_lists_markdown_only(client, config_storage, integrations):
    config_storage.get.return_value = make_dest_config()
    integrations.destination = FakeDestination({"/a.md": b"#", "/b.pdf": b"x"})

    rows = list(csv.DictReader(io.StringIO(_download(client, "csv", DEST_CONFIG_ID).text)))

    assert [r["name"] for r in rows] == ["a.md"]


def test_download_filename_replaces_unsafe_characters(client, config_storage, integrations):
    config_storage.get.return_value = make_config(name='a<b>c:d"e/f\\g|h?i*j\tk\x01l')
    integrations.source = FakeSource()

    response = _download(client, "csv")

    assert response.headers["content-disposition"] == (
        'attachment; filename="a_b_c_d_e_f_g_h_i_jk_l_fileinfo.csv"'
    )


def test_download_filename_with_non_ascii_name_uses_rfc5987_encoding(client, config_storage, integrations):
    config_storage.get.return_value = make_config(name="Città Æ")
    integrations.source = FakeSource()

    response = _download(client, "csv")

    expected = quote("CittàÆ_fileinfo.csv", safe="")
    assert response.headers["content-disposition"] == f"attachment; filename*=UTF-8''{expected}"


# ------------------------------------------------------------- ZIP download

def _zip_entries(response) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.testzip() is None
        return {name: archive.read(name) for name in archive.namelist()}


def test_download_zip_preserves_relative_folders(client, config_storage, integrations):
    config_storage.get.return_value = make_config(name="My Source")
    source = FakeSource(
        {
            "/in/sub/report.pdf": b"PDF-BYTES",
            "\\win\\style.docx": b"DOCX",
            "top.txt": b"TOP",
            "/in/skip.md": b"not convertible",
        }
    )
    integrations.source = source

    response = _download(client, "zip")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert 'filename="MySource_files.zip"' in response.headers["content-disposition"]
    assert _zip_entries(response) == {
        "in/sub/report.pdf": b"PDF-BYTES",
        "win/style.docx": b"DOCX",
        "top.txt": b"TOP",
    }
    assert source.disconnect_calls == 1


def test_download_zip_of_empty_listing_is_a_valid_empty_archive(client, config_storage, integrations):
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource()

    response = _download(client, "zip")

    assert response.status_code == 200
    assert _zip_entries(response) == {}


def test_download_zip_cleans_up_its_temporary_directory(client, config_storage, integrations, monkeypatch):
    created = []
    real_tempdir = main.TemporaryDirectory

    def tracking_tempdir(*args, **kwargs):
        temporary = real_tempdir(*args, **kwargs)
        created.append(Path(temporary.name))
        return temporary

    monkeypatch.setattr(main, "TemporaryDirectory", tracking_tempdir)
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource({"/a.pdf": b"x"})

    assert _download(client, "zip").status_code == 200

    assert len(created) == 1
    assert created[0].name.startswith("configuration_download_")
    assert not created[0].exists()  # removed by the response's background task


@pytest.mark.parametrize(
    "files, offending",
    [
        ({"/ok.pdf": b"1", "/../escape.pdf": b"2"}, "/../escape.pdf"),
        ({"/a/../../b.pdf": b"1"}, "/a/../../b.pdf"),
        ({"C:/windows/evil.pdf": b"1"}, "C:/windows/evil.pdf"),
        ({"/": b"1"}, "/"),
        ({"/dup.pdf": b"1", "dup.pdf": b"2"}, "dup.pdf"),
    ],
)
def test_download_zip_rejects_unsafe_or_duplicate_paths(
    client, config_storage, integrations, monkeypatch, files, offending
):
    created = []
    real_tempdir = main.TemporaryDirectory

    def tracking_tempdir(*args, **kwargs):
        temporary = real_tempdir(*args, **kwargs)
        created.append(Path(temporary.name))
        return temporary

    monkeypatch.setattr(main, "TemporaryDirectory", tracking_tempdir)
    config_storage.get.return_value = make_config()
    source = FakeSource(files)

    async def list_everything(extensions=None, folder_path=None):
        from tests.helpers import make_file_info
        return [make_file_info(path, name="x.pdf") for path in files]

    source.list_files = list_everything
    integrations.source = source

    response = _download(client, "zip")

    assert response.status_code == 502
    assert response.json() == {
        "detail": f"File export failed: Unsafe or duplicate file path: {offending}"
    }
    # failure must not leak the staging directory
    assert created and not created[0].exists()


def test_download_zip_fails_rather_than_returning_a_partial_archive(
    client, config_storage, integrations, caplog
):
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource(
        {"/a.pdf": b"1", "/b.pdf": b"2"}, fail_download={"/b.pdf"}
    )

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = _download(client, "zip")

    assert response.status_code == 502
    assert response.json() == {"detail": "File export failed: Failed to download file: /b.pdf"}
    assert f"File export failed for configuration {CONFIG_ID}" in caplog.text


def test_download_connection_failure_is_502(client, config_storage, integrations):
    config_storage.get.return_value = make_config()
    integrations.source = FakeSource(connect_ok=False)

    response = _download(client, "csv")

    assert response.status_code == 502
    assert response.json() == {"detail": "File export failed: Failed to connect to local"}


def test_download_requires_a_known_format(client, config_storage):
    config_storage.get.return_value = make_config()
    assert client.get(f"/configurations/{CONFIG_ID}/test/download").status_code == 422
    assert _download(client, "tar").status_code == 422


def test_download_configuration_not_found(client, config_storage):
    config_storage.get.return_value = None
    assert _download(client, "zip").status_code == 404


# ------------------------------------------------------ job from configurations

FROM_CONFIGS_BODY = {
    "name": "from configs",
    "source_config_id": CONFIG_ID,
    "destination_config_id": DEST_CONFIG_ID,
}


def _configs(**by_id):
    async def get(config_id):
        return by_id.get(config_id)
    return get


def test_job_from_configs(client, storage, config_storage, scheduler):
    config_storage.get.side_effect = _configs(
        **{CONFIG_ID: make_config(type="s3", config={"bucket": "src"}), DEST_CONFIG_ID: make_dest_config()}
    )
    storage.create_job.return_value = make_job(name="from configs")

    response = client.post(
        "/jobs/from-configs",
        json={
            **FROM_CONFIGS_BODY,
            "description": "d",
            "source_extensions": [".pdf"],
            "source_folder": "in",
            "destination_folder": "out",
            "conversion_strategy": "balanced",
            "batch_size": 2,
            "llm_settings": {"llm_model": "m"},
        },
    )

    assert response.status_code == 200
    assert response.json()["_id"] == JOB_ID
    request = storage.create_job.await_args.args[0]
    assert isinstance(request, JobCreateRequest)
    assert request.name == "from configs"
    assert request.description == "d"
    assert (request.source.type, request.source.config) == ("s3", {"bucket": "src"})
    assert (request.destination.type, request.destination.config) == ("local", {"path": "/data/out"})
    assert request.source_extensions == [".pdf"]
    assert (request.source_folder, request.destination_folder) == ("in", "out")
    assert request.conversion_strategy == "balanced"
    assert request.batch_size == 2
    assert request.llm_settings.llm_model == "m"
    assert [c.args for c in config_storage.increment_use_count.await_args_list] == [
        (CONFIG_ID,),
        (DEST_CONFIG_ID,),
    ]
    scheduler.add_schedule.assert_not_awaited()


def test_job_from_configs_registers_schedule(client, storage, config_storage, scheduler):
    config_storage.get.side_effect = _configs(
        **{CONFIG_ID: make_config(), DEST_CONFIG_ID: make_dest_config()}
    )
    storage.create_job.return_value = make_job(schedule_enabled=True, schedule_cron="0 3 * * 1")

    response = client.post(
        "/jobs/from-configs",
        json={**FROM_CONFIGS_BODY, "schedule_cron": "0 3 * * 1", "schedule_enabled": True},
    )

    assert response.status_code == 200
    request = storage.create_job.await_args.args[0]
    assert (request.schedule_cron, request.schedule_enabled) == ("0 3 * * 1", True)
    assert scheduler.add_schedule.await_args.kwargs["id"] == f"job-{JOB_ID}"


def test_job_from_configs_survives_an_invalid_cron_expression(
    client, storage, config_storage, scheduler, caplog
):
    config_storage.get.side_effect = _configs(
        **{CONFIG_ID: make_config(), DEST_CONFIG_ID: make_dest_config()}
    )
    storage.create_job.return_value = make_job(schedule_enabled=True, schedule_cron="nope")

    with caplog.at_level(logging.ERROR, logger="src.main"):
        response = client.post("/jobs/from-configs", json=FROM_CONFIGS_BODY)

    assert response.status_code == 200
    assert "Failed to create schedule for new job" in caplog.text
    assert config_storage.increment_use_count.await_count == 2


@pytest.mark.parametrize(
    "configs, status, detail",
    [
        ({}, 404, "Source configuration not found"),
        ({CONFIG_ID: make_config(is_source=False)}, 400, "Selected configuration is not a source"),
        ({CONFIG_ID: make_config()}, 404, "Destination configuration not found"),
        (
            {CONFIG_ID: make_config(), DEST_CONFIG_ID: make_config(_id=DEST_CONFIG_ID, is_source=True)},
            400,
            "Selected configuration is not a destination",
        ),
    ],
)
def test_job_from_configs_validates_the_selected_configurations(
    client, storage, config_storage, configs, status, detail
):
    config_storage.get.side_effect = _configs(**configs)

    response = client.post("/jobs/from-configs", json=FROM_CONFIGS_BODY)

    assert response.status_code == status
    assert response.json() == {"detail": detail}
    storage.create_job.assert_not_awaited()
    config_storage.increment_use_count.assert_not_awaited()


def test_job_from_configs_requires_both_storages(client, storage):
    # job storage present, configuration storage missing
    response = client.post("/jobs/from-configs", json=FROM_CONFIGS_BODY)
    assert response.status_code == 503


def test_job_from_configs_requires_job_storage(client, config_storage):
    response = client.post("/jobs/from-configs", json=FROM_CONFIGS_BODY)
    assert response.status_code == 503
    assert response.json() == {"detail": "Storage not available"}
