# Ws-Mark-Flow AI Converter

Convert files from various sources (SharePoint, S3, Azure Blob, etc.) to Markdown and upload to destinations (Google Drive, SharePoint, etc.).

## Features

- **Multi-source support**: SharePoint, S3, Azure Blob Storage (extensible)
- **Multi-destination support**: Google Drive, SharePoint, S3 (extensible)
- **File conversion**: PDF, DOCX, PPTX, XLSX, CSV, images, and more → Markdown
- **Incremental conversion**: Only converts files not already in destination
- **Job persistence**: MongoDB-backed job storage for resumable pipelines
- **REST API**: FastAPI-based API for job management
- **Progress tracking**: Real-time conversion progress and statistics

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│   Source    │────▶│  Converter   │────▶│  Destination  │
│ (SharePoint)│     │ (MarkItDown) │     │(Google Drive) │
└─────────────┘     └──────────────┘     └───────────────┘
                           │
                    ┌──────▼──────┐
                    │   MongoDB   │
                    │ (Job Store) │
                    └─────────────┘
```

## Installation

```bash
# Install dependencies
uv pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your MongoDB URI

# Run with auto-reload
uvicorn src.main:app --reload --port 8000
```

## API Documentation
- API docs: http://localhost:8000/docs
- Redocly UI: http://localhost:8000/redoc
- OpenAPI spec: http://localhost:8000/openapi.json

### Connection test downloads

For saved source and destination configurations, the **Test** results modal offers:

- **Download file info (CSV)**: a UTF-8, comma-separated CSV with a header and one row
  per `FileInfo` returned by `list_files()`. Columns include `name`, `path`,
  `modified_at`, `size`, `content_type`, `stem`, `suffix`, and `parent_path`.
  Missing optional values are empty cells; empty listings still include the header.
  The filename is `<connection-name>_fileinfo.csv`, with whitespace removed from
  the connection name (for example, `dhl-fs-CSTEAMS_fileinfo.csv`).
  Characters unsafe in filenames are replaced with underscores.
- **Download all files (ZIP)**: downloads each listed file using the connection's
  download method and packages them into a ZIP, preserving relative folders.
  The filename is `<connection-name>_files.zip` (for example,
  `dhl-fs-CSTEAMS_files.zip`), using the same name cleanup as the CSV export.
  The browser saves the archive to its configured download location (usually
  Downloads); extract it locally to get the files in a folder.

The test preview and both exports filter source files by `CONVERTIBLE_EXTENSIONS`
and destination files by `MARKDOWN_EXTENSIONS`. Both exports re-list the connection
and include all matching results, not just the modal's first ten preview files.
Connector-specific root folders, prefixes, and other listing rules still apply. ZIP preparation happens
on the server using temporary disk storage, which is cleaned up after delivery
or failure. Failed file downloads fail the export rather than returning an
incomplete ZIP.

API: `GET /configurations/{id}/test/download?format=csv` or `format=zip`,
using the same authentication as the other API endpoints.


## Supported Integrations

### Sources
- **SharePoint** (`sharepoint`): Microsoft Graph API
- More coming: S3, Azure Blob, Local filesystem

### Destinations  
- **Google Drive** (`google_drive`): Google Drive API v3
- More coming: SharePoint, S3, Azure Blob

## Supported File Types

Converted using [Microsoft MarkItDown](https://github.com/microsoft/markitdown), [Docling](https://github.com/docling-project/docling) or LLM-based analysis for complex PDFs & images.

- Documents: PDF, DOCX, DOC, RTF, TXT
- Presentations: PPTX, PPT
- Spreadsheets: XLSX, XLS, CSV
- Web: HTML, XML, JSON, YAML
- Images: PNG, JPG, GIF, BMP, TIFF (OCR)

## Configuration

### Main Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_USERNAME` | `admin` | Basic auth username |
| `AUTH_PASSWORD` | `yourpassword` | Basic auth password |
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `converter` | Database name |
| `TEMP_DIR` | `./.data/converter` | Temporary file storage |

## Development

### 🔖 requirements

- install uv venv package management

```bash
py -m pip install --upgrade uv
# create venv
uv venv
# activate venv
#win: .venv/Scripts/activate
#linux: source .venv/bin/activate
```

- project requirements update

```bash
python -m requirements_update
python -m requirements_update requirements-dev.txt
uv pip install --upgrade -r requirements.txt -r requirements-dev.txt
# always re-run the test suite after an update (see "unit tests" below)
pytest --cov
```

- dev tools (tests, build/publish, code quality)

```bash
uv pip install --upgrade -r requirements-dev.txt
```

### ▶️ VS Code launch configurations

`.vscode/launch.json` groups the common commands in the Run and Debug dropdown.
Python entries use the interpreter selected in VS Code and need the Python Debugger
extension (`ms-python.debugpy`), recommended in `.vscode/extensions.json`.

| Group | Name | Does |
|-------|------|------|
| app | `app:run` | uvicorn with `--reload` on port 8000 |
| app | `app:debug` | uvicorn without reload, `justMyCode: false` |
| test | `test:run` / `test:cov` | pytest / pytest with coverage gate + `htmlcov/` |
| test | `test:file` | debug the test file open in the editor |
| test | `test:live` | live tests against the MongoDB in `.env` (see below) |
| quality | `quality:lint` / `quality:complexity` / `quality:security` / `quality:reqs` | prospector profiles and scanreq (see below) |
| docker | `docker:build` / `docker:run` | build `./app` image, run it on port 8080 with the root `.env` |
| dist | `dist:check` | `python release.py`: bump the version if already on PyPI, clean, build, `twine check --strict` |
| dist | `dist:publish` | `twine upload --verbose dist/*` (kept separate on purpose) |

### 🪛 build

- bump the patch version if the current one is already on PyPI, clean, build and check (`--dry-run` only reports)

```bash
python release.py
```

- or manually: clean dist and build package

```pwsh
if (Test-Path ./dist) {rm ./dist -r -force}; `
python -m build && twine check dist/*
```

- linux/mac

```bash
[ -d ./dist ] && rm -rf ./dist
python -m build && twine check dist/*
```

### ✅ unit tests

The suite under `tests/` is hermetic: no network (non-loopback sockets are
blocked), no MongoDB, no LLM calls, no Docling model loading and the local
`.env` is never read. External SDKs are faked at the client or HTTP transport
level, so an incompatible dependency upgrade fails a test instead of production.

```bash
uv pip install -U -r requirements-dev.txt
# fast run
pytest
# with coverage: fails below the threshold set in pyproject.toml ([tool.coverage.report].fail_under)
pytest --cov
# html report in ./htmlcov
pytest --cov --cov-report=html
```

Known bugs are pinned with `@pytest.mark.xfail(strict=True, reason="BUG: ...")`:
once a bug is fixed the test starts passing and must be un-marked.

#### live tests (MongoDB)

`tests/live/` runs the storages and the app (real lifespan, APScheduler MongoDB
data store, local-folder source/destination) against a real MongoDB. They are
skipped unless `--live` is passed, and are not part of the coverage gate.

```bash
pytest tests/live --live -rs
```

- connection: `MONGODB_URI` / `MONGODB_DATABASE` from the shell, else only those
  two keys from `.env`
- database: `{MONGODB_DATABASE}-tests-live`, dropped before every test and at
  the end of the run; the configured database is never touched
- the end-to-end conversion test is skipped on Windows until the pinned
  `run_conversion` path bug is fixed

### 📦 test / 🧪 debugger

Install the package in editable project location

```pwsh
uv pip install -U -e .
uv pip show ws-mark-flow
```

code quality tools
  
```pwsh
# .\src\robot
## unused requirements
scanreq -r requirements.txt -p ./src
## style/linting
prospector ./src -t pylint -t pydocstyle
## code quality/complexity
prospector ./src -t vulture -t mccabe -t mypy 
## security
prospector ./src -t dodgy -t bandit
## package
prospector ./src -t pyroma
```

### ✈️ publish

- [pypi](https://pypi.org/project/ws-mark-flow/), after `python release.py`

  ```bash
  twine upload --verbose dist/* 

  ```

### Docker
- Build the Docker image (override version at build time if needed)

```bash
docker build -t ws-mark-flow ./app

# Copy environment file
cp .env.example ./app/.env
# Edit .env 

docker run -p 80:80 --env-file ./app/.env ws-mark-flow
# use host.docker.internal for MongoDB connection from container to host
docker run --add-host=host.docker.internal:host-gateway -p 80:80 --env-file ./app/.env ws-mark-flow
```

## License

MIT
