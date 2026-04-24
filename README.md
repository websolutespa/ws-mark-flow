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
uvicorn src.app:app --reload --port 8000
```

## API Documentation
- API docs: http://localhost:8000/docs
- Redocly UI: http://localhost:8000/redoc
- OpenAPI spec: http://localhost:8000/openapi.json


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
uv pip install --upgrade -r requirements.txt
```

- build tools

```bash
uv pip install --upgrade setuptools build twine 
```

### 🪛 build

- clean dist and build package

```pwsh
if (Test-Path ./dist) {rm ./dist -r -force}; `
python -m build && twine check dist/*
```

- linux/mac

```bash
[ -d ./dist ] && rm -rf ./dist
python -m build && twine check dist/*
```

### 📦 test / 🧪 debugger

Install the package in editable project location

```pwsh
uv pip install -U -e .
uv pip show ws-mark-flow
```

code quality tools
  
```pwsh
# .\src\robot
uv pip install -U scanreq prospector[with_everything]
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

- [pypi](https://pypi.org/project/ws-mark-flow/)

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
