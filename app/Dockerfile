# syntax=docker/dockerfile:1.7
FROM --platform=$TARGETPLATFORM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/websolutespa/ws-mark-flow"

ARG DOCLING_PRELOAD_MODELS=1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    DOCLING_ARTIFACTS_PATH=/opt/docling/artifacts \
    HF_HOME=/tmp/huggingface \
    TORCH_HOME=/tmp/torch

WORKDIR /app

# 1) System deps — changes very rarely, gets cached.
#    `upgrade` pulls Debian security fixes not yet in the base image (e.g. zlib1g).
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
        libmagic1 \
        libgl1 \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# 2) Python deps. Install CPU-only torch first so the ws-mark-flow dependency
#    resolver cannot pull the much larger default CUDA wheels from PyPI.
#    pip is the installer itself, so it cannot be pinned by the package: upgrade it here.
#    setuptools (old on the PyTorch index) is pinned by ws-mark-flow's requirements.
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --upgrade pip \
    && pip install \
        --index-url https://download.pytorch.org/whl/cpu \
        --upgrade \
        torch \
        torchvision \
    && pip freeze | grep -E '^(torch|torchvision)==' > /tmp/torch-cpu-constraints.txt \
    && pip install --upgrade -c /tmp/torch-cpu-constraints.txt ws-mark-flow docling \
    && rm -f /tmp/torch-cpu-constraints.txt

# 3) Model download
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    mkdir -p "${DOCLING_ARTIFACTS_PATH}" \
    && if [ "${DOCLING_PRELOAD_MODELS}" = "1" ]; then \
        docling-tools models download -o "${DOCLING_ARTIFACTS_PATH}"; \
      fi \
    && rm -rf "${HF_HOME}" "${TORCH_HOME}" /root/.cache/huggingface /root/.cache/torch

# 4) App code — changes most often, kept LAST so steps 1-3 stay cached
COPY . .

EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]