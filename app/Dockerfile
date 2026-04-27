# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/websolutespa/ws-mark-flow"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DOCLING_ARTIFACTS_PATH=/.cache/docling \
    HF_HOME=/.cache/huggingface \
    TORCH_HOME=/.cache/torch

WORKDIR /app

# 1) System deps — changes very rarely, gets cached
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libmagic1 \
        libgl1 \
        libglib2.0-0 \
        curl

# 2) Python deps — pinned version => cache hit until version bumps.
#    BuildKit cache mount keeps wheels across builds even when the layer is invalidated.
ARG WS_MARK_FLOW_VERSION=0.0.0
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install "ws-mark-flow==${WS_MARK_FLOW_VERSION}"

# 3) Model download — cached as long as the docling/ws-mark-flow versions don't change
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    docling-tools models download -o ${DOCLING_ARTIFACTS_PATH}

# 4) App code — changes most often, kept LAST so steps 1-3 stay cached
COPY . .

EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]