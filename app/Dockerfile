FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DOCLING_ARTIFACTS_PATH=/.cache/docling \
    HF_HOME=/.cache/huggingface \
    TORCH_HOME=/.cache/torch 

WORKDIR /app

# system deps 
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libmagic1 \
        libgl1 \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# install main package
RUN pip install ws-mark-flow==0.0.3

# pre-download docling models
RUN docling-tools models download -o ${DOCLING_ARTIFACTS_PATH}

# minimal app (main.py and .env)
COPY . .

EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
