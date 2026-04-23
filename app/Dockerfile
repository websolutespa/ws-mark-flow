# Build/test the minimal app that consumes the published PyPI package
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps needed by some python wheels (docling/torch/pymupdf/paramiko)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libmagic1 \
        libgl1 \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install the package from PyPI 
RUN pip install ws-mark-flow==0.0.2

# Copy the minimal app (main.py and .env)
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
