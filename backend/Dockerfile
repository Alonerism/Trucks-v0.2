# Backend Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl g++ cmake ninja-build pkg-config && \
    rm -rf /var/lib/apt/lists/*

# Help native builds on ARM (PyVRP etc.)
ENV CXXFLAGS="-Wno-error" \
    CMAKE_BUILD_PARALLEL_LEVEL=4

# Copy project files (ensure package is present before install)
COPY pyproject.toml poetry.lock README.md /app/
COPY backend /app/backend

# Install dependencies (including dev for pytest)
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --with dev

# Ports and env
ENV HOST=0.0.0.0 \
    PORT=8001 \
    ROUTING_PROVIDER=straight \
    STRICT_DEV_INVARIANTS=0

EXPOSE 8001

# Start server using python -m to avoid missing entrypoint issues
CMD ["python", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8001"]
