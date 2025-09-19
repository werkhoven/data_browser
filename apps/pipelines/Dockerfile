# Use Python 3.12 slim image
FROM python:3.12-slim

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install uv package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy workspace files (including data lib)
COPY libs ./libs
COPY apps ./apps
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN cd apps/pipelines && uv sync --no-cache

# Copy source code
COPY apps/pipelines/src/ ./src/

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app/src
ENV UV_SYSTEM_PYTHON=1

# Run the application
CMD ["uv", "run", "python", "-m", "pipelines.main"]
