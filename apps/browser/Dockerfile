FROM python:3.12

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
RUN cd apps/browser && uv sync --no-cache

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONPATH=/app/src
ENV BROWSER_HOST=${BROWSER_HOST:-0.0.0.0}
ENV BROWSER_PORT=${BROWSER_PORT:-8001}
ENV PIPELINES_URL=${PIPELINES_URL:-http://pipelines-api:8000}

# Expose port
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8001/ || exit 1

# Run the application
CMD ["uv", "run", "python", "-m", "browser.main"]
