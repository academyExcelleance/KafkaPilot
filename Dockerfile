# Use Python 3.12 slim as base image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_CACHE_DIR=/tmp/uv-cache

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

  
# Create app directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .


# Create non-root user for security
# RUN groupadd -r mcpuser && useradd -r -g mcpuser mcpuser
# RUN chown -R mcpuser:mcpuser /app
# RUN chown -R mcpuser:mcpuser /tmp/uv-cache
# RUN chmod -R 777 /home


# Switch to non-root user
#USER mcpuser

# Expose port (adjust as needed for your MCP server)
EXPOSE 8000

# Run the MCP server
CMD ["uv", "run", "mcp", "run", "main.py"]