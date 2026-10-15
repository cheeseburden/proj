FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and data assets
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY data/sample/ /app/data/sample/
COPY data/models/ /app/data/models/

# Ensure runtime directories exist
RUN mkdir -p /app/data/raw /app/data/models /app/data/hdfs_buffer

# Default exposed port for FastAPI
EXPOSE 8000

# Default command runs the API server
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
