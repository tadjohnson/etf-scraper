# Use Python 3.11 slim image with AMD64 support (for fly.io)
FROM --platform=linux/amd64 python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gcc \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY api_server.py .
COPY scraper.py .
COPY scraper_requests.py .
COPY dividend_history_manager.py .
COPY frontend.html .

# Create output directory
RUN mkdir -p /app/output

# Expose port for the API server
EXPOSE 5000

# Run the API server
CMD ["python", "api_server.py"]