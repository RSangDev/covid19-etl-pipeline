FROM python:3.11-slim

# Metadata
LABEL maintainer="seu-email@example.com"
LABEL description="COVID-19 ETL Pipeline with PySpark"

# Set working directory
WORKDIR /app

# Install system dependencies (Java for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy requirements first (for Docker cache)
COPY requirements_docker.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements_docker.txt

# Copy application code
COPY . .

# Create necessary directories with permissions
RUN mkdir -p data/raw data/processed data/database logs && \
    chmod -R 777 data logs

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Default command (run pipeline)
CMD ["python", "main.py"]