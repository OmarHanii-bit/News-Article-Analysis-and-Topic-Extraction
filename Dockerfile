# ============================================================================
# Multi-Stage Dockerfile for News Article Big Data Pipeline
# Integrates Python 3.11 + OpenJDK 17 (for PySpark) + Streamlit Web Dashboard
# ============================================================================
FROM python:3.11-slim

# Prevent Python from writing .pyc files & enable unbuffered standard out
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install OpenJDK 17 (Required for PySpark Catalyst and MLlib JVM components)
# along with curl, procps, and build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME environment variable for PySpark
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set application working directory inside container
WORKDIR /app

# Copy dependency definition
COPY requirements.txt .

# Install dependencies into container
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Pre-download NLTK VADER lexicon for distributed sentiment analysis
RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"

# Copy entire project directory into container
COPY . .

# Expose ports:
# 8501: Streamlit Executive Dashboard
# 8888: Jupyter Notebook
EXPOSE 8501 8888

# Health check to verify Streamlit accessibility
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Default command: Launch the interactive Streamlit Executive Dashboard
CMD ["streamlit", "run", "src/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
