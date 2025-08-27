# Base Airflow image
FROM apache/airflow:2.8.0-python3.10

USER root

# Install system libraries + Java (PySpark driver requires JVM); no Spark here
# Spark is provided by an external spark-master cluster
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    openjdk-17-jre-headless \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
USER airflow
COPY requirements.txt .
# Cache pip wheels between builds
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install -r requirements.txt

# Copy DAGs and ETL code
COPY dags /opt/airflow/dags
COPY etl /opt/airflow/etl

# Update PYTHONPATH for etl.* imports
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/etl"
