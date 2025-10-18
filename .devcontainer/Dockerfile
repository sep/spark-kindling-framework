# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/devcontainers/python:3.11

# Install system dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        openjdk-21-jdk-headless \
        git \
        curl \
        wget \
        ca-certificates \
        gnupg \
        lsb-release \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Azure CLI using download-and-execute method (works on all Debian versions)
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Set Java environment (Java 21 for Spark 3.5)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python packages for development
# PySpark acts as client to connect to the Spark cluster in docker-compose
RUN pip install --no-cache-dir \
    pyspark==3.5.5 \
    delta-spark==3.3.2 \
    pytest \
    pytest-cov \
    black \
    pylint \
    mypy \
    ipython \
    jupyter \
    pandas \
    pyarrow

# Set up workspace directory for Spark
# Note: vscode user already exists in the base image
RUN mkdir -p /spark-warehouse && chown -R vscode:vscode /spark-warehouse

WORKDIR /workspace