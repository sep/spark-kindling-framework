FROM mcr.microsoft.com/devcontainers/python:3.11

# Install system dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    openjdk-21-jdk-headless \
    git \
    curl \
    wget \
    ca-certificates \
    lsb-release \
    gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Azure CLI
RUN curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg \
    && echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update \
    && apt-get -y install --no-install-recommends azure-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment (Java 21 for Spark 3.5)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python packages for development
# PySpark acts as client to connect to the Spark cluster in docker-compose
# Using Spark 3.5.5 for Java 21 compatibility
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
    pyarrow \
    azure-identity \
    azure-storage-blob

# Set up workspace directory for Spark
# Note: vscode user already exists in the base image
RUN mkdir -p /spark-warehouse && chown -R vscode:vscode /spark-warehouse

WORKDIR /workspace