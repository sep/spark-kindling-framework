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

# Install GitHub CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update \
    && apt-get install -y gh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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
    isort \
    flake8 \
    pre-commit \
    pylint \
    mypy \
    ipython \
    jupyter \
    pandas \
    pyarrow \
    # Core framework dependencies
    injector \
    dynaconf \
    pyyaml \
    blinker \
    # Azure dependencies for deployment and testing
    azure-identity \
    azure-core \
    azure-storage-blob \
    azure-storage-file-datalake \
    azure-synapse-artifacts==0.17.0 \
    # Databricks SDK for testing
    databricks-sdk \
    # Build tools
    poetry \
    poethepoet

# Set up workspace directory for Spark
# Note: vscode user already exists in the base image
RUN mkdir -p /spark-warehouse && chown -R vscode:vscode /spark-warehouse

WORKDIR /workspace
