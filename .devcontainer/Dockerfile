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
    pytest-xdist \
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

# Pre-download hadoop-azure JARs for local ABFSS integration testing.
# Stored at /opt/hadoop-jars (stable image layer); /tmp/hadoop-jars is a
# symlink so conftest.py and `kindling env check --local` find them at the
# expected path without any manual setup.
RUN mkdir -p /opt/hadoop-jars \
    && curl -fsSL -o /opt/hadoop-jars/hadoop-azure-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    && curl -fsSL -o /opt/hadoop-jars/hadoop-azure-datalake-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.4/hadoop-azure-datalake-3.3.4.jar \
    && curl -fsSL -o /opt/hadoop-jars/azure-storage-8.6.6.jar \
        https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    && curl -fsSL -o /opt/hadoop-jars/wildfly-openssl-1.1.3.Final.jar \
        https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar \
    && curl -fsSL -o /opt/hadoop-jars/jetty-util-ajax-9.4.51.v20230217.jar \
        https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.51.v20230217/jetty-util-ajax-9.4.51.v20230217.jar \
    && ln -s /opt/hadoop-jars /tmp/hadoop-jars

# Set up workspace directory for Spark
# Note: vscode user already exists in the base image
RUN mkdir -p /spark-warehouse && chown -R vscode:vscode /spark-warehouse

WORKDIR /workspace
