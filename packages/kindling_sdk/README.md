# kindling-sdk

Design-time SDK for Kindling:

- Platform API client factory for tests/CI
- Platform-specific remote operations for Fabric, Synapse, and Databricks

This package is intentionally separate from runtime bootstrap/runtime execution concerns.

## Install

```bash
pip install spark-kindling-sdk
```

`pip install spark-kindling-cli` installs it too (the CLI depends on it). Pin it to the same
Kindling release as your other Kindling packages (`spark-kindling-sdk==0.14.0`). Without PyPI
access, install the `spark_kindling_sdk-<version>-py3-none-any.whl` attached to the matching
[GitHub Release](https://github.com/sep/spark-kindling-framework/releases).
