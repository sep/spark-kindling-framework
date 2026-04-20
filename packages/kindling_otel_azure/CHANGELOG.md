# Changelog

All notable changes to the kindling-otel-azure extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-04-20

### Changed
- **BREAKING**: Collapsed three per-platform wheels (`kindling_otel_azure_fabric`, `_synapse`, `_databricks`) into a single `kindling-otel-azure` wheel. Pre-v0.4 wheels differed only in pin profile, not code — Fabric pinned the older OpenTelemetry line (1.6.x / 1.20.x) for compatibility with Fabric's azure-core 1.30.x runtime, Synapse/Databricks used the modern line (1.8.x / 1.21.x).
- Ships one universal pin profile going forward — the Fabric-compatible line (`azure-monitor-opentelemetry>=1.6.0,<1.7.0`, `opentelemetry-api/sdk ~1.20.0`). This line is API-compatible with every symbol the extension uses and it's the only line that works on Fabric. Synapse and Databricks consumers don't lose any functionality.

### Rationale for not using extras

Initially attempted to express per-platform pin divergence via PEP 508 markers on optional extras. The extension install path in `packages/kindling/bootstrap.py` (for consumers listing `kindling-otel-azure` under `kindling.extensions` in settings) runs `pip install <local_wheel>` without requesting extras, so extra-gated deps would silently skip and `import kindling_otel_azure` would fail at runtime on every platform. Universal hard pins avoid this foot-gun.

### Removed
- `scripts/generate_extension_config.py` and `scripts/build_extensions.py` — single-wheel build runs through `scripts/build.py` alongside the other design-time wheels.

### Migration Guide

**Before (pre-v0.4):**
```yaml
kindling:
  extensions:
    - kindling-otel-azure-fabric>=0.3.2   # or -synapse / -databricks
```

**After (v0.4.0):**
```yaml
kindling:
  extensions:
    - kindling-otel-azure>=0.4.0
```

The Python import path (`from kindling_otel_azure import ...`) is unchanged.

## [0.3.0-alpha.1] - 2026-01-27

### Changed
- **BREAKING**: Updated `azure-monitor-opentelemetry` dependency from `1.2.0` to `^1.8.0` for better cross-platform compatibility
- Changed dependency pinning from exact versions to semantic versioning ranges:
  - `azure-monitor-opentelemetry`: `1.2.0` → `^1.8.0` (allows 1.8.x, 1.9.x, etc.)
  - `opentelemetry-api`: `1.21.0` → `^1.21.0` (allows 1.21.x, 1.22.x, etc.)
  - `opentelemetry-sdk`: `1.21.0` → `^1.21.0` (allows 1.21.x, 1.22.x, etc.)

### Motivation
- Fixes dependency conflicts in Databricks environments
- Leverages 12 months of bug fixes and improvements (1.2.0 → 1.8.4)
- Provides better flexibility for pip dependency resolution across platforms
- No breaking API changes in azure-monitor-opentelemetry between 1.2.0 and 1.8.4

### Migration Guide
No code changes required. The extension API remains unchanged. Simply update your `settings.yaml`:

```yaml
kindling:
  extensions:
    - kindling-otel-azure>=0.3.0-alpha.1  # Updated version
```

### Testing
- ✅ Builds successfully on Python 3.10+
- ⏳ Requires validation on Fabric, Synapse, and Databricks platforms

## [0.2.0] - 2026-01-25

### Added
- Initial stable release
- Azure Monitor logger provider implementation
- Azure Monitor trace provider implementation
- Automatic dependency injection integration
- Configuration via `settings.yaml` or `BOOTSTRAP_CONFIG`

### Features
- Sends logs to Azure Monitor/Application Insights
- Distributed tracing with automatic trace context propagation
- Seamless integration with Kindling framework's DI container
- Works across Fabric, Synapse, and Databricks platforms
