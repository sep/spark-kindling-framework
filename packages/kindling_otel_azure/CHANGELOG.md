# Changelog

All notable changes to the kindling-otel-azure extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
