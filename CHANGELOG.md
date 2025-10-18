# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial public release preparation
- Security policy and vulnerability reporting process
- Code of conduct for community guidelines
- Comprehensive CI/CD pipeline with GitHub Actions

## [0.1.0] - Initial Release

### Added

#### Core Framework
- **Dependency Injection Engine**: IoC container for loose coupling and testability
- **Data Entities**: Entity registry and storage abstraction system
- **Data Pipes**: Declarative transformation pipeline definition and execution
- **Entity Providers**: Pluggable storage abstraction for Delta Lake tables
- **Watermarking**: Change tracking and incremental processing support
- **File Ingestion**: Pattern-based file discovery and loading
- **Stage Processing**: Multi-stage pipeline orchestration
- **Common Transforms**: Reusable data transformation utilities
- **Logging & Tracing**: Comprehensive observability features

#### Platform Support
- **Azure Synapse**: Full support with ABFSS storage integration
- **Microsoft Fabric**: OneLake integration and Fabric-specific features
- **Azure Databricks**: DBFS and Delta Lake support
- **Local Development**: File system-based development and testing

#### KDA Packaging System
- Platform-specific wheel building
- Dependency bundling and isolation
- Automated deployment workflows
- Configuration management across environments

#### Testing Infrastructure
- Unit tests for core components
- Integration tests for platform services
- System tests for end-to-end workflows
- KDA packaging and deployment tests

#### Documentation
- Comprehensive README with quickstart
- Detailed architecture and design documentation
- Setup and configuration guides
- API reference documentation
- Platform-specific usage guides
- Example notebooks and applications

#### Development Tools
- Poetry-based dependency management
- Black code formatting
- Pylint code quality checks
- Mypy static type checking
- Pre-configured development container

#### CI/CD Pipeline
- Automated unit testing
- Integration testing with real Azure storage
- Security scanning (Safety, Bandit)
- Code quality checks
- Multi-platform system tests
- Automated wheel building and publishing

### Dependencies
- Python 3.10+
- Apache Spark 3.4+
- Delta Lake 2.4+
- Azure Storage SDK
- Azure Identity SDK
- Databricks SDK
- injector (dependency injection)
- dynaconf (configuration management)

### License
- MIT License

---

## Release Notes Guidelines

### Version Numbers
- **Major (X.0.0)**: Breaking changes, major new features
- **Minor (0.X.0)**: New features, backward compatible
- **Patch (0.0.X)**: Bug fixes, minor improvements

### Categories
- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Features that will be removed in future versions
- **Removed**: Features removed in this version
- **Fixed**: Bug fixes
- **Security**: Security vulnerability fixes

[Unreleased]: https://github.com/sep/spark-kindling-framework/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sep/spark-kindling-framework/releases/tag/v0.1.0
