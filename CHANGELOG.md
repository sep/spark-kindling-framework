# Changelog

All notable changes to the Kindling Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Security policy (SECURITY.md) with vulnerability reporting guidelines
- Code of Conduct (CODE_OF_CONDUCT.md) for community engagement
- Comprehensive CHANGELOG for tracking version history

### Changed
- Improved CONTRIBUTING.md with clearer guidelines

### Fixed
- Documentation improvements in setup guide

## [0.1.0] - 2025-10-18

### Added
- Initial public release of Kindling Framework
- Core dependency injection engine with IoC container
- Data entities system for entity registry and storage abstraction
- Data pipes framework for transformation pipeline definition and execution
- Watermarking support for change tracking and incremental processing
- File ingestion capabilities for pattern-based file discovery and loading
- Stage processing for multi-stage pipeline orchestration
- Common transformation utilities
- Comprehensive logging and tracing infrastructure
- Cross-platform support:
  - Azure Synapse Analytics
  - Microsoft Fabric
  - Azure Databricks
  - Local development mode
- Platform abstraction layer for unified API across platforms
- KDA (Kindling Data App) packaging system
- ABFSS (Azure Blob File System) integration
- Delta Lake support for reliable storage
- Configuration management via Dynaconf
- Complete test infrastructure:
  - Unit tests for component-level testing
  - Integration tests for platform service interaction
  - System tests for end-to-end validation
- CI/CD pipeline with GitHub Actions:
  - Automated testing across all test levels
  - Security scanning (safety, bandit)
  - Code coverage reporting
  - Package publishing preparation
- Development tools:
  - Dev container configuration
  - Poetry-based dependency management
  - Build scripts for platform-specific wheels
  - Azure initialization scripts
- Documentation:
  - Architecture overview
  - API reference
  - Platform-specific guides
  - Setup and configuration guides
  - Development workflow documentation
  - Testing strategy documentation
  - Examples and tutorials

### Platform Support
- **Azure Synapse**: Full support with KDA deployment, ABFSS storage, Spark jobs
- **Microsoft Fabric**: Full support with KDA deployment, OneLake storage, Notebooks
- **Databricks**: Full support with KDA deployment, DBFS storage, Clusters
- **Local**: Development mode with local file system

### Dependencies
- Python 3.10+
- Apache Spark 3.4.0+
- Delta Spark 2.4.0+
- Azure SDK packages
- Injector for dependency injection
- Dynaconf for configuration management

## Release Notes

### Version 0.1.0 - Initial Public Release

This is the first public release of the Kindling Framework, a comprehensive solution for building robust data pipelines on Apache Spark. The framework has been battle-tested in production environments and is now ready for broader adoption.

**Key Highlights:**
- Declarative approach to data pipeline definition
- Multi-platform support (Azure Synapse, Fabric, Databricks, Local)
- Strong governance and observability features
- Extensive testing and CI/CD infrastructure
- Production-ready architecture

**Getting Started:**
```bash
pip install kindling
```

See [README.md](README.md) for quickstart guide and [docs/setup_guide.md](docs/setup_guide.md) for detailed setup instructions.

**Known Limitations:**
- Databricks DLT integration is in preview (see packages/kindling_databricks_dlt)
- Some platform-specific features require additional configuration
- Documentation continues to evolve

**Next Steps:**
- Community feedback and contributions
- Additional platform features
- Enhanced monitoring and observability
- Performance optimizations

---

## Release Process

### Version Numbering
We follow [Semantic Versioning](https://semver.org/):
- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible functionality additions
- **PATCH** version for backwards-compatible bug fixes

### Release Types
- **Major releases** (1.0.0, 2.0.0): Breaking changes, significant new features
- **Minor releases** (0.1.0, 0.2.0): New features, non-breaking improvements
- **Patch releases** (0.1.1, 0.1.2): Bug fixes, security updates

### How to Contribute
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to this project.

### Security Updates
Security vulnerabilities are addressed with priority. See [SECURITY.md](SECURITY.md) for our security policy.

---

**Maintained by:** Software Engineering Professionals, Inc.  
**Website:** https://www.sep.com  
**License:** MIT
