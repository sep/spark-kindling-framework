# Contributing to Kindling Framework

Thank you for your interest in contributing to the Kindling Framework! We welcome contributions from the community and are grateful for any help you can provide, whether it's fixing bugs, adding features, improving documentation, or reporting issues.

## Getting Started

Before you begin:
- Read our [Code of Conduct](CODE_OF_CONDUCT.md)
- Check out the [README](README.md) for project overview
- Review the [documentation](docs/) to understand the architecture

## How to Contribute

There are many ways to contribute:
- Report bugs and request features
- Fix bugs and implement features
- Improve documentation
- Review pull requests
- Help other users with questions

## Bug Reports and Feature Requests

If you encounter a bug or have a feature idea:

1. **Search existing issues** - Check if someone has already reported it
2. **Create a detailed issue** - Use our issue templates when available
3. **Provide context** - Include:
   - Clear description of the problem or feature
   - Steps to reproduce (for bugs)
   - Expected vs. actual behavior (for bugs)
   - Your environment (Python version, Spark version, platform)
   - Relevant code snippets or error messages
4. **Be responsive** - Answer questions from maintainers to help resolve the issue

## Development Setup

1. **Fork and clone** the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/spark-kindling-framework.git
   cd spark-kindling-framework
   ```

2. **Set up development environment**:
   ```bash
   # Install Poetry (if not already installed)
   curl -sSL https://install.python-poetry.org | python3 -
   
   # Install dependencies
   poetry install
   
   # Activate virtual environment
   poetry shell
   ```

3. **Configure environment** (for integration tests):
   ```bash
   cp .env.template .env
   # Edit .env with your Azure credentials (if testing Azure features)
   ```

4. **Verify setup**:
   ```bash
   # Run unit tests
   pytest tests/unit/
   ```

## Development Guidelines

### Code Standards
- Follow **PEP 8** style guidelines for Python code
- Use **Black** for code formatting (configured in pyproject.toml)
- Include comprehensive **docstrings** for all public functions and classes
- Add **type hints** where appropriate for better code clarity
- Write clear, descriptive variable and function names
- Keep functions focused and modular

### Code Quality Tools
We use several tools to maintain code quality:

```bash
# Format code with Black
black packages/kindling tests/

# Run linting
pylint packages/kindling

# Type checking (optional but recommended)
mypy packages/kindling

# Run all checks before committing
black packages/ tests/ && pylint packages/kindling && pytest tests/unit/
```

### Testing Requirements
- All new features **must** include unit tests using pytest
- Bug fixes **should** include a test that reproduces the bug
- Maintain or improve current test coverage (aim for >80%)
- Run the full test suite before submitting a PR
- Integration tests should be marked appropriately:
  ```python
  @pytest.mark.integration
  def test_azure_storage_integration():
      # Test code
  ```

### Test Structure
```
tests/
├── unit/           # Fast, isolated tests with mocking
├── integration/    # Tests with real external services
└── system/         # End-to-end workflow tests
```

### Spark Compatibility
- Ensure compatibility with **Spark 3.4+** (minimum supported version)
- Test with multiple environments when possible:
  - Local Spark (required)
  - Azure Synapse (for platform-specific features)
  - Microsoft Fabric (for platform-specific features)
  - Azure Databricks (for platform-specific features)
- Document any Spark-specific configuration requirements
- Consider performance implications for large datasets

## Submitting Pull Requests

### Before You Start
- Create an issue first to discuss major changes
- Ensure your changes align with the project's goals
- Check that nobody else is working on the same thing

### Pull Request Process

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-number-description
   ```

2. **Make your changes**:
   - Write clean, well-documented code
   - Follow the coding standards outlined above
   - Keep commits atomic and well-described

3. **Write tests**:
   - Add unit tests for all new functionality
   - Update existing tests if behavior changes
   - Ensure all tests pass locally

4. **Update documentation**:
   - Update README.md if adding user-facing features
   - Add docstrings to new functions/classes
   - Update relevant documentation in `docs/`
   - Add entry to CHANGELOG.md under [Unreleased]

5. **Run quality checks**:
   ```bash
   # Format and lint
   black packages/ tests/
   pylint packages/kindling
   
   # Run tests
   pytest tests/unit/ --cov=kindling
   
   # (Optional) Run integration tests if applicable
   pytest tests/integration/ -m integration
   ```

6. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Brief description of change
   
   More detailed explanation if needed.
   
   Fixes #issue-number"
   ```

7. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Open a Pull Request**:
   - Use a clear, descriptive title
   - Reference related issues (e.g., "Fixes #123")
   - Describe what changes you made and why
   - Explain any design decisions
   - Note any breaking changes
   - Add screenshots for UI changes (if applicable)

9. **Respond to feedback**:
   - Be open to suggestions and constructive criticism
   - Make requested changes promptly
   - Ask questions if anything is unclear
   - Keep the discussion respectful and professional

## Code Review Process
All contributions undergo formal code review before merging:

- At least one maintainer must approve the PR
- Code must meet quality and testing standards
- Documentation must be updated as needed
- Breaking changes require additional discussion

## Dependency Management
When adding or updating dependencies:
- Document why the dependency is needed
- Verify license compatibility (MIT, Apache 2.0, BSD preferred)
- Update requirements.txt or setup.py appropriately
- Note any version constraints

## Documentation
- Update README.md if adding features
- Include inline code comments for complex logic
- Provide usage examples for new functionality
- Update CHANGELOG.md with your changes

## Community

### Getting Help
- **Questions**: Open an issue with the "question" label
- **Discussions**: Use GitHub Discussions for general topics
- **Bugs**: Report via GitHub Issues
- **Security**: Email security@sep.com (see [SECURITY.md](SECURITY.md))

### Contribution Ideas

Looking for ways to contribute? Here are some ideas:
- Fix issues labeled "good first issue" or "help wanted"
- Improve test coverage
- Enhance documentation with examples
- Add support for additional platforms
- Optimize performance
- Report bugs you encounter

## License
By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition
All contributors will be acknowledged in the project documentation. Thank you for helping make this framework better for everyone!
