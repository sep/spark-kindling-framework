# Contributing to Kindling
We welcome contributions from the community!

## Bug Reports and Fixes
If you encounter a bug:

1. **Check existing issues** - Search the Issues tab to see if it's already reported
2. **Open a new issue** - Provide detailed reproduction steps, expected behavior, and actual behavior
3. **Submit a Pull Request** - If you can fix it yourself, we encourage contributions

## Development Guidelines
### Code Standards
- Follow PEP 8 style guidelines for Python code
- Include docstrings for all public functions and classes
- Add type hints where appropriate
- Write clear, descriptive variable and function names

### Testing
- All new features must include unit tests using pytest
- Bug fixes should include a test that reproduces the bug
- Maintain or improve current test coverage
- Run the full test suite before submitting a PR

### Spark Compatibility
- Ensure compatibility with Spark 3.x (specify minimum version in PR)
- Test with both local Spark, Azure Synapse, Azure Fabric, and Databricks environments where applicable and possible
- Document any Spark-specific configuration requirements

## Submitting Pull Requests
1. **Fork the repository** and create a feature branch
2. **Write tests** for your changes
3. **Update documentation** if you're adding or changing functionality
4. **Run tests** to ensure everything passes
5. **Submit PR** with a clear description of the changes and why they're needed
6. **Respond to feedback** from code reviewers

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

## Questions or Suggestions?
- Open an issue for feature requests or questions
- Start a discussion for architectural changes
- Contact the maintainers directly for security concerns

## License
By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition
All contributors will be acknowledged in the project documentation. Thank you for helping make this framework better for everyone!
