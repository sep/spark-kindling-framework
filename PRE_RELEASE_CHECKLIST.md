# Pre-Release Checklist

Use this checklist before making the repository public and releasing v0.1.0.

## Repository Configuration

### GitHub Settings
- [ ] Repository visibility set to Public
- [ ] Repository description updated
- [ ] Topics/tags added (spark, data-engineering, azure, databricks, fabric, delta-lake, etl)
- [ ] Website URL added (if applicable)
- [ ] License displayed correctly (MIT)
- [ ] Social preview image created and uploaded (optional)

### Branch Protection
- [ ] Branch protection enabled on `main` branch
- [ ] Require pull request reviews before merging
- [ ] Require status checks to pass before merging
- [ ] Require branches to be up to date before merging
- [ ] Include administrators in restrictions

### GitHub Features
- [ ] Issues enabled
- [ ] Discussions enabled (recommended)
- [ ] Wiki enabled or disabled as appropriate
- [ ] Projects enabled (optional)
- [ ] Security tab reviewed
- [ ] Insights available

## CI/CD Setup

### GitHub Actions
- [ ] Workflows run successfully on test PR
- [ ] All required secrets configured:
  - [ ] `AZURE_STORAGE_KEY_TEST` (if running integration tests)
  - [ ] `AZURE_STORAGE_KEY_STAGING` (if running system tests)
  - [ ] `CODECOV_TOKEN` (for coverage reporting)
  - [ ] `PYPI_API_TOKEN` (for future PyPI publishing)
- [ ] All required variables configured:
  - [ ] `TEST_STORAGE_ACCOUNT`
  - [ ] `TEST_CONTAINER`
  - [ ] `STAGING_STORAGE_ACCOUNT`
  - [ ] `STAGING_CONTAINER`
- [ ] Codecov integration working
- [ ] Security scanning (Bandit, Safety) running

### Workflow Testing
- [ ] Unit tests workflow passes
- [ ] Linting and formatting workflow passes
- [ ] Integration tests workflow passes (or skips gracefully)
- [ ] Build workflow produces artifacts
- [ ] Test summary workflow generates report

## Code Quality

### Testing
- [ ] All unit tests pass locally
- [ ] All unit tests pass in CI
- [ ] Test coverage meets minimum threshold (>30%)
- [ ] No broken tests disabled/skipped inappropriately

### Code Standards
- [ ] Black formatter check passes
- [ ] Pylint runs without critical errors
- [ ] Mypy type checking passes (or errors documented)
- [ ] No obvious code smells or technical debt

### Security
- [ ] No credentials in code
- [ ] No .env files committed
- [ ] .gitignore properly configured
- [ ] Security scanning passes
- [ ] Dependencies have no critical vulnerabilities

## Documentation

### Core Documentation
- [ ] README.md is clear and welcoming
- [ ] CONTRIBUTING.md has setup instructions
- [ ] SECURITY.md has vulnerability reporting process
- [ ] CODE_OF_CONDUCT.md in place
- [ ] CHANGELOG.md initialized with v0.1.0
- [ ] LICENSE file correct (MIT)

### Technical Documentation
- [ ] All docs/ files reviewed for accuracy
- [ ] No placeholder text remains
- [ ] Links are working
- [ ] Code examples are correct
- [ ] Installation instructions tested

### Examples
- [ ] All example files are valid Python
- [ ] Examples compile/run without errors
- [ ] Examples demonstrate key features
- [ ] Examples include comments and explanations

## Package Metadata

### pyproject.toml
- [ ] Version number correct (0.1.0)
- [ ] Description accurate
- [ ] Authors/maintainers listed
- [ ] License specified (MIT)
- [ ] Homepage URL correct
- [ ] Repository URL correct
- [ ] Documentation URL correct
- [ ] Keywords added for discoverability
- [ ] Classifiers appropriate
- [ ] Dependencies complete and versions specified

### Package Structure
- [ ] All required files included in package
- [ ] No unnecessary files in package
- [ ] Package builds successfully
- [ ] Package can be installed locally

## Legal & Compliance

### Licensing
- [ ] LICENSE file present (MIT)
- [ ] Copyright year current (2025)
- [ ] All source files have appropriate headers (if applicable)
- [ ] Third-party code properly attributed
- [ ] Dependency licenses reviewed and compatible

### Disclaimers
- [ ] DISCLAIMER.md reviewed
- [ ] No warranty statements clear
- [ ] Liability limitations stated
- [ ] Third-party dependencies listed

## Communication

### Release Preparation
- [ ] Release notes drafted
- [ ] v0.1.0 tag ready to create
- [ ] GitHub Release drafted
- [ ] Announcement text prepared
- [ ] Screenshots/demos ready (if applicable)

### Contact Information
- [ ] support@sep.com verified working
- [ ] security@sep.com verified working
- [ ] conduct@sep.com verified working
- [ ] SEP website link correct (www.sep.com)

### Community
- [ ] Issue templates functional
- [ ] PR template functional
- [ ] Response plan for initial issues
- [ ] Support availability planned

## Post-Release Planning

### Monitoring
- [ ] GitHub notifications configured
- [ ] Team members have appropriate access
- [ ] Issue triage process defined
- [ ] PR review process defined

### Next Steps Planned
- [ ] v0.2.0 features identified
- [ ] Roadmap communicated (in discussions or docs)
- [ ] PyPI publishing planned
- [ ] Documentation site planned (optional)

## Final Verification

### Smoke Tests
- [ ] Clone fresh repository
- [ ] Install dependencies with poetry
- [ ] Run unit tests
- [ ] Build package
- [ ] Install built package
- [ ] Run simple example

### Review
- [ ] At least one team member has reviewed all changes
- [ ] Security team has approved (if required)
- [ ] Legal has approved (if required)
- [ ] Management has approved public release

## Pre-Release Commands

Run these commands before tagging the release:

```bash
# Ensure you're on main branch with latest code
git checkout main
git pull origin main

# Verify clean state
git status

# Run full test suite
poetry run pytest tests/unit/ -v

# Run linters
poetry run black --check packages/ tests/
poetry run pylint packages/kindling/

# Build package
poetry build

# Verify package contents
tar tzf dist/kindling-0.1.0.tar.gz | head -20
```

## Release Commands

Once all checks pass:

```bash
# Create annotated tag
git tag -a v0.1.0 -m "Release version 0.1.0 - Initial public release"

# Push tag
git push origin v0.1.0

# Create GitHub Release via UI with:
# - Release notes from CHANGELOG.md
# - Attach built wheels if available
# - Mark as "Latest release"
```

## Post-Release Verification

Within 24 hours of release:

- [ ] GitHub Release is published and visible
- [ ] Tag is visible in repository
- [ ] CI/CD ran successfully on tag
- [ ] Wheels attached to release (if applicable)
- [ ] Repository is public and accessible
- [ ] Documentation is accessible
- [ ] No critical issues reported
- [ ] First announcement posted (if planned)

## Emergency Rollback

If critical issues found immediately after release:

1. Mark release as pre-release in GitHub
2. Add warning to README
3. Create hotfix branch
4. Fix critical issues
5. Test thoroughly
6. Release v0.1.1 with fixes

---

**Last Updated**: October 18, 2025  
**Next Review**: Before v0.2.0 release

## Notes

Add any environment-specific notes or reminders here:

- [ ] Additional item 1
- [ ] Additional item 2
- [ ] Additional item 3
