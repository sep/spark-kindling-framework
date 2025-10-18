# Security Policy

## Supported Versions

We release security updates for the following versions of the Kindling Framework:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of the Kindling Framework seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Where to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities by emailing:

**security@sep.com**

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

### What to Include

Please include the following information in your report (as much as you can provide):

- Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

This information will help us triage your report more quickly.

### What to Expect

- We will acknowledge receipt of your vulnerability report within 48 hours
- We will send a more detailed response within 5 business days indicating the next steps
- We will work with you to understand the scope and severity of the issue
- We will notify you when the issue is fixed
- We will publicly acknowledge your responsible disclosure (unless you prefer to remain anonymous)

## Security Best Practices

When using the Kindling Framework:

### Credentials Management
- **Never commit credentials** to source control (`.env` files, connection strings, API keys)
- Use environment variables or Azure Key Vault for sensitive configuration
- Use managed identities when deploying to Azure services
- Rotate service principal credentials regularly

### Data Access
- Follow the principle of least privilege for storage account access
- Use Azure RBAC (Role-Based Access Control) for fine-grained permissions
- Enable Azure Storage account firewalls and virtual network rules
- Use SAS tokens with limited scope and expiration when appropriate

### Dependencies
- Keep dependencies up to date
- Monitor for security advisories in Python packages
- Use `pip install --upgrade` regularly for security patches
- Review dependency licenses for compatibility

### Platform Security
- Enable audit logging in your Azure Synapse/Fabric/Databricks workspaces
- Monitor for unusual activity in your data pipelines
- Implement data classification and sensitivity labels
- Use encryption at rest and in transit

### Code Security
- Validate and sanitize all external inputs
- Avoid dynamic SQL generation; use parameterized queries
- Be cautious with `eval()` or `exec()` functions
- Review third-party code before including in your pipelines

## Security Updates

Security updates will be released as soon as possible after a vulnerability is confirmed and patched. Updates will be announced via:

- GitHub Security Advisories
- Release notes in CHANGELOG.md
- GitHub releases

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release patches as soon as possible

We aim to fully disclose security issues once a fix is available and deployed. We will credit the reporter in our security advisories (unless anonymity is requested).

## Comments on This Policy

If you have suggestions on how this process could be improved, please submit a pull request or open an issue.

## Attribution

This security policy is adapted from industry best practices and recommendations from:
- [GitHub's Security Lab](https://securitylab.github.com/)
- [OWASP Vulnerability Disclosure Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Vulnerability_Disclosure_Cheat_Sheet.html)

---

**Maintained by:** Software Engineering Professionals, Inc.  
**Contact:** security@sep.com  
**Last Updated:** October 2025
