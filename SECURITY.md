# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Currently supported versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Spark Kindling Framework seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please do NOT:

- Open a public GitHub issue for security vulnerabilities
- Disclose the vulnerability publicly before it has been addressed

### Please DO:

**Report security vulnerabilities by emailing:** security@sep.com

Please include the following information in your report:

- Type of vulnerability (e.g., SQL injection, cross-site scripting, authentication bypass, etc.)
- Full paths of source file(s) related to the manifestation of the vulnerability
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the vulnerability, including how an attacker might exploit it

### What to expect:

- We will acknowledge your email within 48 hours
- We will provide a more detailed response within 5 business days indicating the next steps in handling your report
- We will keep you informed about the progress toward a fix and full announcement
- We may ask for additional information or guidance

### Disclosure Policy

- We will investigate and confirm the security vulnerability
- We will prepare a fix and release it as soon as possible
- We will credit you in the security advisory (unless you prefer to remain anonymous)
- We will publish a security advisory after the fix is released

## Security Best Practices

When using Spark Kindling Framework:

1. **Never commit credentials**: Use environment variables or Azure Key Vault for sensitive configuration
2. **Keep dependencies updated**: Regularly update dependencies to get security patches
3. **Use latest version**: Always use the latest stable version of the framework
4. **Review configurations**: Ensure proper access controls are configured for storage accounts and data platforms
5. **Audit data access**: Enable logging and monitoring for all data operations
6. **Validate input data**: Always validate and sanitize data from external sources
7. **Use managed identities**: When possible, use Azure Managed Identities instead of service principals
8. **Secure notebooks**: Protect notebooks containing business logic and data transformations
9. **Network security**: Use private endpoints and VNets when available
10. **Data encryption**: Enable encryption at rest and in transit for all data

## Known Security Limitations

- This framework processes data in Apache Spark which may cache data in memory
- Credentials may be visible in Spark UI if not properly masked
- Delta Lake time travel may retain deleted data for the configured retention period

## Security-Related Configuration

### Recommended Spark Security Settings

```python
spark_configs = {
    # Mask credentials in Spark UI
    "spark.sql.redaction.regex": "(?i)(password|secret|token|key)",
    
    # Enable event logging for auditing
    "spark.eventLog.enabled": "true",
    
    # Use encrypted communication
    "spark.authenticate": "true",
    "spark.network.crypto.enabled": "true",
}
```

### Azure Security Recommendations

- Use Azure Key Vault for storing secrets
- Enable Azure Storage encryption
- Use Azure Private Link for storage accounts
- Enable Azure AD authentication for Databricks/Synapse
- Configure network security groups appropriately
- Enable audit logging for all resources

## Third-Party Dependencies

This project depends on several third-party libraries. We monitor security advisories for:

- Apache Spark
- Delta Lake
- Azure SDK for Python
- Databricks SDK
- Other Python dependencies

We use automated tools (Safety, Bandit) in our CI/CD pipeline to scan for known vulnerabilities.

## Security Updates

Security updates will be:

1. Released as soon as possible after discovery
2. Announced in the CHANGELOG.md
3. Published as GitHub Security Advisories
4. Communicated to users via GitHub releases

## Contact

For security concerns, contact: security@sep.com

For general questions, use GitHub Issues or Discussions.
