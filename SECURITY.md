# Security Policy

If you discover a security vulnerability in this repository, please **report it privately** and do not disclose it publicly until we have had a chance to address it.

For details on security procedures and how we handle incidents, see [Security Documentation](docs/security/).

## Reporting a Vulnerability

We strongly encourage you to report security vulnerabilities using GitHub's built-in **Security Advisory** feature. This ensures your report is handled securely and privately by the maintainers.

### How to Report

1. Navigate to the repository's **Security and quality** tab
2. Look for **Report a vulnerability** button
3. Click it to open the vulnerability reporting form
4. Fill out the form with detailed information

### What to Include

Please provide as much detail as possible to help us understand and address the issue:

- **Title:** A brief summary of the vulnerability
- **Description:** Clear explanation of what the vulnerability is and how it could be exploited
- **Steps to reproduce:** Detailed steps showing how to trigger the vulnerability
- **Potential impact:** Explanation of what an attacker could do or what could be compromised
- **Affected component:** Which part of the project is affected (ingestion, dbt models, orchestration, inference, deployment, build pipeline, etc.)
- **Proof-of-concept:** Optional, but helpful - code, screenshots, or logs demonstrating the issue
- **Your contact info:** Optional, but helpful for follow-ups if clarification is needed

The more information you provide, the faster we can assess and fix the issue.

## Response Timeline

We take all security reports seriously and are committed to addressing them promptly. Our response timeline is:

- **Within 3 business days:** We will acknowledge receipt of your report and may ask clarifying questions
- **Within 7 business days:** We will complete initial assessment and determine the severity
- **Fix and patch release:** Once verified, we will work on a fix and release it as soon as possible
- **Disclosure:** After a patch is available, we will publicly disclose the vulnerability through a GitHub Security Advisory

**Note:** These timelines begin after we receive your report. For complex vulnerabilities or those requiring extensive testing, the process may take longer. We will keep you updated on the progress.

## Responsible Disclosure

1. Do not publicly disclose the vulnerability until we have released a patch
2. Do not access or modify data beyond demonstrating the issue
3. Give us reasonable time to prepare a fix before any public disclosure
4. Keep vulnerability details confidential until we have released a patch and published the advisory

## Acknowledgments

We appreciate the security research community for responsibly disclosing vulnerabilities and helping us maintain a secure project. Thank you for your contribution to the security of Respira!

## Security Documentation

For a transparent overview of security considerations for this project, see our [Threat Model](docs/security/THREAT_MODEL.md).

## References

- [GitHub Security Advisory](https://docs.github.com/en/code-security/security-advisories)
- [Coordinated Disclosure](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/about-coordinated-disclosure-of-security-vulnerabilities)
- [GitHub Security Best Practices](https://docs.github.com/en/code-security)
