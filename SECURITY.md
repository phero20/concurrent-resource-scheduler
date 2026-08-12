# Security Policy

## Supported Versions

Currently, only the latest release of the Concurrent Resource Scheduler (CRS) library is supported with security updates. 

| Version | Supported          |
| ------- | ------------------ |
| >= 1.0.0| :white_check_mark: |
| < 1.0.0 | :x:                |

## Reporting a Vulnerability

We take the security and reliability of this concurrency library seriously. 

If you discover a security vulnerability, a severe race condition, or a concurrency flaw that could lead to data corruption or denial of service, **please do not report it by creating a public GitHub issue.**

### How to Report

Please use the **GitHub Private Vulnerability Reporting** feature:

1. Go to the [Security tab](https://github.com/phero20/concurrent-resource-scheduler/security) of this repository.
2. Click on **Report a vulnerability**.
3. Provide a detailed description of the issue.

### What to Include

To help us investigate effectively, please include:
- A clear description of the vulnerability or race condition.
- The Go version and OS where the issue was observed.
- A minimal, reproducible example (if possible) or a detailed explanation of the concurrent interleaving that causes the bug.
- Any potential impact on callers of the library.

### Responsible Disclosure

- **Do NOT** disclose the vulnerability publicly (e.g., in issues, discussions, or social media) until a fix has been released.
- We will acknowledge receipt of your vulnerability report as soon as possible and work with you to understand the problem, implement a fix, and coordinate the public disclosure.
