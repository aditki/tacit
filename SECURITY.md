# SECURITY.md

# Security Policy

## Project Status

Tacit is an experimental infrastructure engineering and observability tooling project.

It is currently:

* early-stage
* rapidly evolving
* not production-ready
* intended primarily for experimentation, learning, and portfolio demonstration

The repository explores:

* LLM-assisted observability workflows
* Grafana dashboard automation
* multi-agent orchestration
* investigation planning systems
* operational cognition infrastructure

Because of this, the project should currently be treated as a **development/private-beta system only**.

## Supported Versions

At this time, no versions are officially supported for production deployment.

Security fixes, APIs, configuration formats, and architecture may change significantly between releases.

## Reporting Security Issues

If you discover a security issue, please avoid opening a public GitHub issue.

Instead:

* open a private GitHub security advisory if available
* or, if private advisories are not enabled, open a public issue titled
  `Security disclosure channel request` with no vulnerability details so the
  maintainer can provide a private response channel before details are shared

Please include:

* description of the issue
* reproduction steps
* potential impact
* suggested remediation (if known)

Reasonable disclosure efforts are appreciated.

## Security Expectations

Tacit integrates with:

* Grafana
* observability datasources
* LLM providers
* Slack APIs
* infrastructure telemetry systems

Users should assume generated queries, dashboards, and integrations require careful review before operational use.

### Recommended Safe Usage

* Use least-privilege Grafana service accounts
* Avoid Administrator-level API tokens
* Run only in isolated development/testing environments
* Do not expose Tacit publicly to the internet
* Restrict outbound network access where practical
* Review generated dashboards and queries before sharing internally
* Avoid storing secrets directly in repository files
* Rotate any accidentally exposed credentials immediately

## AI / LLM Caveats

Tacit uses LLMs for:

* intent classification
* metric selection
* query generation
* investigation planning

LLM-generated output may:

* be incorrect
* hallucinate
* generate inefficient queries
* misunderstand operational context
* produce misleading investigation paths

Human review is required.

Tacit should currently be treated as:

* an investigation assistant
* not an autonomous operational system

## Threat Model Limitations

The project currently does **not** guarantee protection against:

* prompt injection attacks
* malicious datasource content
* unsafe generated queries
* multi-tenant isolation risks
* privilege escalation via connected systems
* adversarial telemetry poisoning
* high-cardinality query abuse
* denial-of-service scenarios

Some mitigations exist, but the system has not undergone formal security review.

## Responsible Usage

If you experiment with Tacit:

* avoid connecting sensitive production environments
* avoid exposing internal telemetry publicly
* use non-critical infrastructure where possible
* validate all generated investigations manually

## No Warranty

This project is provided under the MIT License without warranty of any kind.

Use at your own risk.
