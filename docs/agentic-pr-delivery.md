# Faster pull requests with AI agents

## Keep humans in control

AI agents create changes faster than humans can review them. A pull request (PR) qualifies for faster review when:

- It does not break the public API.
- It preserves behavior.
- It does not cause a performance regression.

The crate needs a clear and stable public API, behavior-focused tests, and deterministic benchmarks. If a PR fails a criterion, a human must approve that change.

## Review process

Teams has two channels:

- `Please Review (AI)`
- `Please Review (Human)`

### AI phase

The author can be a human or an AI agent. The author posts the PR in `Please Review (AI)`.

AI agents review the PR. A human author can direct their work. The author resolves all AI comments and makes sure that all builds pass.

The AI Maintainer is a bot that does the final AI review. The AI Maintainer reports on the public API, behavior, and performance. The report lists public API additions, breaking API changes, integration test changes, and performance regressions.

If the PR meets all three criteria, the AI Maintainer approves it. The AI Maintainer always attaches its report and posts the PR in `Please Review (Human)`.

### Human phase

AI agents stop work when the AI Maintainer posts the PR. An approved PR needs one human approval. Otherwise, the report identifies the decisions that need human review.

The channel post contains a short assessment and a link to the report.

Only humans can add comments, respond to comments, and approve the PR.

## Autonomous improvement loops

An autonomous loop is an AI process that works without human input. Each loop continuously improves the project. Each loop has one goal and creates a separate PR for each change.

These loops can improve performance, documentation, tests, and internal code.

Each PR must meet the same criteria. A performance loop must provide measurements that show the improvement.

The loop posts each PR in `Please Review (AI)`. The PR then follows the same review process.

## Summary

AI agents prepare each PR and provide evidence. The AI Maintainer always posts the PR in `Please Review (Human)`. Humans make the final decisions.

Autonomous loops create safe improvements. Any team can use this review process.
