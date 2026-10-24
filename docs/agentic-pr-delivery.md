# Faster pull requests in the agentic age

## Bring the power back to humans

AI agents can create changes faster than humans can review them. Humans need clear evidence to approve safe changes without reading every line.

A pull request (PR) is safe for faster review only when:

- It does not break the public API.
- It preserves behavior.
- It does not cause a performance regression.

These rules protect three promises to users. Their code still compiles. The software behaves in the same way. The software is not slower.

The crate must provide evidence for each promise. Its public API must be small and clear. Behavioral tests must cover behavior, not internal details. Deterministic benchmarks must detect performance regressions.

If a PR breaks a promise, a human must review and approve that decision. Agents provide speed and evidence. Humans keep control.

## Enable the flow

Teams has two channels:

- `Please Review (AI)`
- `Please Review (Human)`

### AI phase

The AI phase starts when the author posts a PR in `Please Review (AI)`. The author can be a human or an AI agent.

AI agents review the PR in this channel. Humans do not review it there. A human author can direct the agents that work on the published PR.

The author resolves every AI comment and makes sure that all builds pass. The AI Maintainer then creates the final report.

The report gives a verdict on the public API, behavior, and performance. It lists API additions, breaking changes, integration test changes, and performance regressions.

If all three areas are safe, the AI Maintainer approves the PR. In all cases, it attaches the report and posts the PR in `Please Review (Human)`.

### Human phase

The human phase starts when the AI Maintainer posts the PR in `Please Review (Human)`. At this point, all builds pass and no comments remain unresolved.

The channel post contains a short assessment and a link to the final report. If the AI Maintainer approved the PR, one human approval is required. Otherwise, the report identifies each decision that needs human review.

AI involvement stops when the human phase starts. Only humans can add comments, respond to comments, and approve the PR.

## Autonomous improvement loops

Autonomous loops continuously improve the project. Each loop has a narrow goal and creates small PRs.

These loops can improve performance, documentation, tests, and internal code.

Each PR must keep the public API stable, preserve behavior, and avoid performance regressions. A performance loop must provide measurements that show the improvement.

The loop posts each PR in `Please Review (AI)`. The PR then follows the same AI and human phases.

The loop does not make product decisions or change user contracts. If a PR needs a human decision, that PR waits in the human phase while the loop continues.

## Summary

AI agents prepare each PR and provide evidence. The AI Maintainer always hands the PR to the human channel. Humans make the final decisions.

Autonomous loops create small, safe improvements. Any team can adopt this model.
