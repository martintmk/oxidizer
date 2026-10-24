# Faster pull requests in the agentic age

## Bring the power back to humans

AI agents can create changes faster than humans can review them. Humans need a clear way to approve safe changes without reading every line.

A pull request is easier to approve when:

- It does not break the public API.
- It does not break behavioral tests.
- It does not cause a performance regression.

These checks protect three promises to users. Their code still compiles. The software still behaves in the same way. The software is not slower.

The crate structure must support these checks. The public API must be small and polished. Integration and behavioral tests must cover behavior instead of internal details. Deterministic benchmarks must measure performance and detect regressions.

If a change keeps all three promises, a human can approve it with more confidence and less effort. If it breaks a promise, a human must review and approve that decision.

Agents do the fast work. Tests provide evidence. Humans keep control of the decisions that affect users.

## Enable the flow

The Teams chat has two channels:

- Please Review (AI)
- Please Review (Human)

### AI phase

The author posts a new PR in the Please Review (AI) channel. The author can be a human or an AI agent.

AI agents use this channel as the source of PRs that are ready for review. Humans do not engage during this phase. A human author can still guide the agents that work on the published PR.

The author addresses all AI feedback manually or with agents. The AI phase ends only when all comments are resolved and all builds pass.

The AI Maintainer bot then creates a final report. It gives a clear verdict on the public API, behavioral tests, and performance. It lists public API additions, breaking changes, integration test changes, and performance regressions.

If all three areas are safe, the bot approves the PR. Whether it approves the PR or not, the bot attaches its report and posts the PR in the Please Review (Human) channel.

### Human phase

The PR enters this phase with passing builds and no unresolved comments. The channel post gives a short assessment and links to the AI Maintainer report.

If the bot approved the PR, only one human approval is required. If the bot did not approve it, the report states what needs a human decision.

AI agents stop working on the PR during this phase. Only humans can add comments, respond to comments, and approve the PR.

## Autonomous improvement loops

Autonomous loops continuously improve the project. Each loop has a narrow goal and creates small PRs that are safe to approve.

These loops can:

- Improve performance.
- Improve documentation.
- Refactor internal code.
- Remove duplication.
- Add or improve behavioral tests.

Each PR must keep the public API stable, preserve behavior, and avoid performance regressions. A performance loop must provide measurements that show the improvement.

The loop posts each PR in the Please Review (AI) channel. The PR then follows the same AI and human phases as any other change.

The loop does not make product decisions or change user contracts. If a change needs such a decision, the loop stops and asks a human.

## Summary

Fast PR delivery needs clear evidence and ownership. AI prepares each PR and reports its risks. Humans own the final review.

Autonomous loops create small, safe improvements. Any team can adopt this model.
