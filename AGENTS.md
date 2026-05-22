# AGENTS.md

## Philosophy

`bilive-rec` values adequacy under real-world failure.

This project exists to do a narrow thing well: record Bilibili live streams and
upload the resulting videos to Bilibili submissions. It should not become a
general automation platform, a multi-site framework, or a wrapper around another
project's architecture.

Adequacy is not minimalism. The right design is the smallest design that still
preserves the truth of the domain: live streams expire, files can be incomplete,
uploads can partially succeed, submissions can be ambiguous, and crashes happen.

Keep what the domain requires. Remove what it does not justify.

## Core Principles

### Adequacy Over Minimality

Do not optimize for fewer lines, fewer files, fewer concepts, or fewer
dependencies by itself.

Optimize for the amount of structure the domain actually needs. A design is not
adequate if it is small but loses state, hides ambiguity, or makes recovery
depend on guesses.

### Harsh At Boundaries, Lean Inside

Be strict where reality enters the system.

External input is not trustworthy: network responses, stream data, user
configuration, files on disk, process lifetime, third-party libraries, and remote
platform behavior must all be treated carefully.

Once data crosses a boundary and becomes part of the project's own model, the
inside should be lean. Do not spread redundant defensive code through trusted
internal paths. If internal code needs constant suspicion, the boundary or model
is probably wrong.

### Persist Truth Before Risk

State is not a private implementation detail. It is how the user understands
what happened after interruption.

Before taking a risky or irreversible action, persist the fact that makes the
action explainable. Recovery should be derived from durable state and files that
exist, not from logs, timing assumptions, or hopeful control flow.

### Ownership As Design

Use Rust ownership to make responsibility visible.

State should have a natural owner. Mutation should happen where that ownership
belongs. Sharing should express a real domain relationship, not compensate for
unclear control flow.

Good ownership reduces the need for defensive code because it narrows who can
change what.

### Honest Boundaries

External systems should stay behind explicit boundaries. Their protocols,
failure modes, and data shapes should not leak into the core domain model.

The project may learn from reference implementations, but it must not inherit
their architecture. Borrow protocol knowledge only when needed, and translate it
into this project's own concepts.

### Failure Must Be Boring

Failure is a normal part of live recording.

Disconnects, expired stream URLs, invalid data, disk errors, interrupted
processes, upload failures, and uncertain submissions should produce inspectable
state and clear next steps.

Do not make state look cleaner by hiding failure. Do not silently reset,
discard, retry, delete, or rename data just to keep the happy path moving.

### Modern Rust Without Ceremony

Modern Rust is useful here because it can express ownership, state transitions,
typed boundaries, and explicit errors.

Use abstractions when they protect an invariant or make the domain clearer. Do
not add ceremony because it looks professional. Good engineering should reduce
noise.

## Refactoring Standard

Refactor when it protects an invariant.

Good refactoring should make at least one of these stronger:

- state is more truthful
- ownership is clearer
- boundaries are stricter
- invalid states are harder to express
- recovery is easier to reason about
- the domain model is less distorted by mechanisms

Large refactors are acceptable when they replace a wrong abstraction. They must
still be scoped, explainable, and justified by the domain.

Do not rewrite for elegance alone. Do not preserve a small abstraction if it lies
about the problem.

## Working Standard

When changing the project, first identify the invariant being protected.

Prefer changes that make future mistakes harder. Prefer explicit state over
implicit timing. Prefer clear refusal over unsafe convenience. Prefer a boring
failure mode over a surprising success path.

A change is not complete just because the happy path works. It must also explain
itself after interruption.

## Ethos

This project should feel like a small, typed, ownership-driven recording system
with durable truth and honest recovery.

Guiding sentence:

> Be harsh where reality enters; be lean where the model owns the truth.
