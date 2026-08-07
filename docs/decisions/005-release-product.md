# Release Product

## Context

Agentio is a library with `publish = false`, but its release workflow attempted
to build and package an example named `main`, which does not exist. The
Makefile used the same nonexistent default example.

## Decision

Agentio remains library-only. The invalid binary artifact release workflow is
removed. Source tags and releases remain operator-controlled until crate
publication or a real diagnostic binary is approved separately. `make run`
uses the existing lightweight single-machine example.

## Consequences

Automation no longer promises a nonexistent executable. A future CLI requires
its own product decision, tests, packaging contract, and supported-platform
matrix.

## Rejected alternatives

- Renaming an arbitrary example to `main` was rejected because demonstrations
  are not a supported product binary.
- Enabling crate publication was rejected because it changes release metadata
  and policy outside this plan.
