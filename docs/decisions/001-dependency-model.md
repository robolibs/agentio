# Dependency Model

## Context

Agentio used sibling path dependencies for peerbus and authbox. That worked only
inside one local directory layout and made a clean CI checkout fail before it
could compile. Peerbus also consumes authbox from Codeberg, which caused two
copies of authbox in the dependency graph.

## Decision

The committed manifest will use immutable Codeberg sources:

- peerbus revision `f4b31c6155a294a10f5902167afae6f0b78aa528`;
- authbox tag `0.1.0`, commit
  `ffbb800de5f1785c5aef90fca1d3ebd12f6f6541`.

The peerbus `0.4.0` tag is not sufficient because agentio uses later APIs,
including `skip_shm`. Local sibling overrides are developer configuration and
must not be required by the committed manifest.

## Consequences

A standalone checkout can resolve its dependencies. Agentio and peerbus share
the same authbox source. Updating peerbus requires an intentional revision
change and a clean verification run.

## Rejected alternatives

- Keeping sibling paths was rejected because CI checks out only agentio.
- Using the peerbus `develop` branch was rejected because it is mutable.
- Using peerbus tag `0.4.0` was rejected because required APIs are absent.
