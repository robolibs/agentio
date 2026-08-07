# Membership and Authorization

## Context

The original builder treated bootstrap targets as peerbus inbound allowlist
entries. Bootstrap is an outbound discovery concern, while peerbus
`allow_peer` controls who may dial the local node. Coupling them blocks normal
client-to-server traffic and gives the setting misleading security semantics.

## Decision

Agentio will expose separate concepts:

- bootstrap peers are outbound directory seeds;
- allowed peers are inbound authorized endpoint IDs;
- `allow_any_peer` is an explicit trusted-network opt-out;
- front-door mode selects a directory authority but grants no authorization.

Default inbound behavior remains deny-by-default. Configuration parsing is
fallible; invalid identities are reported by `build` rather than discarded.

## Consequences

Deployments must configure both discovery and authorization deliberately.
Examples using random client identities must explicitly choose permissive
trusted-network behavior or pre-provision both endpoint IDs.

## Rejected alternatives

- Automatically authorizing bootstrap targets was rejected because it confuses
  outbound and inbound policy.
- Making every node permissive was rejected because the transport ALPN is not
  an access-control secret.
