# Authenticated Topic Ownership

## Context

Directory announcements previously contained an endpoint ID but no proof that
the announcer owned that identity. Any permitted caller could overwrite topic
routing. Topic entries also lacked exchange and response type information.

## Decision

Every announced or withdrawn record will carry a detached Ed25519 signature
over a canonical postcard encoding of:

- protocol version;
- normalized topic;
- exchange kind;
- request or payload type hash;
- optional response type hash;
- owner endpoint ID;
- owner revision;
- lease expiry;
- optional machine name;
- operation kind.

Signing and verification use authbox
`sign_ed25519_detached` and `verify_ed25519_signature`. Agentio retains the
minimum private seed needed to sign its own records and never serializes or
logs it. Invalid, expired, oversized, or mismatched records are rejected before
directory mutation or peer dialing.

## Consequences

An authorized transport peer cannot impersonate a different topic owner.
Changing any signed field invalidates the record. The current unreleased wire
format is replaced rather than supported in parallel.

## Rejected alternatives

- Trusting the request connection was rejected because the current peerbus
  request sample does not expose an authenticated caller identity.
- Custom cryptography was rejected in favor of the existing reviewed identity
  stack.
- Unsigned withdrawals were rejected because they permit unauthorized removal.
