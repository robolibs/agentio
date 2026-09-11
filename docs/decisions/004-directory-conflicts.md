# Directory Conflict and Replication Semantics

## Context

Replicated mode only sent best-effort announcements to static seeds. It did not
reconcile missed state, renew ownership, expire dead records, or define how two
owners claiming the same topic should be handled.

## Decision

A topic and exchange kind have at most one live owner record:

- higher revisions replace older records from the same owner;
- an older revision cannot roll state back;
- an equal revision is idempotent only for the identical signed record and is
  otherwise a typed revision conflict;
- a different owner cannot replace a live record;
- competing live owners produce an observable conflict;
- signed withdrawal removes only the current owner's record;
- records have bounded leases and expire without renewal;
- snapshots are bounded, generation-stable, and reconciled at startup,
  periodically, and on demand;
- front-door mode uses its configured authority for conflict arbitration;
- replicated mode reconciles all configured seeds independently.

## Consequences

Directory state becomes eventually convergent among reachable configured
members. Crashed owners disappear after their lease. Operators can distinguish
missing, expired, conflicted, and invalid records.

## Rejected alternatives

- Last-writer-wins across different owners was rejected because remote clocks
  and revisions are not a trustworthy ownership authority.
- Permanent entries were rejected because handle and process failures otherwise
  leave unbounded stale routing.
- Unbounded full-directory messages were rejected as a memory-exhaustion risk.
