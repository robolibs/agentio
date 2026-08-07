# Agentio Stabilization and Completion Plan

> **Executor instructions**: This is the master implementation plan for the
> complete repository audit performed on 2026-08-07. Read the entire document
> before editing anything. Execute phases in dependency order, run every
> verification gate, and stop rather than improvising when a STOP condition is
> met. Do not combine phases into one unreviewable change.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Category**: correctness, security, tests, architecture, dependencies, DX,
  performance, release, and documentation
- **Planned at**: commit `df87e97`, 2026-08-07
- **Overall status**: TODO

## Mandatory repository rules

These rules apply to every phase:

- Use the repository `Makefile` for all relevant build, test, lint, formatting,
  documentation, and example commands.
- Do not use Python to edit files. Use patch/edit tools.
- Do not add signatures, trailers, co-author lines, or signoffs to commits.
- Commit messages must contain only a Conventional Commit title in this form:
  `<type>(<optional-scope>):<message>`, with no body and no title longer than
  50 characters.
- Keep code comments descriptive. Do not use comments to justify decisions.
  Comments in a code block must remain below 20 percent of its implementation
  lines.
- Do not change package or release version metadata unless the operator
  explicitly requests it.
- Do not push, publish, open a pull request, create a release, or modify tags
  unless the operator explicitly requests it.

## Baseline and drift check

The operator approved and established baseline commit `df87e97`. Before each
phase:

> **Tracking note**: the active global Git ignore file contains a `PLAN.md`
> pattern, so this file does not appear in normal `git status` output. If the
> operator wants it versioned, they must explicitly stage
> `plans/PLAN.md` with `git add -f plans/PLAN.md`. Do not stage it
> automatically.

1. Run `git status --short --branch`.
2. Confirm the baseline remains in history.
3. Run:

   ```bash
   git diff --stat df87e97..HEAD -- \
     Cargo.toml Cargo.lock Makefile README.md .github src examples tests
   ```

4. Compare the current-state excerpts below with the live code. Any material
   mismatch is a STOP condition.

## Goals

After all phases:

1. A clean standalone checkout builds and passes `make verify`.
2. `Agent` shutdown releases its resolver thread, request server, and node.
3. Outbound bootstrap seeds and inbound peer authorization have distinct APIs.
4. Directory records are authenticated, bounded, reconciled, and withdrawn or
   expired when their owner disappears.
5. Control-plane failures are returned or exposed through observable health
   state rather than discarded.
6. Public-name-derived private keys are no longer available as production
   identities.
7. Critical single-host and forced-QUIC paths have automated integration tests.
8. Heavy examples transfer and measure real payload bytes.
9. Run, CI, and release behavior match the fact that `agentio` is a library.
10. `NameTable`, participant terminology, README claims, and public APIs agree.

## Non-goals

- Rewriting peerbus transport internals.
- Adding a second compatibility protocol for the current unreleased directory
  wire format.
- Changing datapod's schema or wire implementation.
- Publishing `agentio` or changing `publish = false` without a separate product
  decision.
- Adding a GUI, CLI product, plugin system, database, or durable history store.
- Optimizing microbenchmarks before the benchmarks transfer real bytes.

## Current state

### Build and automation

- `Cargo.toml:15-16` uses `../peerbus` and `../authbox` path dependencies.
- `.github/workflows/tests.yml:15-29` checks out only this repository and runs
  `make verify`, so a clean runner cannot load those sibling paths.
- `make verify` currently stops at `fmt-check`.
- `make clippy` currently fails on six denied warnings.
- `Makefile:11` defaults `EXAMPLE` to nonexistent `main`.
- `.github/workflows/release.yml:30-32` also builds and packages nonexistent
  example `main`.
- `make check`, `make test`, `make check-all`, `make test-all`, and
  `make rustdoc` pass in the local sibling-repository layout.

### Agent lifecycle and control plane

`src/agent/builder.rs:133-149` currently creates a detached thread:

```rust
let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
if let Ok(server) = node.req_server::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC) {
    let dir_clone = directory.clone();
    std::thread::spawn(move || {
        run_resolution_loop(server, dir_clone, shutdown_rx);
    });
}
```

`src/agent/core.rs:349-353` exits only after a received value:

```rust
if shutdown_rx.try_recv().is_ok() {
    break;
}
```

Dropping the sender closes the channel and returns an error, which the loop
currently treats as "keep running". The worker owns `ReqServer`, and that server
owns the underlying peerbus node state.

### Membership and ACLs

`src/agent/builder.rs:104-112` adds the local endpoint and bootstrap targets to
peerbus's inbound allowlist. Peerbus defines `allow_peer` as inbound-only;
outbound dialing is not restricted. Therefore a client that bootstraps a server
does not cause the server to authorize that client.

`src/agent/builder.rs:71-77` also discards invalid bootstrap identities instead
of failing configuration.

### Directory protocol

- `ResolveRequest::List` is implemented but never initiated.
- `Announce` records are accepted and installed without proving that the caller
  owns the claimed endpoint.
- Announcements are best effort and failures are discarded.
- Replicated mode only contacts a static bootstrap list.
- Publisher/server handle drops do not withdraw their `TopicEntry`.
- Entries contain no revision, lease, expiry, signature, or exchange signature.
- A directory maps one topic string to one entry without an explicit conflict
  rule.

### Identity and naming

`src/identity/source.rs:67-71` derives an Ed25519 secret directly from public
name bytes using BLAKE3. Anyone who guesses the name can reproduce the private
key.

`src/naming/table.rs:17-22` updates both maps without removing displaced old
relationships, leaving stale reverse mappings after a rename or reassignment.

`Participant` is a public data structure, but there is no participant
registration, discovery, ownership, or synchronization API despite README
claims that participant namespacing is a core capability.

### Test and benchmark coverage

- The suite has eight leaf-level unit tests.
- `make test` compiles every example as a zero-test binary but does not run it.
- There are no automated Agent lifecycle, remote referral, ACL, synchronization,
  malformed-control-message, or withdrawal tests.
- `examples/08_heavy_server.rs` sends a real payload only for pub/sub. The RPC,
  que/ans, put/ack, and pip paths send small headers containing claimed byte
  counts, while the client calculates throughput from those counts.

## Required design decisions

Record these decisions in `docs/decisions/` before implementing the dependent
phases. Each decision file should state context, choice, consequences, and
rejected alternatives without embedding implementation history in code
comments.

### Decision A: standalone dependency model

**Resolved choice**: make a clean agentio checkout self-contained with these
verified immutable Codeberg references:

- peerbus revision
  `f4b31c6155a294a10f5902167afae6f0b78aa528`;
- authbox tag `0.1.0`, resolving to
  `ffbb800de5f1785c5aef90fca1d3ebd12f6f6541`.

Both references were verified against Codeberg on 2026-08-07. Do not use the
peerbus `0.4.0` tag: the current agentio API depends on peerbus changes after
that tag, including `skip_shm`. A local-development override may be documented
outside the committed release manifest, but the committed manifest must work
without sibling repositories.

### Decision B: membership and authorization

**Recommended choice**: separate these concepts:

- `bootstrap` or `seed`: outbound directory contacts only.
- `allow_peer`: inbound authorized endpoint IDs only.
- `allow_any_peer`: explicit insecure/trusted-network opt-out.
- `FrontDoor`: directory routing policy, not authorization policy.

Never infer that an outbound seed should be authorized inbound. Examples that
use random client identities may use `allow_any_peer` only when their output and
README text state the trusted-network limitation.

### Decision C: authenticated topic ownership

**Recommended choice**: use signed owner records rather than trusting the
request sender. A record must bind at least:

- normalized topic;
- exchange kind;
- request/payload type hash;
- response type hash where applicable;
- owner EndpointId;
- owner revision;
- lease expiry;
- optional machine name.

Use authbox `0.1.0`'s existing
`authbox::pki::{sign_ed25519_detached, verify_ed25519_signature}` API. It
delegates to keylock's Ed25519 implementation and accepts the 32-byte seed
returned by peerbus/iroh `SecretKey::to_bytes()`. Do not implement custom
cryptography. Retain only the minimum signing capability needed by the Agent;
never expose or serialize the secret seed.

### Decision D: conflict and replication semantics

**Recommended choice**:

- A topic plus exchange kind is owned by at most one live owner record.
- Newer valid owner revisions replace older records from the same owner.
- A different owner cannot replace a live record merely by using a larger
  revision.
- Conflicting live owners produce a typed conflict error and remain observable.
- Records expire without renewal.
- `Withdraw` removes a record only when signed by its current owner.
- Replicated members reconcile a bounded snapshot on startup and reconnection.
- `FrontDoor` remains the authoritative conflict arbiter in front-door mode.

### Decision E: release product

**Recommended choice**: treat agentio as a library-only repository for now.
Remove the binary-artifact release workflow rather than inventing a `main`
example to package. Keep source tags/releases manual until publishing or a real
diagnostic binary is separately approved.

## Commands

Use only the Makefile for relevant repository validation:

| Purpose | Command | Expected result |
|---|---|---|
| Format check | `make fmt-check` | exit 0, no diff |
| Compile | `make check` | exit 0 |
| Compile all features | `make check-all` | exit 0 |
| Tests | `make test` | exit 0, all tests pass |
| Tests all features | `make test-all` | exit 0 |
| Lint | `make clippy` | exit 0, no warnings |
| Documentation | `make rustdoc` | exit 0, no warnings |
| Full gate | `make verify` | exit 0 |
| Default example | `make run` | runs the selected valid example |

If a new specialized test or benchmark command is required, add a named
Makefile target first and invoke that target. Do not bypass the Makefile in the
implementation workflow.

## Scope

### In scope

- `Cargo.toml`
- `Cargo.lock`
- `Makefile`
- `README.md`
- `.github/workflows/tests.yml`
- `.github/workflows/release.yml`
- `src/agent/builder.rs`
- `src/agent/core.rs`
- `src/agent/mode.rs`
- `src/directory/entry.rs`
- `src/directory/protocol.rs`
- `src/directory/store.rs`
- `src/error.rs`
- `src/identity/source.rs`
- `src/identity/mod.rs` when required by the selected signing API
- `src/naming/table.rs`
- `src/naming/machine.rs`
- `src/lib.rs`
- `examples/*.rs`
- `tests/*.rs` and shared test support files to be created
- `docs/decisions/*.md` to be created

### Conditionally in scope

- `../peerbus`: only after the operator explicitly approves cross-repository
  changes and only if Decision C cannot be implemented safely through existing
  APIs.
- `flake.nix`, `flake.lock`, and `.envrc`: only if standalone validation proves
  the existing graphics/NVIDIA-heavy shell blocks supported agentio platforms.

### Out of scope

- Datapod internals or schema changes.
- Authbox/keylock cryptographic implementation changes.
- Peerbus transport rewrites.
- Version bumps and publication metadata.
- Compatibility readers for the current unreleased directory messages.

## Git workflow

After the operator establishes a baseline:

- Work on a dedicated branch such as `fix/agentio-stabilization` unless the
  operator supplies another branch constraint.
- Commit each completed phase separately.
- Never use `git commit -S`, `--signoff`, trailers, bodies, or signatures.
- Allowed title-only examples, all below 50 characters:
  - `chore(ci):restore verification gate`
  - `test(agent):add referral coverage`
  - `fix(agent):stop resolver leak`
  - `fix(agent):separate seeds and ACLs`
  - `feat(directory):sign topic records`
  - `feat(directory):reconcile replicas`
  - `fix(identity):remove derived secrets`
  - `fix(naming):preserve map invariants`
  - `fix(examples):measure real payloads`
  - `docs(api):align composition contract`

## Execution order

| Phase | Title | Priority | Effort | Depends on | Status |
|---|---|---:|---:|---|---|
| 0 | Establish baseline and decisions | P1 | S | none | DONE |
| 1 | Restore clean build and automation | P1 | M | 0 | DONE |
| 2 | Add characterization test harness | P1 | M | 1 | DONE |
| 3 | Fix resolver lifecycle and errors | P1 | M | 2 | DONE |
| 4 | Separate membership from ACLs | P1 | M | 2 | IN PROGRESS |
| 5 | Authenticate directory records | P1 | L | 3, 4 | DONE |
| 6 | Reconcile and expire directory state | P1 | L | 5 | DONE |
| 7 | Remove insecure derived identities | P1 | M | 2 | DONE |
| 8 | Fix naming and participant contract | P2 | M | 6 | DONE |
| 9 | Repair examples and benchmarks | P2 | M | 4, 6 | DONE |
| 10 | Align documentation and release model | P2 | M | 1-9 | TODO |
| 11 | Run final security and release gates | P1 | M | 1-10 | TODO |

## Phase 0: Establish the baseline and decisions

### Steps

1. Obtain an operator-approved baseline commit and write its short SHA into
   this plan.
2. Create the five decision records described above:
   - `docs/decisions/001-dependency-model.md`
   - `docs/decisions/002-membership-authorization.md`
   - `docs/decisions/003-topic-ownership.md`
   - `docs/decisions/004-directory-conflicts.md`
   - `docs/decisions/005-release-product.md`
3. Confirm each recommended choice or record the operator-approved alternative.
4. Update the execution table if an alternative changes dependencies.

### Verify

- `make check` must still exit 0.
- `git diff --check` must report no whitespace errors.
- Each decision file must contain `Context`, `Decision`, `Consequences`, and
  `Rejected alternatives` headings.

### Commit

`docs(architecture):record agentio decisions`

## Phase 1: Restore clean build and automation

### Steps

1. Implement Decision A in `Cargo.toml` and regenerate `Cargo.lock` through a
   Makefile target. Ensure agentio and peerbus resolve one compatible authbox
   source rather than path and Git copies of the same version.
2. Extend the Makefile with any required lockfile/update target so dependency
   operations remain Makefile-driven.
3. Format the agentio package without rewriting sibling repositories. If
   `cargo fmt --all` continues traversing unrelated local packages, scope the
   Makefile target to agentio.
4. Resolve all current clippy failures without blanket lint suppression:
   - implement `Default` for `AgentBuilder`;
   - simplify the reported conditional patterns;
   - derive `Default` for `IdentitySource` when it preserves behavior.
5. Set `EXAMPLE ?= 01_single_machine` or another existing lightweight example.
6. Implement Decision E. Under the recommended library-only decision, remove
   the invalid example-binary release workflow. Do not create a fake binary.
7. Update the test workflow so a clean checkout has everything needed before
   `make verify`.
8. Validate from a fresh temporary checkout or archive containing no sibling
   repositories.

### Tests

- Clean standalone `make check`.
- Clean standalone `make verify`.
- `make run` starts the valid default example; use a temporary
  `AGENTIO_KEYS_DIR` in automation so no user keys are written.

### Done criteria

- [ ] `make verify` exits 0 locally.
- [ ] A clean standalone copy also passes `make verify`.
- [ ] `Cargo.lock` has only the intended authbox source.
- [ ] No workflow references example `main`.
- [ ] `make run` no longer reports a missing target.
- [ ] No lint was disabled to make clippy green.

### Commit

`chore(ci):restore verification gate`

## Phase 2: Add the characterization test harness

Do this before changing lifecycle, ACL, or directory behavior.

### Steps

1. Create integration-test support that always uses:
   - `IdentitySource::Random` unless persistence is the subject of the test;
   - unique topic names;
   - bounded receive/call timeouts;
   - deterministic cleanup;
   - `skip_shm()` for tests claiming to exercise remote QUIC.
2. Add `tests/agent_local.rs` covering local publish/subscribe and all four
   service families.
3. Add `tests/referral_remote.rs` covering forced-QUIC referral and direct
   `by_id` access.
4. Add `tests/agent_lifecycle.rs` with a regression fixture that can prove the
   resolver worker exits. Prefer an internal test hook or joinable worker state
   over counting operating-system threads.
5. Add `tests/control_plane_errors.rs` for invalid bootstrap identities and a
   forced resolution-server initialization failure.
6. Add named Makefile targets for focused integration and forced-remote tests.
7. Document which tests intentionally fail against the current behavior, then
   implement each paired fix in the immediately following phase. Do not leave
   failing tests on a committed branch.

### Verify

- `make test` passes after each committed test/fix slice.
- The forced-remote target must prove SHM is disabled.
- No test depends on sleeps alone for correctness; use bounded readiness or
  polling APIs.

### Commit

`test(agent):add referral coverage`

## Phase 3: Fix resolver lifecycle and control-plane errors

### Steps

1. Change Agent construction so failure to register `RESOLUTION_TOPIC` returns
   an error from `build()`; never return an Agent without its resolver.
2. Store the spawned resolution worker's `JoinHandle` alongside its shutdown
   sender in `AgentInner`.
3. Implement deterministic, idempotent shutdown:
   - the last `AgentInner` drop sends shutdown;
   - a closed channel also terminates the loop;
   - the worker is joined;
   - the peerbus node is closed or allowed to drop only after the worker releases
     its `ReqServer`.
4. If public explicit shutdown is useful, add an idempotent `Agent::close` or
   `Agent::shutdown` API whose behavior with cloned Agents is documented and
   tested. Do not let one clone silently invalidate others unless that is the
   recorded API decision.
5. Preserve and report resolver receive, decode, and response errors through
   typed errors, structured tracing, or health counters. Malformed remote input
   must not kill the resolver loop.
6. Change bootstrap conversion so invalid identities are retained as builder
   configuration errors and returned by `build()`, or provide a fallible
   `try_bootstrap` API. Do not silently drop them.
7. Replace discarded announcement results with an observable result or queued
   retry state. Avoid making a local publisher half-register and then return an
   error without rollback.

### Tests

- Dropping the last Agent terminates and joins its resolver worker.
- Dropping one of several Agent clones does not stop the live Agent.
- Resolver registration failure makes `build()` fail.
- Invalid bootstrap DID/endpoint input makes configuration fail.
- Malformed resolution bytes return or log a bounded error and the next valid
  request still succeeds.
- Failed announcement delivery is visible in health/status state.

### Verify

- `make test` exits 0.
- `make clippy` exits 0.
- `make rustdoc` exits 0.

### Commit

`fix(agent):stop resolver leak`

## Phase 4: Separate membership, seeds, and inbound ACLs

### Steps

1. Implement Decision B in `AgentBuilder` and `DirectoryMode`:
   - bootstrap seeds affect resolution targets only;
   - inbound allowed peers are configured through a separate method;
   - duplicate seeds and allowed peers are deduplicated independently;
   - `FrontDoor(id)` automatically becomes a resolution target but not an
     inbound authorization grant.
2. Preserve deny-by-default peerbus behavior unless the caller explicitly uses
   `allow_any_peer`.
3. Add accessors or debug output that let operators inspect configured seeds,
   allowed peers, directory mode, relay mode, and SHM mode without exposing
   secret key material.
4. Update all examples:
   - single-process examples should use explicit random identities;
   - cross-machine examples must state whether they use a trusted-network
     permissive ACL or preconfigured endpoint allowlists;
   - examples must not imply that `bootstrap(server)` authorizes the client at
     the server.
5. Update README membership and security sections before merging the API
   change.

### Tests

- A server rejects an unlisted forced-QUIC client.
- Adding the client to `allow_peer` permits it.
- Adding the server only as the client's seed does not alter either node's
  inbound ACL.
- `allow_any_peer` permits the connection and emits the expected warning path.
- Front-door routing works with an independently correct ACL.

### Verify

- `make test` and the forced-remote Makefile target exit 0.
- `grep -R "bootstrap.*allow_peer" src/agent` finds no coupling of seeds to
  inbound ACL construction.

### Commit

`fix(agent):separate seeds and ACLs`

## Phase 5: Authenticate directory records

### Steps

1. Implement Decisions C and D in `directory/entry.rs` and
   `directory/protocol.rs`. Use one new current wire format; do not carry a
   legacy reader for the unreleased format.
2. Replace the current unverified `TopicEntry` with a signed owner record. Keep
   signature verification separate from serialization.
3. Represent the exchange kind explicitly: pub/sub, req/res, que/ans, put/ack,
   or pip. Include both request and response hashes for bidirectional service
   types.
4. Add typed errors for invalid signatures, expired records, ownership
   conflicts, unsupported protocol values, and type/exchange mismatches.
5. Sign announcements and withdrawals using the Agent identity already bound to
   the peerbus endpoint. Never log, serialize, or expose the secret key.
6. Verify every received record before inserting it into the Directory.
7. Validate a resolved record against the generic types requested by
   `subscribe`, `req_client`, `que_client`, `put_client`, and `pip_client`
   before dialing peerbus. Retain peerbus handshake validation as defense in
   depth.
8. Bound list and announcement batch sizes before allocation or insertion.
9. Reject malformed or oversized records without terminating the resolver.

### Tests

- Valid owner-signed announcement is accepted.
- Modified topic, owner, hash, expiry, or signature is rejected.
- An allowed but non-owning peer cannot claim another endpoint's topic.
- Expired records are rejected.
- A valid signed withdrawal removes only the owner's current record.
- Wrong exchange kind, request hash, or response hash fails before dialing.
- Oversized batches are rejected at the boundary.

### Verify

- `make test` exits 0 with the security regression tests.
- `make clippy` exits 0.
- No secret bytes appear in test snapshots, errors, or tracing fields.

### Commit

`feat(directory):sign topic records`

## Phase 6: Reconcile, renew, and expire directory state

### Steps

1. Turn `ResolveRequest::List` into a bounded snapshot/reconciliation flow used
   on startup and reconnect.
2. Add owner revisions and leases according to Decision D.
3. Add periodic renewal for every locally hosted publisher/server record.
4. Add explicit withdrawal when a hosted exchange handle closes when the API
   can observe that lifecycle. Where peerbus handle ownership prevents direct
   observation, return agentio wrapper types that delegate the peerbus API and
   withdraw on drop.
5. Expire records whose lease is not renewed; never resolve an expired record.
6. Implement deterministic reconciliation with all configured seeds. A failed
   seed must not prevent healthy seeds from reconciling.
7. Keep front-door and replicated behavior distinct:
   - front-door clients reconcile with the authority;
   - replicated nodes reconcile snapshots and renew their own signed records.
8. Expose directory health information: last successful reconciliation, stale
   seed count, conflicts, rejected records, and pending announcements.
9. Replace fixed `10 × 50 ms` caller-thread sleeps with bounded control-plane
   state and explicit timeouts. Avoid an unbounded blocking call on a robotics
   application thread.

### Tests

- A node that missed an announcement learns it through snapshot reconciliation.
- A late-joining node converges without republishing every topic.
- Withdrawal removes a record from resolvers.
- A crashed owner record expires after its lease.
- Replay of an older signed revision does not overwrite newer state.
- Multiple seeds reconcile despite one unreachable seed.
- Conflicting live owners produce the documented typed error.
- Resolution timeout duration is bounded and tested without wall-clock flakiness.

### Verify

- `make test` exits 0.
- The forced-remote integration target exercises reconnect and reconciliation.
- `grep -R "thread::sleep" src/agent src/directory` returns no fixed retry sleep
  in caller-facing resolution paths.

### Commit

`feat(directory):reconcile replicas`

## Phase 7: Remove insecure name-derived identities

### Steps

1. Remove `IdentitySource::DerivedName` and
   `derive_secret_from_name` from the production public API under the
   recommended decision.
2. If deterministic identities are required for tests, move the helper under
   `#[cfg(test)]` and name it so it cannot be mistaken for a secure production
   identity.
3. Do not replace raw BLAKE3 with a password KDF and continue calling a public
   name a secret. A deterministic production identity must require explicit
   high-entropy secret input and a reviewed KDF design.
4. Add migration documentation warning that identities created through the old
   helper are compromised if the input name was public. Do not print or collect
   any existing key material.
5. Make identity persistence errors visible. If copying a key into the DID
   index is part of the contract, do not discard `save_did_key` errors.
6. Validate existing key file length and Unix permission posture. Do not
   silently rewrite user key files.
7. Serialize environment-mutating identity tests so `AGENTIO_KEYS_DIR` cannot
   race across parallel tests and always restore it after failure.

### Tests

- Production exports contain no name-to-private-key derivation helper.
- Random identities differ.
- Named persistent identities reload the same key from a temporary directory.
- Invalid key length produces a typed error.
- DID-index write failure is observable according to the documented contract.
- Environment changes are isolated between tests.

### Verify

- `make test` exits 0.
- `grep -R "DerivedName\|derive_secret_from_name" src README.md` returns no
  production references.

### Commit

`fix(identity):remove derived secrets`

## Phase 8: Fix naming and the participant contract

### Steps

1. Fix `NameTable::register` so reassignment removes stale mappings in both
   directions under one consistent mutation boundary.
2. Define and test collision behavior:
   - same name and same endpoint is idempotent;
   - same name and new endpoint removes the old reverse entry;
   - same endpoint and new name removes the old forward entry.
3. Populate the NameTable from verified live directory records, and remove
   mappings only when no live record still supports them.
4. Choose one participant scope:
   - **Recommended minimal scope**: make participants first-class local topic
     namespaces with explicit registration and documented machine ownership;
     synchronize them only through signed topic records.
   - If maintainers do not want that API yet, remove claims of participant
     discovery/composition and keep only `qualify_participant_topic`.
5. Avoid creating a separate participant registry unless it has lifecycle,
   ownership, and conflict rules consistent with the directory.

### Tests

- All three NameTable collision cases above.
- Verified remote records populate the expected machine mapping.
- Expired/withdrawn last records remove their supported mapping.
- Participant-relative and absolute topics retain current normalization rules.

### Verify

- `make test` and `make rustdoc` exit 0.
- README terminology matches the selected participant scope.

### Commit

`fix(naming):preserve map invariants`

## Phase 9: Repair examples and heavy benchmarks

### Steps

1. Update basic examples to use the new membership and ACL APIs.
2. Remove correctness checks that rely only on sleeps. Use direct-address
   readiness, bounded receive timeouts, and asserted results.
3. Ensure examples return an error when expected data was not received instead
   of printing `[OK]` unconditionally.
4. For every heavy pattern, allocate and transfer the claimed bytes through the
   datapod payload:
   - pub/sub frame payload;
   - RPC response payload;
   - each que/ans chunk payload;
   - each put/ack upload block payload;
   - each pip frame and response payload where claimed.
5. Compute throughput from actual payload lengths observed at the receiving
   side, not header metadata.
6. Verify deterministic checksums or content patterns on receipt.
7. Rename the examples from "benchmark" to "throughput demo" unless a proper
   repeatable benchmark harness with warmup, sample count, and summary
   statistics is added.
8. Add Makefile targets for a bounded smoke run. Do not run infinite server
   loops in CI.

### Tests

- Tampering with a claimed byte count cannot inflate measured throughput.
- Each exchange validates actual received length and checksum.
- No example prints success when it received fewer than the required samples.
- Smoke mode exits deterministically.

### Verify

- `make examples-smoke` or the selected named target exits 0.
- `make verify` exits 0.
- Manual two-process forced-QUIC smoke run succeeds with temporary identities.

### Commit

`fix(examples):measure real payloads`

## Phase 10: Align documentation and the release model

### Steps

1. Rewrite README architecture claims against the implemented API:
   - machine versus Agent;
   - seeds versus inbound allowed peers;
   - front-door versus replicated resolution;
   - signed records, leases, conflicts, and expiry;
   - participant scope;
   - SHM versus QUIC behavior;
   - identity persistence and security.
2. Use Makefile commands in README examples.
3. Add a security section explaining deny-by-default ACLs and the risks of
   `allow_any_peer` without presenting ALPN as a secret.
4. Add lifecycle documentation for Agent clones and shutdown.
5. Document control-plane health and typed failure modes.
6. Apply Decision E consistently to Makefile help, workflows, and README.
7. Ensure public rustdoc covers every new public method and type without
   justification comments.

### Verify

- Every README command names an existing Makefile target.
- `make rustdoc` exits 0 with warnings denied.
- `make run` follows the documented default behavior.
- No docs claim automatic discovery, participant synchronization, authenticated
  ownership, or binary releases unless implemented and tested.

### Commit

`docs(api):align composition contract`

## Phase 11: Final security and release gates

### Steps

1. Run the complete gate in a clean standalone checkout.
2. Run all forced-remote integration tests with SHM disabled.
3. Run the bounded example smoke suite.
4. Run the ecosystem dependency advisory scanner through a dedicated Makefile
   target. Report reachable high/critical advisories; do not dump unrelated
   low-severity noise into the plan.
5. Inspect `Cargo.lock` for duplicate sources of authbox, peerbus, datapod, and
   incompatible duplicate HTTP/crypto stacks introduced by source drift.
6. Review tracing and errors to confirm no secret key bytes, identity seed
   material, or unbounded remote payloads are logged.
7. Verify all plan scope, commit-message, and no-signature rules.

### Final verification

All must pass:

```text
make fmt-check
make check
make check-all
make test
make test-all
make clippy
make rustdoc
make verify
make examples-smoke
make audit
```

Expected result: every command exits 0; all automated tests pass; formatting,
clippy, rustdoc, and advisory gates report no blocking findings.

### Commit

`test(agent):complete stabilization gate`

## Complete test matrix

| Area | Required coverage |
|---|---|
| Local transport | all five exchange patterns |
| Forced QUIC | referral plus all five clients |
| ACL | denied, explicit allow, explicit allow-any |
| Resolver lifecycle | clone, last-drop, explicit close if added |
| Builder errors | bad seed, resolver collision, key failure |
| Record security | valid, forged, modified, replayed, expired |
| Directory state | announce, snapshot, renewal, withdrawal, expiry |
| Conflicts | same owner revision and competing live owner |
| Types | exchange/request/response mismatch before dial |
| Naming | idempotence, rename, reassignment, expiry cleanup |
| Identity | random, persistent, invalid file, isolated env |
| Payload demo | actual lengths and checksums for all patterns |
| Clean checkout | full Makefile gate without siblings |

## Final done criteria

- [ ] An operator-approved baseline SHA is recorded.
- [ ] All five design decisions are recorded and implemented consistently.
- [ ] Clean standalone `make verify` exits 0.
- [ ] The resolver worker and node do not survive the last Agent drop.
- [ ] Seeds and inbound ACLs are separate concepts in API, tests, and docs.
- [ ] Forged or replayed topic ownership records are rejected.
- [ ] Replicated and front-door modes converge according to documented rules.
- [ ] Dead owner records are withdrawn or expire.
- [ ] No production API derives a private key from a public name alone.
- [ ] NameTable remapping leaves no stale inverse entries.
- [ ] Every critical Agent path has a forced-QUIC integration test.
- [ ] Heavy examples measure actual transferred payload bytes.
- [ ] No workflow or Makefile target references nonexistent example `main`.
- [ ] Cargo dependency sources are standalone and internally consistent.
- [ ] README and rustdoc match actual behavior.
- [ ] No version metadata was changed without explicit approval.
- [ ] No commit contains a signature, trailer, body, or non-Conventional title.

## STOP conditions

Stop and report instead of improvising if any of these occurs:

1. The operator has not established an initial baseline commit.
2. A compatible immutable peerbus/authbox dependency reference does not exist.
3. Authenticated records require unreviewed custom cryptography.
4. Fixing request-source authentication requires modifying peerbus without
   explicit cross-repository approval.
5. The operator rejects one of Decisions B-D without providing replacement
   membership, ownership, conflict, or expiry semantics.
6. A lifecycle fix requires leaking a thread, detaching a join handle, or
   making Agent clones unsafely invalidate each other.
7. A clean checkout cannot reproduce the local validation environment.
8. A phase requires changing version/release metadata.
9. A verification command fails twice after a reasonable scoped fix.
10. Completing a phase would require modifying files outside its declared
    scope or rewriting datapod/peerbus transport internals.

## Maintenance notes

- Treat the signed directory record as a security-sensitive protocol. Any new
  field must be included in canonical signing bytes and covered by tamper tests.
- Adding a new exchange pattern requires updating record signatures, type
  validation, reconciliation tests, docs, and the example matrix together.
- Lease durations must remain configurable and testable without real-time
  sleeps.
- Any future async public API should reuse the same resolver state rather than
  starting a second control plane.
- Reviewers should scrutinize identity migration, Agent clone shutdown,
  directory conflict handling, and ACL examples more closely than formatting or
  API cosmetics.
