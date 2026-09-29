# cthl contract

Copied from the cvld library contracts dated 2026-09-30.

### cthl: throttle (LGPL, corbet-foss)
| | |
|---|---|
| Scope | Rate limits per key and action (token bucket / sliding window) with pluggable storage (in-memory, `cvtl`) |
| Interface | `check(key, action) → allowed / retry_after` |
| Accept | Deterministic under test clock; no member IDs in logs |

## Implementation decisions

- `Throttle::new(namespace, actions, store)` binds immutable named policies to
  a trusted community or global namespace. `check(key, action).await` returns
  `Result<Decision, Error>`: `Allowed`, `Denied { retry_after: Duration }`, or
  an error that the caller must treat as refusal.
- Keys are opaque bytes. Namespace and action are exact, case-sensitive UTF-8
  strings. All three are nonempty and at most 256 bytes. They are separate tuple
  fields, never concatenated; delimiters and binary bytes cannot collide.
  Namespaces come from the trusted service, not from unauthenticated clients.
- `Limit::new(burst, replenish_interval)` delegates token-bucket behavior to
  governor's GCRA. Buckets start full, one allowed check spends one token, and
  the interval replenishes ONE token. There is no fixed/sliding window, batch
  operation, sleeping, reservation, refund, or cryptographic operation.
- The full refill duration must fit in `u64` nanoseconds. Time arithmetic is
  checked before asking governor to advance active state; out-of-range active
  buckets fail closed. Fully replenished buckets can be recreated safely.
- `Store` is an async, `Send + Sync`, fallible admission interface. An operation
  atomically checks and consumes quota for `(namespace, key, action)`.
  `Arc<S>` where `S: Store` shares counters across throttle handles. A future
  `cvtl`/Valkey backend implements this same trait, uses shared server time and
  atomic operations, and returns storage errors without admitting requests.
  Network cancellation or ambiguous writes may consume a token, never refund it.
- `MemoryStore` is the only shipped backend. It wraps governor with a bounded
  hash map and a mutex. The mutex covers lookup, admission, expiry and insertion;
  no lock spans an await. New keys fail closed when capacity is exhausted.
- The default clock is monotonic. An injected governor `Clock`, including
  `FakeRelativeClock`, makes checks deterministic. Each operation samples once
  inside the lock; all rate calculations use that snapshot. Clock regression
  fails closed, and the previous high-water mark is retained. Custom clocks must
  satisfy governor's `Clock`/`Reference` contract (elapsed intervals fit in `u64`
  nanoseconds). Advancing a fake clock beyond that range is caller error.
- Reclamation is conservative: a bucket is removable after a full refill
  duration since its last allowed check. Denials do not extend retention.
  `prune()` removes only recovered buckets; new-key insertion invokes it at
  capacity. A service should call it periodically to release idle opaque keys.
  No background worker is started by the library.
- Conflicting policies on a recovering bucket return `PolicyConflict`. A new
  policy may take effect after the old bucket has fully replenished. Services
  must coordinate policy rollout; competing old/new policy writers are invalid.
- Restarting or replacing a memory store restores full quotas. Limits are
  process-local; multiple service processes need the future shared backend.
  Capacity is per store, so separate stores can isolate community resource use.
- The crate emits no logs and keeps no login dates, request histories, raw gate
  data or member identifiers as a distinct data field. Error messages carry no
  inputs, and `Debug` for requests, stores and throttles is redacted. Callers
  must supply appropriately opaque keys and must not log their values.

## Relational storage boundary

Production `cthl` has no tables, SQL, database connection or dependency on
`libsql`/`crlt`. Its state is explicitly volatile. Relational application state
in cvld belongs behind `crlt`, which uses the official `libsql` crate, with one
DB per community, a community key in every table and indexed queries.

The integration tests use the official `libsql` driver as a **test fixture**
for guarded application writes, not as a throttle backend or a replacement for
`crlt`. They use a disposable community-keyed table with a composite primary
key, test query plans, isolation and transaction rollback, and prove that denied
attempts do not mutate application data. An optional real Turso round trip uses
only caller-provided `TURSO_URL` and `TURSO_TOKEN`; it skips without both. No
credentials are configured in the repository or public CI.

## Acceptance coverage

Tests cover deterministic boundaries down to one nanosecond, burst exhaustion,
continuous refill, idle recovery, independent tuple components, shared stores,
parallel contention, bounded capacity, safe pruning, input validation, unknown
actions, policy conflicts, clock regression/overflow, backend failure, privacy,
and guarded real libSQL operations. No mocks of rate or storage logic are used.

CI runs formatting, Clippy with warnings denied, and all tests on current stable
Rust. Workstations only edit and orchestrate CI; no registry publication.
