# cthl

**Throttle: rate limits with pluggable storage.**

Part of `cvld`, the permanent door of the cmtymeet trust stack, and usable by
`cfrm` outside both doors. Development API; not published to a registry.
The implemented contract is in [docs/CONTRACT.md](docs/CONTRACT.md).

## Usage

```rust
use cthl::{Decision, Limit, MemoryStore, Throttle};
use std::{num::NonZeroUsize, time::Duration};

async fn example() -> Result<(), cthl::Error> {
    let store = MemoryStore::new(NonZeroUsize::new(10_000).unwrap());
    let throttle = Throttle::new(
        "community-a",
        [("register", Limit::new(3, Duration::from_secs(20))?)],
        store,
    )?;
    match throttle.check(b"opaque-key", "register").await? {
        Decision::Allowed => { /* perform the action */ }
        Decision::Denied { retry_after } => { /* report the retry delay */ }
    }
    Ok(())
}
```

The example allows an initial burst of three, then replenishes **one token every
20 seconds**. These are example values, not product policy defaults. Denied
checks do not consume tokens or prolong the retry delay. An allowed attempt
spends a token even if the downstream action fails. Retry delays are advisory;
another caller may consume the next token first.

A bucket is identified by the tuple `(namespace, opaque key, action)`. Use a
trusted community/global namespace and opaque bytes chosen by the service.
Namespace/action names are case-sensitive; each field must contain 1–256 bytes.
Unknown actions, invalid inputs, capacity exhaustion, clock errors and backend
failures return errors and must not admit the action. Configuration errors and
Debug output contain no input values. The crate does not log anything.

## Storage and clocks

`Store::check(Request)` returns a `Send` future with a fallible rate decision.
This keeps the call site ready for a future `cvtl`/Valkey adapter with atomic
server-side admission and shared server time. `MemoryStore` is the only current
backend and uses governor's algorithm without reimplementing token arithmetic.

Share an `Arc<MemoryStore<_>>` across throttles that should share counters. One
store has a caller-chosen maximum number of buckets. It refuses new keys when
all slots are still recovering, and reclaims only fully recovered buckets.
Call `store.prune()` periodically to discard idle keys even without traffic.
Pruning scans at most the configured capacity. A different policy for a still
recovering bucket fails closed; coordinate policy rollout between callers.

`MemoryStore::with_clock(capacity, FakeRelativeClock::default())` supports tests
without sleeps. Clones of the fake clock share time; advance it explicitly.
The production default is `MonotonicClock`. No Tokio runtime is required by the
library. Each admission uses one sampled instant under the store lock.

Memory quotas reset on restart and do not coordinate separate processes. There
is no relational throttle storage. Application persistence belongs to `crlt`;
`libsql` is used here only for integration test fixtures.

## Dependency survey

Surveyed crates.io/docs.rs and upstream GitHub before implementation,
2026-09-30:

| Candidate | Findings and decision |
| --- | --- |
| [governor 0.10.4](https://crates.io/crates/governor), [source](https://github.com/boinkor-net/governor) | **Selected.** Maintained MIT crate; GCRA with token-bucket behavior, precise retry delays, clock injection, established direct/keyed limiters. Enable only `std`; disable optional jitter, quanta and dashmap. |
| [ratelimit 2.0.0](https://crates.io/crates/ratelimit), [source](https://github.com/iopsystems/ratelimit) | Maintained MIT/Apache-2.0 token bucket with clock injection. Suitable alternative; governor already supplies the quota and retry behavior needed here. Neither provides an async network storage failure channel directly. |
| [tower_governor](https://crates.io/crates/tower_governor), [source](https://github.com/benwis/tower-governor) | MIT HTTP/Tower middleware over governor. Appropriate at a service boundary, but adds HTTP extraction and middleware concerns to this transport-independent leaf. Use governor directly. |

Governor's low-level `StateStore` is synchronous and its generic error is the
algorithm's decision; it cannot independently report a backend outage. `cthl`
therefore wraps the **complete atomic check** in its own fallible async trait,
while the memory backend delegates all rate calculations to governor. The
wrapper handles only policy lookup, namespacing, bounded retention and errors.

Test-only dependencies are the official
[libsql](https://github.com/tursodatabase/libsql) driver (MIT, local core plus
remote/TLS), Tokio (MIT, test runtime/macros/synchronization), and tempfile
(MIT/Apache-2.0, disposable local DB files). No custom crypto, GPL-only or
AGPL-only dependency is added.

## Tests and CI

All Cargo commands run on GitHub Actions, using stable Rust:

```text
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`Cargo.lock` is the dependency graph resolved by CI; checks use `--locked`.
Each CI run retains its lockfile and dependency/license metadata as an artifact.

Tests exercise the real memory backend, governor and injected clocks, including
concurrency, failure and boundary cases. The libSQL fixtures test throttled
writes, namespace isolation, indexed query plans, rollback and file reopening.
They do not persist throttle keys or requests.

`tests/libsql.rs` includes `turso_round_trip_if_configured`, which returns early
unless both `TURSO_URL` and `TURSO_TOKEN` are present. It creates a uniquely named
fixture table, runs the round trip, then drops that table. Use a disposable test
database. Connection loss or cancellation can prevent fixture cleanup; any
leftover table has the `cthl_fixture_` prefix. Pass credentials through the
environment only; never commit them or
configure them in public CI. Configured connection failures fail the test with
redacted diagnostics. A skipped remote test is not evidence of Turso validation.

The remaining adapter work is `cvtl`/Valkey, including distributed atomicity,
timeouts, cancellation and multi-process tests. The current memory store uses
one mutex and linear pruning; partition stores by service/community if needed.

## License

Copyright 2026 Julian Y. Richard Corbet. Licensed under [LGPL-3.0-only](LICENSES/LGPL-3.0-only.txt) [WITH LGPL-3.0-linking-exception](LICENSES/LGPL-3.0-linking-exception.txt). See [LICENSE.md](LICENSE.md).
