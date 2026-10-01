//! Rate limits for opaque keys and named actions, backed by pluggable storage.
//!
//! The in-memory backend delegates rate decisions to `governor`'s GCRA, with
//! token-bucket semantics. See the README and `docs/CONTRACT.md` for the storage
//! and privacy contracts. No logging, persistence, sleeping, or tasks are added.
//!
//! ```
//! use cthl::{Decision, Limit, MemoryStore, Throttle};
//! use std::{num::NonZeroUsize, time::Duration};
//!
//! # async fn example() -> Result<(), cthl::Error> {
//! let store = MemoryStore::new(NonZeroUsize::new(10_000).unwrap());
//! let throttle = Throttle::new(
//!     "community-a",
//!     [("register", Limit::new(3, Duration::from_secs(20))?)],
//!     store,
//! )?;
//! assert_eq!(throttle.check(b"opaque-key", "register").await?, Decision::Allowed);
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

mod memory;

use std::{collections::HashMap, fmt, future::Future, num::NonZeroU32, sync::Arc, time::Duration};

/// Clock interfaces and upstream deterministic and monotonic implementations.
pub use governor::clock::{Clock, FakeRelativeClock, MonotonicClock};
pub use memory::MemoryStore;

/// Maximum length in bytes of each namespace, action name, and opaque key.
pub const MAX_INPUT_BYTES: usize = 256;

/// A burst capacity and the time needed to replenish one token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    burst: NonZeroU32,
    replenish_interval: Duration,
}

impl Limit {
    /// Create a quota, initially full, which spends one token per allowed check.
    ///
    /// Rejects zero capacity, zero interval, and a full refill duration that
    /// exceeds `u64::MAX` nanoseconds (the upstream algorithm's time range).
    pub fn new(burst: u32, replenish_interval: Duration) -> Result<Self, Error> {
        let burst = NonZeroU32::new(burst).ok_or(Error::InvalidLimit)?;
        if replenish_interval.is_zero()
            || replenish_interval.as_nanos() * u128::from(burst.get()) > u128::from(u64::MAX)
        {
            return Err(Error::InvalidLimit);
        }
        Ok(Self {
            burst,
            replenish_interval,
        })
    }

    /// Maximum number of requests admitted at once by a full bucket.
    pub fn burst(self) -> u32 {
        self.burst.get()
    }

    /// Time needed to replenish one token, independent of burst size.
    pub fn replenish_interval(self) -> Duration {
        self.replenish_interval
    }

    pub(crate) fn full_refill(self) -> Duration {
        self.replenish_interval * self.burst.get()
    }

    pub(crate) fn quota(self) -> governor::Quota {
        governor::Quota::with_period(self.replenish_interval)
            .expect("validated nonzero interval")
            .allow_burst(self.burst)
    }
}

/// A rate decision. An allowed check consumes one token immediately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum Decision {
    /// The action may proceed.
    Allowed,
    /// The bucket is empty; the check did not consume or reserve a token.
    Denied {
        /// Earliest retry delay, assuming no competing requests consume tokens.
        retry_after: Duration,
    },
}

/// Input or storage failure. Messages never include caller-supplied values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Capacity or interval is zero, or the full refill duration is too large.
    InvalidLimit,
    /// Namespace is empty or exceeds [`MAX_INPUT_BYTES`].
    InvalidNamespace,
    /// Action is empty or exceeds [`MAX_INPUT_BYTES`].
    InvalidAction,
    /// Key is empty or exceeds [`MAX_INPUT_BYTES`].
    InvalidKey,
    /// No action policies were configured.
    NoActions,
    /// An action was configured more than once.
    DuplicateAction,
    /// No policy exists for this action; no request was admitted.
    UnknownAction,
    /// The backend could not safely reach a rate decision.
    Store(StoreError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidLimit => "invalid rate limit",
            Self::InvalidNamespace => "invalid throttle namespace",
            Self::InvalidAction => "invalid throttle action",
            Self::InvalidKey => "invalid throttle key",
            Self::NoActions => "no throttle actions configured",
            Self::DuplicateAction => "duplicate throttle action",
            Self::UnknownAction => "unknown throttle action",
            Self::Store(error) => return error.fmt(f),
        };
        f.write_str(message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

/// Backend failures, deliberately without arbitrary diagnostic payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The backend is unavailable or its state cannot be trusted.
    Unavailable,
    /// All storage slots contain buckets that are still recovering.
    CapacityExhausted,
    /// An existing bucket has a different policy and has not fully recovered.
    PolicyConflict,
    /// An injected clock regressed. State is kept unchanged.
    ClockWentBackwards,
    /// An active bucket exceeded the upstream algorithm's time range.
    ClockOverflow,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "throttle storage unavailable",
            Self::CapacityExhausted => "throttle storage capacity exhausted",
            Self::PolicyConflict => "conflicting throttle policy",
            Self::ClockWentBackwards => "throttle clock went backwards",
            Self::ClockOverflow => "throttle clock range exceeded",
        })
    }
}

impl std::error::Error for StoreError {}

/// A validated check passed to a backend. Debug output is redacted.
#[derive(Clone, Copy)]
pub struct Request<'a> {
    namespace: &'a str,
    key: &'a [u8],
    action: &'a str,
    limit: Limit,
}

impl<'a> Request<'a> {
    /// Community or global scope supplied by the trusted service.
    pub fn namespace(self) -> &'a str {
        self.namespace
    }

    /// Opaque caller-selected bytes. Do not log or interpret them.
    pub fn key(self) -> &'a [u8] {
        self.key
    }

    /// Exact, case-sensitive action name.
    pub fn action(self) -> &'a str {
        self.action
    }

    /// Policy to apply to this bucket.
    pub fn limit(self) -> Limit {
        self.limit
    }
}

impl fmt::Debug for Request<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Request { inputs: [REDACTED] }")
    }
}

/// Atomic, fallible rate-limit storage, suitable for local or network backends.
///
/// Implementations must isolate `(namespace, key, action)`, atomically check and
/// consume one token, and leave quota unchanged on denial. They must not log
/// inputs, evict recovering buckets, or return `Allowed` after storage failure.
/// Conflicting policies must fail closed until the previous bucket is full.
/// Network backends should use a server clock shared by their clients and must
/// handle cancellation/ambiguous writes conservatively: tokens are not refunded.
/// No process-local timestamps may be persisted as distributed state.
pub trait Store: Send + Sync {
    /// Check and consume a single token without sleeping for replenishment.
    fn check(
        &self,
        request: Request<'_>,
    ) -> impl Future<Output = Result<Decision, StoreError>> + Send;
}

impl<S: Store> Store for Arc<S> {
    fn check(
        &self,
        request: Request<'_>,
    ) -> impl Future<Output = Result<Decision, StoreError>> + Send {
        self.as_ref().check(request)
    }
}

/// Immutable action policies in one community or global namespace.
///
/// Reuse the same store to share limits between handles. Creating a new memory
/// store (including after a process restart) starts with full buckets.
pub struct Throttle<S> {
    namespace: String,
    actions: HashMap<String, Limit>,
    store: S,
}

impl<S: Store> Throttle<S> {
    /// Bind validated action policies and a backend to a trusted namespace.
    ///
    /// Namespace and action names are case-sensitive, nonempty, and limited to
    /// [`MAX_INPUT_BYTES`] bytes. Duplicate actions are rejected.
    pub fn new<I, A>(namespace: impl Into<String>, actions: I, store: S) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (A, Limit)>,
        A: Into<String>,
    {
        let namespace = namespace.into();
        validate(namespace.as_bytes(), Error::InvalidNamespace)?;
        let mut policies = HashMap::new();
        for (action, limit) in actions {
            let action = action.into();
            validate(action.as_bytes(), Error::InvalidAction)?;
            if policies.insert(action, limit).is_some() {
                return Err(Error::DuplicateAction);
            }
        }
        if policies.is_empty() {
            return Err(Error::NoActions);
        }
        Ok(Self {
            namespace,
            actions: policies,
            store,
        })
    }

    /// Check and consume one token for the key and action.
    ///
    /// Errors fail closed. Unknown actions do not reach storage. The delay on a
    /// denial is advisory, not a reservation. Downstream failure does not refund
    /// an already admitted attempt.
    pub async fn check(&self, key: &[u8], action: &str) -> Result<Decision, Error> {
        validate(key, Error::InvalidKey)?;
        validate(action.as_bytes(), Error::InvalidAction)?;
        let limit = *self.actions.get(action).ok_or(Error::UnknownAction)?;
        self.store
            .check(Request {
                namespace: &self.namespace,
                key,
                action,
                limit,
            })
            .await
            .map_err(Error::Store)
    }
}

impl<S> fmt::Debug for Throttle<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Throttle { inputs: [REDACTED] }")
    }
}

fn validate(value: &[u8], error: Error) -> Result<(), Error> {
    if value.is_empty() || value.len() > MAX_INPUT_BYTES {
        Err(error)
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/debug.rs"]
mod tests;
