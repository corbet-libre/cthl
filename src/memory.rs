use std::{cell::Cell, collections::HashMap, fmt, num::NonZeroUsize, sync::Mutex, time::Duration};

use governor::{
    RateLimiter, clock::Reference, middleware::NoOpMiddleware, state::InMemoryState,
    state::NotKeyed,
};

use crate::{Clock, Decision, Limit, MonotonicClock, Request, Store, StoreError};

// A single clock reading drives each atomic operation, even if a test advances
// the external clock concurrently. Relative durations also avoid adding future
// deadlines to an external instant near its representable upper bound.
struct SnapshotClock(Cell<Duration>);

impl Clock for SnapshotClock {
    type Instant = Duration;

    fn now(&self) -> Duration {
        self.0.get()
    }
}

#[derive(Hash, PartialEq, Eq)]
struct BucketKey {
    namespace: String,
    key: Vec<u8>,
    action: String,
}

struct Bucket<T> {
    limit: Limit,
    created: T,
    last_allowed: T,
    limiter: RateLimiter<NotKeyed, InMemoryState, SnapshotClock, NoOpMiddleware<Duration>>,
}

impl<T: Reference> Bucket<T> {
    fn new(limit: Limit, now: T) -> Self {
        Self {
            limit,
            created: now,
            last_allowed: now,
            limiter: RateLimiter::direct_with_clock(
                limit.quota(),
                SnapshotClock(Cell::new(Duration::ZERO)),
            ),
        }
    }

    fn recovered(&self, now: T) -> bool {
        Duration::from(now.duration_since(self.last_allowed)) >= self.limit.full_refill()
    }

    fn check(&mut self, now: T) -> Result<Decision, StoreError> {
        let elapsed = now.duration_since(self.created).as_u64();
        let horizon = self.limit.full_refill().as_nanos();
        if u128::from(elapsed) + horizon > u128::from(u64::MAX) {
            return Err(StoreError::ClockOverflow);
        }
        let elapsed = Duration::from_nanos(elapsed);
        self.limiter.clock().0.set(elapsed);
        match self.limiter.check() {
            Ok(()) => {
                self.last_allowed = now;
                Ok(Decision::Allowed)
            }
            Err(denial) => Ok(Decision::Denied {
                retry_after: denial.wait_time_from(elapsed),
            }),
        }
    }
}

struct State<T> {
    buckets: HashMap<BucketKey, Bucket<T>>,
    last_observed: Option<T>,
}

impl<T: Reference> State<T> {
    fn observe(&mut self, now: T) -> Result<(), StoreError> {
        if self.last_observed.is_some_and(|previous| now < previous) {
            return Err(StoreError::ClockWentBackwards);
        }
        self.last_observed = Some(now);
        Ok(())
    }

    fn prune(&mut self, now: T) -> usize {
        let before = self.buckets.len();
        self.buckets.retain(|_, bucket| !bucket.recovered(now));
        before - self.buckets.len()
    }
}

/// Bounded, process-local storage using `governor` for all rate decisions.
///
/// A mutex makes admission, expiry, and insertion atomic across shared handles.
/// No guard is held across an await. Exhaustion refuses new keys instead of
/// evicting active limits. The store samples the injected clock once per check.
pub struct MemoryStore<C: Clock = MonotonicClock> {
    clock: C,
    capacity: NonZeroUsize,
    state: Mutex<State<C::Instant>>,
}

impl MemoryStore<MonotonicClock> {
    /// Create a store with a monotonic clock and an explicit bucket capacity.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self::with_clock(capacity, MonotonicClock)
    }
}

impl<C: Clock> MemoryStore<C> {
    /// Create a store using an injected clock, such as [`crate::FakeRelativeClock`].
    ///
    /// Custom clocks must satisfy `governor`'s `Clock`/`Reference` contract and
    /// represent elapsed intervals in `u64` nanoseconds. Regressions fail closed.
    pub fn with_clock(capacity: NonZeroUsize, clock: C) -> Self {
        Self {
            clock,
            capacity,
            state: Mutex::new(State {
                buckets: HashMap::new(),
                last_observed: None,
            }),
        }
    }

    /// Remove buckets idle for at least their entire refill duration.
    ///
    /// Call periodically to discard opaque keys even when there is no traffic.
    /// Insertion also prunes when capacity is reached. Denials do not prolong
    /// retention. No recovering bucket is removed, so pruning grants no tokens.
    pub fn prune(&self) -> Result<usize, StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Unavailable)?;
        let now = self.clock.now();
        state.observe(now)?;
        Ok(state.prune(now))
    }

    fn check_now(&self, request: Request<'_>) -> Result<Decision, StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Unavailable)?;
        let now = self.clock.now();
        state.observe(now)?;
        let key = BucketKey {
            namespace: request.namespace().to_owned(),
            key: request.key().to_vec(),
            action: request.action().to_owned(),
        };

        if let Some(bucket) = state.buckets.get_mut(&key) {
            if bucket.recovered(now) {
                *bucket = Bucket::new(request.limit(), now);
            } else if bucket.limit != request.limit() {
                return Err(StoreError::PolicyConflict);
            }
            return bucket.check(now);
        }

        if state.buckets.len() >= self.capacity.get() {
            state.prune(now);
            if state.buckets.len() >= self.capacity.get() {
                return Err(StoreError::CapacityExhausted);
            }
        }
        let mut bucket = Bucket::new(request.limit(), now);
        let decision = bucket.check(now)?;
        state.buckets.insert(key, bucket);
        Ok(decision)
    }
}

impl<C: Clock + Send + Sync> Store for MemoryStore<C> {
    async fn check(&self, request: Request<'_>) -> Result<Decision, StoreError> {
        self.check_now(request)
    }
}

impl<C: Clock> fmt::Debug for MemoryStore<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MemoryStore { inputs: [REDACTED] }")
    }
}
