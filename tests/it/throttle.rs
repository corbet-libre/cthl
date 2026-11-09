//! Admission, isolation, concurrency, failure, and privacy tests with real stores.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use cthl::{
    Clock, Decision, Error, FakeRelativeClock, Limit, MAX_INPUT_BYTES, MemoryStore, StoreError,
    Throttle,
};

fn capacity(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn limit(burst: u32, nanos: u64) -> Limit {
    Limit::new(burst, Duration::from_nanos(nanos)).unwrap()
}

fn denied(nanos: u64) -> Result<Decision, Error> {
    Ok(Decision::Denied {
        retry_after: Duration::from_nanos(nanos),
    })
}

#[tokio::test]
async fn burst_and_exact_refill_boundaries() {
    let clock = FakeRelativeClock::default();
    let store = MemoryStore::with_clock(capacity(10), clock.clone());
    let throttle = Throttle::new("a", [("join", limit(3, 10))], store).unwrap();
    for _ in 0..3 {
        assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    }
    for _ in 0..100 {
        assert_eq!(throttle.check(b"key", "join").await, denied(10));
    }
    clock.advance(Duration::from_nanos(9));
    assert_eq!(throttle.check(b"key", "join").await, denied(1));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(throttle.check(b"key", "join").await, denied(10));
    clock.advance(Duration::from_nanos(25));
    for _ in 0..2 {
        assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    }
    assert_eq!(throttle.check(b"key", "join").await, denied(5));
}

#[tokio::test]
async fn idle_recovery_is_capped_at_the_burst() {
    let clock = FakeRelativeClock::default();
    clock.advance(Duration::from_secs(100));
    let store = MemoryStore::with_clock(capacity(1), clock.clone());
    let throttle = Throttle::new("a", [("join", limit(2, 1))], store).unwrap();
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    clock.advance(Duration::from_secs(1_000_000));
    for _ in 0..2 {
        assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    }
    assert_eq!(throttle.check(b"key", "join").await, denied(1));
}

#[tokio::test]
async fn keys_actions_and_namespaces_have_independent_quotas() {
    let store = Arc::new(MemoryStore::with_clock(
        capacity(10),
        FakeRelativeClock::default(),
    ));
    let policies = [("join", limit(1, 10)), ("lookup", limit(2, 20))];
    let a = Throttle::new("a", policies, store.clone()).unwrap();
    let a_shared = Throttle::new("a", policies, store.clone()).unwrap();
    let b = Throttle::new("b", policies, store).unwrap();
    assert_eq!(a.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(a_shared.check(b"key", "join").await, denied(10));
    assert_eq!(b.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(a.check(b"other", "join").await, Ok(Decision::Allowed));
    for _ in 0..2 {
        assert_eq!(a.check(b"key", "lookup").await, Ok(Decision::Allowed));
    }
    assert_eq!(a.check(b"key", "lookup").await, denied(20));
}

#[tokio::test]
async fn tuple_encoding_does_not_confuse_delimiters_or_binary_keys() {
    let store = Arc::new(MemoryStore::with_clock(
        capacity(20),
        FakeRelativeClock::default(),
    ));
    let a = Throttle::new(
        "a",
        [("b:c", limit(1, 1)), ("c", limit(1, 1))],
        store.clone(),
    )
    .unwrap();
    let b = Throttle::new("a:b", [("c", limit(1, 1))], store).unwrap();
    for (throttle, key, action) in [
        (&a, b"x".as_slice(), "b:c"),
        (&b, b"x".as_slice(), "c"),
        (&a, b"x:b".as_slice(), "c"),
        (&a, b"\0\xff".as_slice(), "c"),
        (&a, b"\xff\0".as_slice(), "c"),
    ] {
        assert_eq!(throttle.check(key, action).await, Ok(Decision::Allowed));
        assert_eq!(throttle.check(key, action).await, denied(1));
    }
}

#[tokio::test]
async fn policies_change_only_after_recovery_without_extra_quota() {
    let clock = FakeRelativeClock::default();
    let store = Arc::new(MemoryStore::with_clock(capacity(1), clock.clone()));
    let old = Throttle::new("a", [("join", limit(2, 10))], store.clone()).unwrap();
    let new = Throttle::new("a", [("join", limit(4, 1))], store).unwrap();
    assert_eq!(old.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(
        new.check(b"key", "join").await,
        Err(Error::Store(StoreError::PolicyConflict))
    );
    assert_eq!(old.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(old.check(b"key", "join").await, denied(10));
    clock.advance(Duration::from_nanos(19));
    assert_eq!(
        new.check(b"key", "join").await,
        Err(Error::Store(StoreError::PolicyConflict))
    );
    clock.advance(Duration::from_nanos(1));
    for _ in 0..4 {
        assert_eq!(new.check(b"key", "join").await, Ok(Decision::Allowed));
    }
    assert_eq!(new.check(b"key", "join").await, denied(1));
}

#[tokio::test]
async fn full_store_refuses_new_keys_without_resetting_existing_limits() {
    let clock = FakeRelativeClock::default();
    let store = Arc::new(MemoryStore::with_clock(capacity(1), clock.clone()));
    let throttle = Throttle::new("a", [("join", limit(2, 10))], store.clone()).unwrap();
    for _ in 0..2 {
        assert_eq!(
            throttle.check(b"first", "join").await,
            Ok(Decision::Allowed)
        );
    }
    clock.advance(Duration::from_nanos(19));
    assert_eq!(store.prune(), Ok(0));
    assert_eq!(
        throttle.check(b"second", "join").await,
        Err(Error::Store(StoreError::CapacityExhausted))
    );
    clock.advance(Duration::from_nanos(1));
    // Insertion prunes automatically, even without an explicit prune call.
    assert_eq!(
        throttle.check(b"second", "join").await,
        Ok(Decision::Allowed)
    );
    assert_eq!(
        throttle.check(b"first", "join").await,
        Err(Error::Store(StoreError::CapacityExhausted))
    );
}

#[tokio::test]
async fn pruning_neither_grants_tokens_nor_extends_retention_on_denial() {
    let clock = FakeRelativeClock::default();
    let store = Arc::new(MemoryStore::with_clock(capacity(2), clock.clone()));
    let throttle = Throttle::new("a", [("join", limit(1, 10))], store.clone()).unwrap();
    assert_eq!(store.prune(), Ok(0));
    assert_eq!(
        throttle.check(b"first", "join").await,
        Ok(Decision::Allowed)
    );
    clock.advance(Duration::from_nanos(9));
    assert_eq!(throttle.check(b"first", "join").await, denied(1));
    assert_eq!(
        throttle.check(b"second", "join").await,
        Ok(Decision::Allowed)
    );
    assert_eq!(store.prune(), Ok(0));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(store.prune(), Ok(1));
    assert_eq!(store.prune(), Ok(0));
    assert_eq!(
        throttle.check(b"first", "join").await,
        Ok(Decision::Allowed)
    );
    assert_eq!(throttle.check(b"second", "join").await, denied(9));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_checks_across_shared_handles_admit_exactly_the_burst() {
    let store = Arc::new(MemoryStore::with_clock(
        capacity(1),
        FakeRelativeClock::default(),
    ));
    let barrier = Arc::new(tokio::sync::Barrier::new(128));
    let mut tasks = Vec::new();
    for _ in 0..128 {
        let throttle = Throttle::new("a", [("join", limit(17, 10))], store.clone()).unwrap();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            throttle.check(b"key", "join").await.unwrap()
        }));
    }
    let mut allowed = 0;
    for task in tasks {
        match task.await.unwrap() {
            Decision::Allowed => allowed += 1,
            Decision::Denied { retry_after } => {
                assert_eq!(retry_after, Duration::from_nanos(10));
            }
        }
    }
    assert_eq!(allowed, 17);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_insertion_cannot_exceed_store_capacity() {
    let store = Arc::new(MemoryStore::with_clock(
        capacity(3),
        FakeRelativeClock::default(),
    ));
    let throttle = Arc::new(Throttle::new("a", [("join", limit(1, 10))], store).unwrap());
    let mut tasks = Vec::new();
    for key in 0..64_u8 {
        let throttle = throttle.clone();
        tasks.push(tokio::spawn(
            async move { throttle.check(&[key], "join").await },
        ));
    }
    let mut allowed = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(Decision::Allowed) => allowed += 1,
            Err(Error::Store(StoreError::CapacityExhausted)) => {}
            other => panic!("unexpected result: {other:?}"),
        }
    }
    assert_eq!(allowed, 3);
}

#[test]
fn invalid_limits_are_rejected_without_overflow() {
    for (burst, period) in [
        (0, Duration::from_secs(1)),
        (1, Duration::ZERO),
        (1, Duration::MAX),
        (u32::MAX, Duration::MAX),
        (2, Duration::from_nanos(u64::MAX)),
    ] {
        assert_eq!(Limit::new(burst, period), Err(Error::InvalidLimit));
    }
    let smallest = limit(u32::MAX, 1);
    assert_eq!(smallest.burst(), u32::MAX);
    assert_eq!(smallest.replenish_interval(), Duration::from_nanos(1));
    assert!(Limit::new(1, Duration::from_nanos(u64::MAX)).is_ok());
}

#[test]
fn invalid_configuration_is_rejected_and_redacted() {
    let store = || MemoryStore::new(capacity(1));
    for namespace in [String::new(), "s".repeat(MAX_INPUT_BYTES + 1)] {
        assert_eq!(
            Throttle::new(namespace, [("join", limit(1, 1))], store()).unwrap_err(),
            Error::InvalidNamespace
        );
    }
    for action in [String::new(), "s".repeat(MAX_INPUT_BYTES + 1)] {
        assert_eq!(
            Throttle::new("a", [(action, limit(1, 1))], store()).unwrap_err(),
            Error::InvalidAction
        );
    }
    assert_eq!(
        Throttle::new("a", [("join", limit(1, 1)), ("join", limit(2, 2))], store()).unwrap_err(),
        Error::DuplicateAction
    );
    assert_eq!(
        Throttle::new("a", Vec::<(&str, Limit)>::new(), store()).unwrap_err(),
        Error::NoActions
    );
}

#[tokio::test]
async fn invalid_requests_do_not_allocate_or_consume_quota() {
    let store = MemoryStore::with_clock(capacity(1), FakeRelativeClock::default());
    let throttle = Throttle::new("a", [("join", limit(1, 1))], store).unwrap();
    for key in [Vec::new(), vec![0; MAX_INPUT_BYTES + 1]] {
        assert_eq!(throttle.check(&key, "join").await, Err(Error::InvalidKey));
    }
    assert_eq!(throttle.check(b"key", "").await, Err(Error::InvalidAction));
    assert_eq!(
        throttle
            .check(b"key", &"s".repeat(MAX_INPUT_BYTES + 1))
            .await,
        Err(Error::InvalidAction)
    );
    for unknown in ["Join", "missing"] {
        assert_eq!(
            throttle.check(b"key", unknown).await,
            Err(Error::UnknownAction)
        );
    }
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(throttle.check(b"key", "join").await, denied(1));
}

#[tokio::test]
async fn maximum_length_and_unicode_inputs_are_exact() {
    let namespace = "é".repeat(MAX_INPUT_BYTES / 2);
    let action = "z".repeat(MAX_INPUT_BYTES);
    let key = vec![0xff; MAX_INPUT_BYTES];
    let store = MemoryStore::with_clock(capacity(1), FakeRelativeClock::default());
    let throttle = Throttle::new(namespace, [(&action, limit(1, 1))], store).unwrap();
    assert_eq!(throttle.check(&key, &action).await, Ok(Decision::Allowed));
    assert_eq!(throttle.check(&key, &action).await, denied(1));
}

#[tokio::test]
async fn a_new_memory_store_has_fresh_quota() {
    for _ in 0..2 {
        let store = MemoryStore::with_clock(capacity(1), FakeRelativeClock::default());
        let throttle = Throttle::new("a", [("join", limit(1, 1))], store).unwrap();
        assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
        assert_eq!(throttle.check(b"key", "join").await, denied(1));
    }
}

#[tokio::test]
async fn default_monotonic_clock_can_admit_requests() {
    let store = MemoryStore::new(capacity(1));
    let throttle = Throttle::new("a", [("join", limit(1, 86_400_000_000_000))], store).unwrap();
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    assert!(matches!(
        throttle.check(b"key", "join").await,
        Ok(Decision::Denied { retry_after }) if retry_after > Duration::ZERO
    ));
}

#[derive(Clone, Default)]
struct AdjustableClock(Arc<AtomicU64>);

impl Clock for AdjustableClock {
    type Instant = Duration;

    fn now(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

#[tokio::test]
async fn regressing_clock_fails_closed_and_preserves_debt() {
    let clock = AdjustableClock::default();
    clock.0.store(100, Ordering::SeqCst);
    let store = Arc::new(MemoryStore::with_clock(capacity(1), clock.clone()));
    let throttle = Throttle::new("a", [("join", limit(1, 10))], store.clone()).unwrap();
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    clock.0.store(99, Ordering::SeqCst);
    assert_eq!(
        throttle.check(b"key", "join").await,
        Err(Error::Store(StoreError::ClockWentBackwards))
    );
    assert_eq!(store.prune(), Err(StoreError::ClockWentBackwards));
    clock.0.store(100, Ordering::SeqCst);
    assert_eq!(throttle.check(b"key", "join").await, denied(10));
    clock.0.store(110, Ordering::SeqCst);
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
}

#[tokio::test]
async fn huge_intervals_and_near_maximum_clock_values_do_not_panic() {
    let clock = FakeRelativeClock::default();
    clock.advance(Duration::from_nanos(u64::MAX - 10));
    let store = MemoryStore::with_clock(capacity(1), clock.clone());
    let throttle = Throttle::new("a", [("join", limit(1, u64::MAX))], store).unwrap();
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    assert_eq!(throttle.check(b"key", "join").await, denied(u64::MAX));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(
        throttle.check(b"key", "join").await,
        Err(Error::Store(StoreError::ClockOverflow))
    );
}

#[tokio::test]
async fn deterministic_schedules_respect_the_token_budget() {
    // Integer schedules give an independent token-budget oracle. The real
    // backend and governor are exercised; no rate/storage logic is mocked.
    for burst in [1, 2, 7] {
        for interval in [1, 3, 100] {
            let clock = FakeRelativeClock::default();
            let store = MemoryStore::with_clock(capacity(1), clock.clone());
            let throttle = Throttle::new("a", [("join", limit(burst, interval))], store).unwrap();
            let max_credit = u64::from(burst) * interval;
            let mut credit = max_credit;
            let mut seed = 12345_u64;
            for _ in 0..500 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let step = (seed >> 32) % (interval * 2 + 1);
                clock.advance(Duration::from_nanos(step));
                credit = (credit + step).min(max_credit);
                let expected = if credit >= interval {
                    credit -= interval;
                    Ok(Decision::Allowed)
                } else {
                    denied(interval - credit)
                };
                assert_eq!(throttle.check(b"key", "join").await, expected);
            }
        }
    }
}

#[tokio::test]
async fn debug_and_error_surfaces_redact_input_values() {
    let secret = "sensitive-member-value";
    let store = Arc::new(MemoryStore::with_clock(
        capacity(1),
        FakeRelativeClock::default(),
    ));
    let throttle = Throttle::new(secret, [(secret, limit(1, 1))], store.clone()).unwrap();
    assert_eq!(
        throttle.check(secret.as_bytes(), secret).await,
        Ok(Decision::Allowed)
    );
    for output in [format!("{store:?}"), format!("{throttle:?}")] {
        assert!(output.contains("REDACTED"));
        assert!(!output.contains(secret));
    }
    let error = throttle
        .check(secret.as_bytes(), "missing")
        .await
        .unwrap_err();
    assert!(!format!("{error} {error:?}").contains(secret));
}

#[derive(Clone, Default)]
struct FailingClock(Arc<std::sync::atomic::AtomicBool>);

impl Clock for FailingClock {
    type Instant = Duration;

    fn now(&self) -> Duration {
        assert!(!self.0.load(Ordering::SeqCst), "injected clock failure");
        Duration::ZERO
    }
}

#[tokio::test]
async fn poisoned_backend_returns_unavailable_without_admission() {
    let clock = FailingClock::default();
    let store = Arc::new(MemoryStore::with_clock(capacity(1), clock.clone()));
    let throttle = Arc::new(Throttle::new("a", [("join", limit(1, 1))], store.clone()).unwrap());
    assert_eq!(throttle.check(b"key", "join").await, Ok(Decision::Allowed));
    clock.0.store(true, Ordering::SeqCst);
    let task_throttle = throttle.clone();
    let task = tokio::spawn(async move { task_throttle.check(b"key", "join").await });
    assert!(task.await.unwrap_err().is_panic());
    clock.0.store(false, Ordering::SeqCst);
    assert_eq!(
        throttle.check(b"key", "join").await,
        Err(Error::Store(StoreError::Unavailable))
    );
    assert_eq!(store.prune(), Err(StoreError::Unavailable));
}
