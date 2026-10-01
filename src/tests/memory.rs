use super::*;
use governor::nanos::Nanos;
use std::{
    ops::Add,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

static MEASUREMENTS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CountedInstant(u64);

impl Add<Nanos> for CountedInstant {
    type Output = Self;
    fn add(self, rhs: Nanos) -> Self {
        Self(self.0.saturating_add(rhs.as_u64()))
    }
}
impl Reference for CountedInstant {
    fn duration_since(&self, earlier: Self) -> Nanos {
        MEASUREMENTS.fetch_add(1, Ordering::SeqCst);
        self.0.saturating_sub(earlier.0).into()
    }
    fn saturating_sub(&self, duration: Nanos) -> Self {
        Self(self.0.saturating_sub(duration.as_u64()))
    }
}
#[derive(Clone, Default)]
struct CountedClock(Arc<AtomicU64>);
impl Clock for CountedClock {
    type Instant = CountedInstant;
    fn now(&self) -> Self::Instant {
        CountedInstant(self.0.load(Ordering::SeqCst))
    }
}
fn request(key: &[u8], nanos: u64) -> Request<'_> {
    Request {
        namespace: "scope",
        key,
        action: "join",
        limit: Limit::new(1, Duration::from_nanos(nanos)).unwrap(),
    }
}

#[test]
fn new_key_flood_scans_once_until_recovery_is_possible() {
    let clock = CountedClock::default();
    let store = MemoryStore::with_clock(NonZeroUsize::new(64).unwrap(), clock.clone());
    for key in 0_u64..64 {
        assert_eq!(
            store.check_now(request(&key.to_be_bytes(), 20)),
            Ok(Decision::Allowed)
        );
    }
    MEASUREMENTS.store(0, Ordering::SeqCst);
    for key in 64_u64..1064 {
        assert_eq!(
            store.check_now(request(&key.to_be_bytes(), 20)),
            Err(StoreError::CapacityExhausted)
        );
    }
    // One 64-bucket scan plus constant work per subsequent denial, not 64,000 visits.
    assert!(MEASUREMENTS.load(Ordering::SeqCst) <= 1100);
    clock.0.store(19, Ordering::SeqCst);
    assert_eq!(
        store.check_now(request(b"new", 20)),
        Err(StoreError::CapacityExhausted)
    );
    clock.0.store(20, Ordering::SeqCst);
    assert_eq!(store.check_now(request(b"new", 20)), Ok(Decision::Allowed));
}

#[test]
fn earlier_recovery_from_new_or_replaced_buckets_is_not_skipped() {
    let clock = crate::FakeRelativeClock::default();
    let store = MemoryStore::with_clock(NonZeroUsize::new(2).unwrap(), clock.clone());
    assert_eq!(store.check_now(request(b"a", 100)), Ok(Decision::Allowed));
    assert_eq!(store.check_now(request(b"b", 10)), Ok(Decision::Allowed));
    assert_eq!(store.prune(), Ok(0));
    clock.advance(Duration::from_nanos(10));
    // Replacing a recovered bucket may select a much shorter policy.
    assert_eq!(store.check_now(request(b"b", 1)), Ok(Decision::Allowed));
    assert_eq!(store.prune(), Ok(0));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(store.prune(), Ok(1));
    // Insertion must invalidate the remaining bucket's later checkpoint.
    assert_eq!(store.check_now(request(b"b", 1)), Ok(Decision::Allowed));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(store.check_now(request(b"c", 1)), Ok(Decision::Allowed));
    assert_eq!(
        store.check_now(request(b"a", 100)),
        Ok(Decision::Denied {
            retry_after: Duration::from_nanos(88)
        })
    );
}

#[test]
fn poisoned_state_refuses_admission_and_pruning() {
    let store = Arc::new(MemoryStore::new(NonZeroUsize::new(2).unwrap()));
    assert_eq!(
        store.check_now(request(b"opaque", 100)),
        Ok(Decision::Allowed)
    );
    let other = store.clone();
    let result = std::thread::spawn(move || {
        let _guard = other.state.lock().unwrap();
        panic!("synthetic state writer failure");
    })
    .join();
    assert!(result.is_err());
    assert_eq!(
        store.check_now(request(b"opaque", 100)),
        Err(StoreError::Unavailable)
    );
    assert_eq!(store.prune(), Err(StoreError::Unavailable));
}
