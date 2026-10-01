//! Static diagnostics retain causes without exposing throttle inputs.

use cthl::{Error, StoreError};

#[test]
fn input_and_storage_diagnostics_are_safe_and_keep_typed_causes() {
    for error in [
        Error::InvalidLimit,
        Error::InvalidNamespace,
        Error::InvalidAction,
        Error::InvalidKey,
        Error::NoActions,
        Error::DuplicateAction,
        Error::UnknownAction,
    ] {
        assert!(!error.to_string().is_empty());
        assert!(std::error::Error::source(&error).is_none());
    }
    for cause in [
        StoreError::Unavailable,
        StoreError::CapacityExhausted,
        StoreError::PolicyConflict,
        StoreError::ClockWentBackwards,
        StoreError::ClockOverflow,
    ] {
        let error = Error::Store(cause);
        assert!(!cause.to_string().is_empty());
        assert_eq!(error.to_string(), cause.to_string());
        let source = std::error::Error::source(&error).unwrap();
        assert_eq!(source.downcast_ref::<StoreError>(), Some(&cause));
        assert!(std::error::Error::source(&cause).is_none());
    }
}
