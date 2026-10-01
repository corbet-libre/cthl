use super::*;

#[test]
fn request_debug_redacts_every_input() {
    let request = Request {
        namespace: "private-community",
        key: b"private-member",
        action: "private-action",
        limit: Limit::new(1, Duration::from_secs(1)).unwrap(),
    };
    let debug = format!("{request:?}");
    assert_eq!(debug, "Request { inputs: [REDACTED] }");
}
