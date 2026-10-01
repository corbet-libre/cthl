//! Real application-storage fixtures; throttle state always stays in memory.
//! Driver errors are replaced by static labels so remote credentials and URLs
//! cannot appear in test failure diagnostics.

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use crlt::{Community, Config, Db, Migration, params};
use cthl::{Decision, FakeRelativeClock, Limit, MemoryStore, Store, Throttle};

type TestResult<T = ()> = Result<T, &'static str>;

fn require(condition: bool, message: &'static str) -> TestResult {
    if condition { Ok(()) } else { Err(message) }
}

const SCHEMA: &str = "CREATE TABLE cthl_fixture (
    community_id TEXT NOT NULL, resource TEXT NOT NULL,
    value INTEGER NOT NULL CHECK (value >= 0),
    PRIMARY KEY (community_id, resource)
) WITHOUT ROWID";

async fn prepare(db: &Db) -> TestResult {
    db.migrate(&[Migration::new(1, "throttle_fixture", SCHEMA)])
        .await
        .map_err(|_| "migrate fixture failed")?;
    Ok(())
}

async fn read_value(connection: &Community) -> TestResult<i64> {
    let rows = connection
        .query(
            "SELECT value FROM cthl_fixture WHERE resource = ?1",
            params!["example-setting"],
        )
        .await
        .map_err(|_| "indexed read failed")?;
    rows.first()
        .ok_or("expected fixture row missing")?
        .get_i64(0)
        .map_err(|_| "read value failed")
}

async fn guarded_write<S: Store>(
    throttle: &Throttle<S>,
    connection: &Community,
) -> TestResult<Decision> {
    let decision = throttle
        .check(b"opaque-test-key", "update-setting")
        .await
        .map_err(|_| "throttle check failed")?;
    if decision == Decision::Allowed {
        let mut tx = connection
            .tx()
            .await
            .map_err(|_| "guarded transaction failed")?;
        let exists = !tx
            .query(
                "SELECT value FROM cthl_fixture WHERE resource = ?1",
                params!["example-setting"],
            )
            .await
            .map_err(|_| "guarded read failed")?
            .is_empty();
        let sql = if exists {
            "UPDATE cthl_fixture SET value = value + 1 WHERE resource = ?1"
        } else {
            "INSERT INTO cthl_fixture (resource, value) VALUES (?1, 1)"
        };
        tx.execute(sql, params!["example-setting"])
            .await
            .map_err(|_| "guarded write failed")?;
        tx.commit().await.map_err(|_| "guarded commit failed")?;
    }
    Ok(decision)
}

async fn exercise(db: &Db) -> TestResult {
    let connection_a = db.community("community-a").map_err(|_| "scope failed")?;
    let connection_b = db.community("community-b").map_err(|_| "scope failed")?;
    let clock = FakeRelativeClock::default();
    let store = Arc::new(MemoryStore::with_clock(
        NonZeroUsize::new(8).unwrap(),
        clock.clone(),
    ));
    let policy = [(
        "update-setting",
        Limit::new(1, Duration::from_secs(1)).unwrap(),
    )];
    let a = Throttle::new("community-a", policy, store.clone()).unwrap();
    let b = Throttle::new("community-b", policy, store).unwrap();

    require(
        guarded_write(&a, &connection_a).await? == Decision::Allowed,
        "first write should pass",
    )?;
    require(
        guarded_write(&a, &connection_a).await?
            == Decision::Denied {
                retry_after: Duration::from_secs(1),
            },
        "repeated write should be denied",
    )?;
    require(
        guarded_write(&b, &connection_b).await? == Decision::Allowed,
        "second community should have separate quota",
    )?;
    require(
        read_value(&connection_a).await? == 1,
        "denied write changed data",
    )?;
    require(
        read_value(&connection_b).await? == 1,
        "community isolation failed",
    )?;

    clock.advance(Duration::from_secs(1));
    require(
        guarded_write(&a, &connection_a).await? == Decision::Allowed,
        "write should pass after replenishment",
    )?;
    require(
        read_value(&connection_a).await? == 2,
        "updated value missing",
    )?;
    require(
        read_value(&connection_b).await? == 1,
        "update leaked between communities",
    )?;

    let select = "SELECT value FROM cthl_fixture WHERE resource = ?1";
    let update = "UPDATE cthl_fixture SET value = value + 1 WHERE resource = ?1";
    let delete = "DELETE FROM cthl_fixture WHERE resource = ?1";
    // crlt rejects scans when explaining or executing these exact scoped queries.
    for sql in [select, update, delete] {
        connection_a
            .explain(sql, params!["example-setting"])
            .await
            .map_err(|_| "indexed query plan failed")?;
    }

    // A successful admission is not a promise that the application transaction
    // will commit. Real rollback must preserve data without refunding quota.
    clock.advance(Duration::from_secs(1));
    require(
        a.check(b"opaque-test-key", "update-setting")
            .await
            .map_err(|_| "check failed")?
            == Decision::Allowed,
        "transaction admission failed",
    )?;
    let mut transaction = connection_a
        .tx()
        .await
        .map_err(|_| "begin transaction failed")?;
    transaction
        .execute(update, params!["example-setting"])
        .await
        .map_err(|_| "transaction update failed")?;
    transaction
        .rollback()
        .await
        .map_err(|_| "rollback failed")?;
    require(
        read_value(&connection_a).await? == 2,
        "rollback changed data",
    )?;
    require(
        matches!(
            a.check(b"opaque-test-key", "update-setting").await,
            Ok(Decision::Denied { .. })
        ),
        "rollback incorrectly refunded quota",
    )?;

    clock.advance(Duration::from_secs(1));
    require(
        a.check(b"opaque-test-key", "update-setting")
            .await
            .map_err(|_| "check failed")?
            == Decision::Allowed,
        "constraint test admission failed",
    )?;
    require(
        connection_a
            .execute(
                "UPDATE cthl_fixture SET value = -1 WHERE resource = ?1",
                params!["example-setting"],
            )
            .await
            .is_err(),
        "database constraint failure was not surfaced",
    )?;
    require(
        read_value(&connection_a).await? == 2,
        "constraint failure changed data",
    )?;
    require(
        matches!(
            a.check(b"opaque-test-key", "update-setting").await,
            Ok(Decision::Denied { .. })
        ),
        "database failure incorrectly refunded quota",
    )?;
    Ok(())
}

#[tokio::test]
async fn temporary_libsql_round_trip() -> TestResult {
    let directory = tempfile::tempdir().map_err(|_| "create temp directory failed")?;
    let config = Config::new(
        format!("file://{}", directory.path().join("fixture.db").display()),
        "",
    );
    let db = Db::open(config).await.map_err(|_| "open DB failed")?;
    prepare(&db).await?;
    exercise(&db).await
}

#[tokio::test]
async fn file_database_retains_only_application_state_on_reopen() -> TestResult {
    let directory = tempfile::tempdir().map_err(|_| "create temp directory failed")?;
    let url = format!(
        "file://{}",
        directory.path().join("application.db").display()
    );
    {
        let db = Db::open(Config::new(&url, ""))
            .await
            .map_err(|_| "open file DB failed")?;
        prepare(&db).await?;
        exercise(&db).await?;
    }
    let db = Db::open(Config::new(&url, ""))
        .await
        .map_err(|_| "reopen file DB failed")?;
    prepare(&db).await?;
    let a = db.community("community-a").map_err(|_| "scope failed")?;
    let b = db.community("community-b").map_err(|_| "scope failed")?;
    require(read_value(&a).await? == 2, "reopened data differs")?;
    require(
        read_value(&b).await? == 1,
        "reopened community isolation failed",
    )
}

#[tokio::test]
async fn turso_round_trip_if_configured() -> TestResult {
    let (Ok(url), Ok(token)) = (std::env::var("TURSO_URL"), std::env::var("TURSO_TOKEN")) else {
        return Ok(());
    };
    if url.is_empty() || token.is_empty() {
        return Ok(());
    }
    // Use an explicitly disposable database. Both transports use crlt's audited TLS path.
    let db = Db::open(Config::new(url, token))
        .await
        .map_err(|_| "open Turso failed")?;
    prepare(&db).await?;
    let result = exercise(&db).await;
    for scope in ["community-a", "community-b"] {
        db.community(scope)
            .map_err(|_| "scope failed")?
            .execute(
                "DELETE FROM cthl_fixture WHERE resource = ?1",
                params!["example-setting"],
            )
            .await
            .map_err(|_| "fixture cleanup failed")?;
    }
    result
}
