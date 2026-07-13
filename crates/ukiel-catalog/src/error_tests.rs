//! Plan 42: the catalog error taxonomy, pinned.
//!
//! These are pure tests on purpose. A classification that can only be checked by
//! provoking a real database into a real failure is a classification nobody
//! checks — and the two mappings that matter most here (managed-failover
//! SQLSTATEs, and "unknown means permanent") are exactly the ones a live test
//! would struggle to produce.

use crate::error::class_for_sqlstate;
use crate::{CatalogError, CatalogErrorClass};

#[test]
fn every_semantic_variant_has_a_stable_class() {
    use CatalogErrorClass::*;
    let cases: Vec<(CatalogError, CatalogErrorClass)> = vec![
        (
            CatalogError::Conflict {
                expected: 3,
                live_matched: 2,
            },
            SemanticConflict,
        ),
        (
            CatalogError::LeaseLost {
                partition: "{}".into(),
            },
            OwnershipViolation,
        ),
        (
            CatalogError::OffsetRace {
                topic: "t".into(),
                partition: 0,
            },
            OwnershipViolation,
        ),
        (
            CatalogError::LeasePartitionMismatch {
                partition: "{}".into(),
            },
            PermanentInput,
        ),
        (CatalogError::InvalidLeaseTtl("zero".into()), PermanentInput),
        (CatalogError::NotFound("events".into()), AbsentResource),
    ];
    for (err, want) in cases {
        assert_eq!(err.class(), want, "{err:?}");
        // Only transport-ish failures may drive a role rebuild. A conflict, a
        // lost lease, or a bad schema must never look "recoverable" — that is
        // how a semantic failure becomes an infinite retry.
        assert!(!err.is_recoverable_transport(), "{err:?}");
    }
}

#[test]
fn pool_timeout_and_io_are_recoverable_transport() {
    // The pool could not hand out a live connection inside its acquire timeout:
    // the database is unreachable *right now*, which is the failover signature.
    let err = CatalogError::Db(sqlx::Error::PoolTimedOut);
    assert_eq!(err.class(), CatalogErrorClass::Transport);
    assert!(err.is_recoverable_transport());

    let err = CatalogError::Db(sqlx::Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "reset by peer",
    )));
    assert_eq!(err.class(), CatalogErrorClass::Transport);
    assert!(err.is_recoverable_transport());
}

#[test]
fn a_closed_pool_is_our_shutdown_not_the_databases() {
    // Reconnecting a closed pool will never work, so retrying is a livelock.
    let err = CatalogError::Db(sqlx::Error::PoolClosed);
    assert_eq!(err.class(), CatalogErrorClass::PermanentDatabase);
    assert!(!err.is_recoverable_transport());
}

#[test]
fn sqlstate_mapping_covers_the_failover_signatures() {
    // What a managed writer actually emits while it goes away.
    for code in ["57P01", "57P02", "57P03", "53300"] {
        assert_eq!(
            class_for_sqlstate(code),
            CatalogErrorClass::Transport,
            "{code}"
        );
    }
    // Connection exception, whole class.
    for code in ["08000", "08003", "08006", "08001", "08004", "08P01"] {
        assert_eq!(
            class_for_sqlstate(code),
            CatalogErrorClass::Transport,
            "{code}"
        );
    }
    // PostgreSQL explicitly inviting a retry of the transaction.
    for code in ["40001", "40P01"] {
        assert_eq!(
            class_for_sqlstate(code),
            CatalogErrorClass::RetryableDatabase,
            "{code}"
        );
    }
}

#[test]
fn unknown_sqlstates_default_to_permanent() {
    // The load-bearing default. An unclassified failure treated as transient is
    // an infinite retry against a database that will never change its mind.
    for code in [
        "42601", // syntax error
        "42501", // insufficient privilege
        "23505", // unique violation
        "XX000", // internal error
        "",      // no code at all
        "99999", // not a real class
    ] {
        assert_eq!(
            class_for_sqlstate(code),
            CatalogErrorClass::PermanentDatabase,
            "{code}"
        );
    }
}

#[test]
fn an_unreachable_database_is_transport_even_when_it_arrives_as_a_migration_error() {
    // The failure that made this necessary: `run` classified every migration
    // error as permanent, so a process starting during a writer failover died
    // instead of waiting — and the restart hit the primary while it was still
    // being promoted.
    let err = CatalogError::Migrate(sqlx::migrate::MigrateError::Execute(sqlx::Error::Io(
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
    )));
    assert_eq!(err.class(), CatalogErrorClass::Transport);
    assert!(err.is_recoverable_transport());

    // But a migration that is actually wrong stays permanent: a checksum
    // mismatch is not fixed by waiting, and retrying it forever hides it.
    let err = CatalogError::Migrate(sqlx::migrate::MigrateError::VersionMismatch(3));
    assert_eq!(err.class(), CatalogErrorClass::PermanentDatabase);
    assert!(!err.is_recoverable_transport());
}
