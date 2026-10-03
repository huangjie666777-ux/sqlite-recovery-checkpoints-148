import sqlite3
import threading

import pytest

from app.config import MIGRATION_TABLE
from app.engine import (
    DatabaseRegistry,
    HistoryMismatch,
    ScriptFailed,
    VersionConflict,
    apply_manifest,
    status,
)
from app.guard import GuardError, inspect_script
from app.manifest import MigrationManifest


def manifest(items, expected=None):
    return MigrationManifest.model_validate(
        {
            "expected_version": len(items) if expected is None else expected,
            "scripts": [
                {"version": i + 1, "description": f"m{i}", "sql": sql}
                for i, sql in enumerate(items)
            ],
        }
    )


@pytest.fixture()
def db(tmp_path):
    p = tmp_path / "app.db"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'old')")
    conn.commit()
    conn.close()
    return p


def test_apply_and_idempotent_resend(db):
    locks = DatabaseRegistry()
    m1 = manifest(
        ["ALTER TABLE t ADD COLUMN note TEXT;", "INSERT INTO t (id, v, note) VALUES (2, 'new', 'hi');"]
    )
    m1.expected_version = 0
    result = apply_manifest(db, m1, locks.lock_for("x"))
    assert result.after_version == 2
    assert result.applied == [1, 2]

    resend = manifest(
        ["ALTER TABLE t ADD COLUMN note TEXT;", "INSERT INTO t (id, v, note) VALUES (2, 'new', 'hi');"]
    )
    result2 = apply_manifest(db, resend, locks.lock_for("x"))
    assert result2.applied == []
    assert result2.after_version == 2
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    conn.close()


def test_rewritten_history_rejected(db):
    locks = DatabaseRegistry()
    m1 = manifest(["ALTER TABLE t ADD COLUMN note TEXT;"])
    m1.expected_version = 0
    apply_manifest(db, m1, locks.lock_for("x"))

    changed = manifest(["ALTER TABLE t ADD COLUMN other TEXT;"], expected=1)
    with pytest.raises(HistoryMismatch):
        apply_manifest(db, changed, locks.lock_for("x"))


def test_missing_history_rejected(db):
    locks = DatabaseRegistry()
    m1 = manifest(["ALTER TABLE t ADD COLUMN note TEXT;"])
    m1.expected_version = 0
    apply_manifest(db, m1, locks.lock_for("x"))

    valid = MigrationManifest.model_validate(
        {
            "expected_version": 1,
            "scripts": [
                {"version": 1, "description": "m0", "sql": "ALTER TABLE t ADD COLUMN note TEXT;"},
                {"version": 2, "description": "m1", "sql": "INSERT INTO t (id, v, note) VALUES (3, 'x', 'y');"},
            ],
        }
    )
    apply_manifest(db, valid, locks.lock_for("x"))

    truncated = MigrationManifest.model_validate(
        {
            "expected_version": 2,
            "scripts": [
                {"version": 1, "description": "m0", "sql": "ALTER TABLE t ADD COLUMN note TEXT;"},
            ],
        }
    )
    with pytest.raises(HistoryMismatch):
        apply_manifest(db, truncated, locks.lock_for("x"))


def test_expected_version_conflict(db):
    m = manifest(["ALTER TABLE t ADD COLUMN note TEXT;"])
    m.expected_version = 5
    with pytest.raises(VersionConflict):
        apply_manifest(db, m, DatabaseRegistry().lock_for("x"))


def test_failure_rolls_back_whole_batch(db):
    m = manifest(
        [
            "CREATE TABLE ok (id INTEGER); INSERT INTO ok VALUES (1);",
            "INSERT INTO nope VALUES (1);",
        ]
    )
    m.expected_version = 0
    with pytest.raises(ScriptFailed) as exc:
        apply_manifest(db, m, DatabaseRegistry().lock_for("x"))
    assert exc.value.version == 2
    conn = sqlite3.connect(db)
    names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "ok" not in names
    conn.close()
    assert status(db)["current_version"] == 0


def test_foreign_key_violation_rollback(tmp_path):
    p = tmp_path / "fk.db"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))"
    )
    conn.close()
    m = manifest(
        [
            "INSERT INTO p VALUES (1);",
            "INSERT INTO c VALUES (1, 999);",
        ]
    )
    m.expected_version = 0
    with pytest.raises(ScriptFailed) as exc:
        apply_manifest(p, m, DatabaseRegistry().lock_for("c"))
    assert exc.value.version == 2
    assert "foreign key" in exc.value.reason
    conn = sqlite3.connect(p)
    assert conn.execute("SELECT COUNT(*) FROM p").fetchone()[0] == 0
    conn.close()


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN; CREATE TABLE x(a);",
        "COMMIT;",
        "ROLLBACK;",
        "SAVEPOINT s;",
        "ATTACH DATABASE '/tmp/x.db' AS x;",
        "DETACH DATABASE x;",
        "PRAGMA journal_mode=WAL;",
        "VACUUM;",
        "SELECT load_extension('x');",
    ],
)
def test_forbidden_statements(sql):
    with pytest.raises(GuardError):
        inspect_script(sql, MIGRATION_TABLE)


def test_migration_table_write_denied(db):
    m = manifest([f"INSERT INTO {MIGRATION_TABLE} VALUES (1, 'x', 'y');"])
    m.expected_version = 0
    with pytest.raises(ScriptFailed) as exc:
        apply_manifest(db, m, DatabaseRegistry().lock_for("x"))
    assert exc.value.version == 1


def test_trigger_on_migration_table_denied(db):
    sql = (
        f"CREATE TRIGGER evil AFTER INSERT ON {MIGRATION_TABLE} "
        "BEGIN INSERT INTO t VALUES (9,'hack'); END;"
    )
    m = manifest([sql])
    m.expected_version = 0
    with pytest.raises(ScriptFailed):
        apply_manifest(db, m, DatabaseRegistry().lock_for("x"))


def test_status_reports_digests(db):
    s0 = status(db)
    assert s0["current_version"] == 0
    m = manifest(["ALTER TABLE t ADD COLUMN note TEXT;"])
    m.expected_version = 0
    apply_manifest(db, m, threading.RLock())
    s1 = status(db)
    assert s1["current_version"] == 1
    assert len(s1["applied"][0]["sql_sha256"]) == 64


def test_multi_statement_trigger_runs(db):
    sql = (
        "CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT); "
        "CREATE TRIGGER tr AFTER INSERT ON t "
        "BEGIN "
        "INSERT INTO log(msg) VALUES ('a;b'); "
        "INSERT INTO log(msg) VALUES ('c'); "
        "END; "
        "INSERT INTO t (id, v) VALUES (7, 'trig');"
    )
    m = manifest([sql])
    m.expected_version = 0
    apply_manifest(db, m, DatabaseRegistry().lock_for("x"))
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT msg FROM log ORDER BY id").fetchall()
    assert rows == [("a;b",), ("c",)]
    conn.close()
