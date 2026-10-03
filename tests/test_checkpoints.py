import json
import sqlite3

import pytest

from app.checkpoints import (
    CheckpointAliasMismatch,
    CheckpointCorrupt,
    CheckpointNotFound,
    CheckpointStore,
)
from app.config import MIGRATION_TABLE
from app.engine import (
    DatabaseRegistry,
    ScriptFailed,
    VersionConflict,
    apply_manifest,
    status,
)
from app.manifest import MigrationManifest


def manifest(items, expected):
    return MigrationManifest.model_validate(
        {
            "expected_version": expected,
            "scripts": [
                {"version": i + 1, "description": f"m{i}", "sql": sql}
                for i, sql in enumerate(items)
            ],
        }
    )


@pytest.fixture()
def env(tmp_path):
    db = tmp_path / "data" / "app.db"
    db.parent.mkdir()
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'old')")
    conn.commit()
    conn.close()
    store = CheckpointStore(tmp_path / "checkpoints")
    locks = DatabaseRegistry()
    return db, store, locks


def test_create_list_and_persist_across_restart(env):
    db, store, locks = env
    apply_manifest(db, manifest(["ALTER TABLE t ADD COLUMN note TEXT;"], 0), locks.lock_for("a"))
    meta = store.create("demo", db, locks.lock_for("a"))
    assert meta.alias == "demo"
    assert meta.version == 1
    assert meta.sha256 and meta.size_bytes > 0
    # 新实例模拟重启：目录仍在，可查询。
    reopened = CheckpointStore(store.root)
    assert [m.id for m in reopened.list("demo")] == [meta.id]
    assert reopened.list("other") == []
    # 临时/不完整快照不可见。
    (store.root / ".cp-hidden.db.tmp").write_bytes(b"junk")
    (store.root / "cp-20990101T000000Z-deadbeef.json").write_text(
        json.dumps({"id": "cp-20990101T000000Z-deadbeef", "alias": "demo", "version": 0,
                    "created_at": "x", "sha256": "y", "size_bytes": 1})
    )
    assert [m.id for m in reopened.list("demo")] == [meta.id]


def test_snapshot_includes_committed_wal_and_objects(env):
    db, store, locks = env
    apply_manifest(
        db,
        manifest(
            [
                "CREATE INDEX idx_v ON t(v); "
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE t SET v = v WHERE 0; END;",
                "INSERT INTO t VALUES (2, 'wal-row');",
            ],
            0,
        ),
        locks.lock_for("a"),
    )
    meta = store.create("demo", db, locks.lock_for("a"))
    snap = store.root / f"{meta.id}.db"
    conn = sqlite3.connect(snap)
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    assert {"t", "idx_v", "trg", MIGRATION_TABLE} <= names
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    assert conn.execute(f"SELECT MAX(version) FROM {MIGRATION_TABLE}").fetchone()[0] == 2
    conn.close()


def test_restore_removes_later_objects_and_rewinds_history(env):
    db, store, locks = env
    apply_manifest(db, manifest(["ALTER TABLE t ADD COLUMN note TEXT;"], 0), locks.lock_for("a"))
    meta = store.create("demo", db, locks.lock_for("a"))
    apply_manifest(
        db,
        manifest(
            [
                "ALTER TABLE t ADD COLUMN note TEXT;",
                "CREATE TABLE later (id INTEGER); INSERT INTO t (id, v) VALUES (9, 'post');",
            ],
            1,
        ),
        locks.lock_for("a"),
    )
    assert status(db)["current_version"] == 2
    store.restore("demo", db, meta.id, 2, locks.lock_for("a"))
    after = status(db)
    assert after["current_version"] == 1
    conn = sqlite3.connect(db)
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "later" not in names
    assert conn.execute("SELECT COUNT(*) FROM t WHERE id = 9").fetchone()[0] == 0
    conn.close()
    # 快照与目录保留，可再次迁移。
    assert store.get(meta.id).sha256 == meta.sha256
    result = apply_manifest(
        db,
        manifest(
            ["ALTER TABLE t ADD COLUMN note TEXT;", "INSERT INTO t (id, v) VALUES (5, 'again');"],
            1,
        ),
        locks.lock_for("a"),
    )
    assert result.after_version == 2


def test_restore_rejects_unknown_alias_mismatch_and_version_conflict(env):
    db, store, locks = env
    meta = store.create("demo", db, locks.lock_for("a"))
    with pytest.raises(CheckpointNotFound):
        store.restore("demo", db, "cp-20000101T000000Z-nope000", 0, locks.lock_for("a"))
    with pytest.raises(CheckpointAliasMismatch):
        store.restore("billing", db, meta.id, 0, locks.lock_for("b"))
    with pytest.raises(VersionConflict):
        store.restore("demo", db, meta.id, 7, locks.lock_for("a"))
    # 拒绝后原库不变。
    assert status(db)["current_version"] == 0
    assert sqlite3.connect(db).execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1


def test_restore_rejects_corrupt_snapshot_and_leaves_db(env):
    db, store, locks = env
    meta = store.create("demo", db, locks.lock_for("a"))
    snap = store.root / f"{meta.id}.db"
    original = snap.read_bytes()
    snap.write_bytes(b"not a sqlite database")
    with pytest.raises(CheckpointCorrupt):
        store.restore("demo", db, meta.id, 0, locks.lock_for("a"))
    assert status(db)["current_version"] == 0
    snap.write_bytes(original)  # 快照恢复后仍可再次使用
    store.restore("demo", db, meta.id, 0, locks.lock_for("a"))


def test_forbidden_sql_is_business_error_with_failed_version(env):
    db, store, locks = env
    m = manifest(["VACUUM;"], 0)
    with pytest.raises(ScriptFailed) as exc:
        apply_manifest(db, m, locks.lock_for("a"))
    assert exc.value.version == 1
    assert "rejected sql" in exc.value.reason
    assert status(db)["current_version"] == 0


def test_deferred_foreign_key_fixed_across_scripts(env):
    db, store, locks = env
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER "
        "REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)"
    )
    conn.commit()
    conn.close()
    m = manifest(
        [
            "INSERT INTO c VALUES (1, 42);",  # 父行下一脚本才补齐
            "INSERT INTO p VALUES (42);",
        ],
        0,
    )
    result = apply_manifest(db, m, locks.lock_for("a"))
    assert result.applied == [1, 2]


def test_unresolved_foreign_key_still_rolls_back(env):
    db, store, locks = env
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER "
        "REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)"
    )
    conn.commit()
    conn.close()
    m = manifest(["INSERT INTO c VALUES (1, 42);"], 0)
    with pytest.raises(ScriptFailed) as exc:
        apply_manifest(db, m, locks.lock_for("a"))
    assert "foreign key" in exc.value.reason
    assert status(db)["current_version"] == 0
