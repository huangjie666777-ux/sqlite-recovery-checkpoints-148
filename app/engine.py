"""迁移存储引擎：版本读取、历史核对与事务化应用。"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .config import MIGRATION_TABLE, SQLITE_BUSY_TIMEOUT_SECONDS
from .guard import GuardError, inspect_script, make_authorizer
from .manifest import MigrationItem, MigrationManifest, sql_digest
from .sqlsplit import TokenizeError


class MigrationError(Exception):
    """迁移业务失败基类。"""


class VersionConflict(MigrationError):
    pass


class HistoryMismatch(MigrationError):
    pass

class DatabaseBusy(MigrationError):
    pass


class ScriptFailed(MigrationError):
    def __init__(self, version: int, reason: str):
        super().__init__(f"migration v{version} failed: {reason}")
        self.version = version
        self.reason = reason


@dataclass(frozen=True)
class AppliedRecord:
    version: int
    description: str
    sql_sha256: str


@dataclass(frozen=True)
class ApplyResult:
    before_version: int
    after_version: int
    applied: list[int]


class DatabaseRegistry:
    """每个别名一把可重入锁，序列化进程内的版本检查与写入。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.RLock] = {}
        self._guard = threading.Lock()

    def lock_for(self, alias: str) -> threading.RLock:
        with self._guard:
            lock = self._locks.get(alias)
            if lock is None:
                lock = threading.RLock()
                self._locks[alias] = lock
            return lock


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=SQLITE_BUSY_TIMEOUT_SECONDS,
        isolation_level=None,  # 手动事务，避免驱动隐式 BEGIN
    )
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
            version INTEGER PRIMARY KEY CHECK (version >= 1),
            description TEXT NOT NULL,
            sql_sha256 TEXT NOT NULL
        )
        """
    )


def _read_records(conn: sqlite3.Connection) -> list[AppliedRecord]:
    rows = conn.execute(
        f"SELECT version, description, sql_sha256 FROM {MIGRATION_TABLE} ORDER BY version"
    ).fetchall()
    return [
        AppliedRecord(r["version"], r["description"], r["sql_sha256"]) for r in rows
    ]


def status(db_path: Path) -> dict:
    """读取真实已应用版本与摘要，进程内只持有短连接。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(_connect(db_path)) as conn:
        _ensure_table(conn)
        records = _read_records(conn)
    return {
        "current_version": records[-1].version if records else 0,
        "applied": [
            {"version": r.version, "description": r.description, "sql_sha256": r.sql_sha256}
            for r in records
        ],
    }


def _check_history(records: list[AppliedRecord], scripts: list[MigrationItem]) -> None:
    applied_versions = [r.version for r in records]
    expected = list(range(1, len(records) + 1))
    if applied_versions != expected:
        raise HistoryMismatch(
            f"applied versions are not a 1-based prefix: {applied_versions}"
        )
    for record, item in zip(records, scripts):
        digest = sql_digest(item.sql)
        if record.sql_sha256 != digest:
            raise HistoryMismatch(
                f"history digest mismatch at version {record.version}; "
                f"stored={record.sql_sha256} submitted={digest}"
            )
        if record.description != item.description:
            raise HistoryMismatch(
                f"history description mismatch at version {record.version}"
            )
    if len(scripts) < len(records):
        raise HistoryMismatch(
            f"manifest omits applied history: applied={len(records)} submitted={len(scripts)}"
        )


def apply_manifest(
    db_path: Path,
    manifest: MigrationManifest,
    lock: threading.RLock,
) -> ApplyResult:
    """在锁保护下完成 检查 -> 执行 -> 记录，全部同事务提交或整批回滚。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with lock:
        return _apply_locked(db_path, manifest)


def _apply_locked(db_path: Path, manifest: MigrationManifest) -> ApplyResult:
    with closing(_connect(db_path)) as conn:
        try:
            conn.execute("PRAGMA busy_timeout = %d" % int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000))
            conn.execute("PRAGMA foreign_keys = ON")
            # 先取得数据库写锁，再在事务内检查版本与历史，
            # 避免检查与写入之间被其它写入者插队。
            conn.execute("BEGIN IMMEDIATE")
            try:
                _ensure_table(conn)
                records = _read_records(conn)
                current = records[-1].version if records else 0

                if current != manifest.expected_version:
                    raise VersionConflict(
                        f"expected_version={manifest.expected_version} but database is at {current}"
                    )
                _check_history(records, manifest.scripts)

                pending = manifest.scripts[current:]
                before = current

                if not pending:
                    conn.execute("COMMIT")
                    return ApplyResult(
                        before_version=before, after_version=current, applied=[]
                    )

                # 全部待执行脚本先做语句层校验，任何一条不合法都不动数据库。
                # 禁用 SQL 属于业务拒绝，返回失败版本而不是 HTTP 500。
                parsed: list[tuple[MigrationItem, list[str]]] = []
                authorizer = make_authorizer(MIGRATION_TABLE)

                def _allow_all(*_args):
                    return sqlite3.SQLITE_OK

                for item in pending:
                    try:
                        statements = inspect_script(item.sql, MIGRATION_TABLE)
                    except (GuardError, TokenizeError) as exc:
                        raise ScriptFailed(item.version, f"rejected sql: {exc}") from exc
                    parsed.append((item, statements))

                conn.set_authorizer(authorizer)
                applied: list[int] = []
                for item, statements in parsed:
                    try:
                        for stmt in statements:
                            conn.execute(stmt)
                        conn.set_authorizer(_allow_all)
                        conn.execute(
                            f"INSERT INTO {MIGRATION_TABLE} (version, description, sql_sha256) "
                            "VALUES (?, ?, ?)",
                            (item.version, item.description, sql_digest(item.sql)),
                        )
                        conn.set_authorizer(authorizer)
                        applied.append(item.version)
                    except ScriptFailed:
                        raise
                    except sqlite3.Error as exc:
                        if "foreign key" in str(exc).lower():
                            raise ScriptFailed(
                                item.version, "foreign key constraint violation"
                            ) from exc
                        raise ScriptFailed(item.version, str(exc)) from exc
                # 外键（含延迟外键）在整批末尾统一验证，
                # 允许跨脚本先插子行后补父行的合法修复。
                conn.set_authorizer(_allow_all)
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    detail = ", ".join(
                        f"table={v[0]} rowid={v[1]} target={v[2]} fkid={v[3]}"
                        for v in violations[:5]
                    )
                    raise ScriptFailed(
                        pending[-1].version, f"foreign key violation: {detail}"
                    )
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.set_authorizer(None)

            return ApplyResult(
                before_version=before,
                after_version=before + len(applied),
                applied=applied,
            )
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" in msg or "busy" in msg:
                raise DatabaseBusy(str(exc)) from exc
            raise
