"""部署前检查点：一致快照生成、目录持久化与整库恢复。

快照通过 SQLite 在线备份 API（Connection.backup）生成，包含应用表、数据、
索引、触发器与迁移记录，并覆盖已提交的 WAL 数据，而不是裸复制主库文件。

快照与目录（JSON 元数据）保存在应用库之外的独立目录，先写临时文件再
原子改名，重启后可查询，历史快照生成后不可改写，临时/不完整快照不可见。

恢复在别名锁内完成：校验摘要与 SQLite 完整性后，先把快照备份到库旁的
临时文件并再次校验，再原子替换主库文件，查询不会看到半恢复状态，
失败时原库保持不变。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .engine import (
    DatabaseBusy,
    MigrationError,
    VersionConflict,
    _connect,
    _ensure_table,
    _read_records,
)


class CheckpointError(MigrationError):
    """检查点业务失败基类。"""


class CheckpointNotFound(CheckpointError):
    pass


class CheckpointAliasMismatch(CheckpointError):
    pass


class CheckpointCorrupt(CheckpointError):
    pass


@dataclass(frozen=True)
class CheckpointMeta:
    id: str
    alias: str
    version: int
    created_at: str
    sha256: str
    size_bytes: int


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _integrity_check(path: Path) -> None:
    with closing(sqlite3.connect(str(path))) as conn:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    if not rows or rows[0][0] != "ok":
        detail = "; ".join(str(r[0]) for r in rows[:5]) or "empty result"
        raise CheckpointCorrupt(f"integrity_check failed for {path.name}: {detail}")


def _current_version(db_path: Path) -> int:
    with closing(_connect(db_path)) as conn:
        _ensure_table(conn)
        records = _read_records(conn)
    return records[-1].version if records else 0


class CheckpointStore:
    """检查点目录：一个快照文件 + 一个同名 JSON 元数据，按 ID 不可改写。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _snap_path(self, checkpoint_id: str) -> Path:
        return self.root / f"{checkpoint_id}.db"

    def _meta_path(self, checkpoint_id: str) -> Path:
        return self.root / f"{checkpoint_id}.json"

    def _write_meta(self, meta: CheckpointMeta) -> None:
        tmp = self.root / f".{meta.id}.json.tmp"
        tmp.write_text(json.dumps(asdict(meta), indent=2), encoding="utf-8")
        os.replace(tmp, self._meta_path(meta.id))

    def create(self, alias: str, db_path: Path, lock: threading.RLock) -> CheckpointMeta:
        """在别名锁内用在线备份 API 生成一致快照并登记目录。"""
        with lock:
            version = _current_version(db_path)
            checkpoint_id = (
                datetime.now(timezone.utc).strftime("cp-%Y%m%dT%H%M%SZ-")
                + uuid.uuid4().hex[:8]
            )
            tmp_snap = self.root / f".{checkpoint_id}.db.tmp"
            try:
                with closing(_connect(db_path)) as src:
                    with closing(sqlite3.connect(str(tmp_snap))) as dst:
                        src.backup(dst)
                _integrity_check(tmp_snap)
                meta = CheckpointMeta(
                    id=checkpoint_id,
                    alias=alias,
                    version=version,
                    created_at=datetime.now(timezone.utc).isoformat(),
                    sha256=_sha256_file(tmp_snap),
                    size_bytes=tmp_snap.stat().st_size,
                )
                os.replace(tmp_snap, self._snap_path(checkpoint_id))
                self._write_meta(meta)
            finally:
                tmp_snap.unlink(missing_ok=True)
            return meta

    def list(self, alias: str | None = None) -> list[CheckpointMeta]:
        """只列出元数据与快照都完整的检查点，按创建时间排序。"""
        metas: list[CheckpointMeta] = []
        for meta_path in sorted(self.root.glob("cp-*.json")):
            try:
                raw = json.loads(meta_path.read_text(encoding="utf-8"))
                meta = CheckpointMeta(**raw)
            except (ValueError, TypeError, KeyError):
                continue
            if not self._snap_path(meta.id).is_file():
                continue
            if alias is not None and meta.alias != alias:
                continue
            metas.append(meta)
        metas.sort(key=lambda m: (m.created_at, m.id))
        return metas

    def get(self, checkpoint_id: str) -> CheckpointMeta:
        meta_path = self._meta_path(checkpoint_id)
        if not meta_path.is_file():
            raise CheckpointNotFound(f"unknown checkpoint: {checkpoint_id}")
        try:
            meta = CheckpointMeta(**json.loads(meta_path.read_text(encoding="utf-8")))
        except (ValueError, TypeError, KeyError) as exc:
            raise CheckpointCorrupt(f"checkpoint catalog entry corrupt: {checkpoint_id}") from exc
        if not self._snap_path(meta.id).is_file():
            raise CheckpointCorrupt(f"checkpoint snapshot missing: {checkpoint_id}")
        return meta

    def restore(
        self,
        alias: str,
        db_path: Path,
        checkpoint_id: str,
        expected_version: int,
        lock: threading.RLock,
    ) -> CheckpointMeta:
        """校验后整库恢复；任何失败都不动原库。"""
        with lock:
            meta = self.get(checkpoint_id)
            if meta.alias != alias:
                raise CheckpointAliasMismatch(
                    f"checkpoint {checkpoint_id} belongs to alias "
                    f"{meta.alias!r}, not {alias!r}"
                )
            current = _current_version(db_path)
            if current != expected_version:
                raise VersionConflict(
                    f"expected_version={expected_version} but database is at {current}"
                )
            snap = self._snap_path(meta.id)
            if _sha256_file(snap) != meta.sha256:
                raise CheckpointCorrupt(
                    f"checkpoint snapshot digest mismatch: {checkpoint_id}"
                )
            _integrity_check(snap)
            # 恢复前探测写锁：忙则明确拒绝，原库不变。
            try:
                with closing(_connect(db_path)) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("ROLLBACK")
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "locked" in msg or "busy" in msg:
                    raise DatabaseBusy(str(exc)) from exc
                raise
            # 先把快照备份到库旁临时文件并校验，再原子替换主库。
            tmp_restore = db_path.parent / f".{db_path.name}.restore-{uuid.uuid4().hex[:8]}.tmp"
            try:
                with closing(sqlite3.connect(str(snap))) as src:
                    with closing(sqlite3.connect(str(tmp_restore))) as dst:
                        src.backup(dst)
                _integrity_check(tmp_restore)
                os.replace(tmp_restore, db_path)
                for suffix in ("-wal", "-shm"):
                    Path(str(db_path) + suffix).unlink(missing_ok=True)
            finally:
                tmp_restore.unlink(missing_ok=True)
            return meta

