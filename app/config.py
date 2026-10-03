"""Runtime configuration loaded from aliases.json.


HTTP 调用方只提交数据库别名，真实文件路径只在服务端配置，

避免客户端借迁移接口操作任意 SQLite 文件。

"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

MAX_REQUEST_BYTES = int(os.environ.get("MIGRATION_MAX_REQUEST_BYTES", 2 * 1024 * 1024))
MAX_SCRIPTS = int(os.environ.get("MIGRATION_MAX_SCRIPTS", 200))
SQLITE_BUSY_TIMEOUT_SECONDS = float(os.environ.get("MIGRATION_SQLITE_TIMEOUT", 5))

# 服务端内部保存迁移历史的表名。脚本对该表的任何读写都会被拒绝。
MIGRATION_TABLE = "__schema_migration_log__"


@dataclass(frozen=True)
class Settings:
    aliases: dict[str, Path]
    checkpoint_dir: Path


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    cfg_path = Path(path or os.environ.get("MIGRATION_CONFIG", "aliases.json"))
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    aliases: dict[str, Path] = {}
    for alias, db_path in raw["aliases"].items():
        if not isinstance(alias, str) or not alias.strip():
            raise ValueError(f"invalid alias: {alias!r}")
        p = Path(db_path)
        if not p.is_absolute():
            p = cfg_path.parent / p
        aliases[alias] = p.resolve()
    if not aliases:
        raise ValueError("aliases.json must define at least one alias")
    cp_raw = raw.get("checkpoint_dir", "checkpoints")
    cp = Path(cp_raw)
    if not cp.is_absolute():
        cp = cfg_path.parent / cp
    return Settings(aliases=aliases, checkpoint_dir=cp.resolve())
