"""SQLite 持久层：DDL、行映射。WAL + busy_timeout 配合全局锁。"""
import sqlite3

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS backups (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    kind            TEXT NOT NULL CHECK (kind IN ('full','incremental')),
    taken_at        TEXT NOT NULL,              -- 备份时点(UTC ISO)
    expires_at      TEXT,                       -- 过期时点；NULL=不过期
    parent_id       INTEGER REFERENCES backups(id),
    size_kb         INTEGER NOT NULL DEFAULT 0,
    pending_delete  INTEGER NOT NULL DEFAULT 0,-- 清理已标记，等最后一场占用演练结束
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_backups_chain ON backups(parent_id);

CREATE TABLE IF NOT EXISTS drills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    label           TEXT NOT NULL,
    target_time     TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN
                    ('queued','running','interrupted','partial','completed','failed')),
    recovered_time  TEXT,                       -- 实际恢复到的时点
    gap_backup_id   INTEGER REFERENCES backups(id) ON DELETE SET NULL, -- 首个缺段；备份被清理后置空
    gap_kind        TEXT,                       -- missing_full | deleted | expired
    gap_detail      TEXT,
    plan_frozen_at  TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drill_steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    drill_id        INTEGER NOT NULL REFERENCES drills(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    backup_id       INTEGER REFERENCES backups(id), -- 备份被清理后置空，步骤审计保留
    backup_name     TEXT,
    backup_time     TEXT,                       -- 冗余留存：备份被清理后恢复时点仍可算
    status          TEXT NOT NULL CHECK (status IN ('pending','done','skipped')),
    executed_at     TEXT,
    UNIQUE(drill_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_steps_drill ON drill_steps(drill_id, seq);

-- 一场演练对一个备份至多占用一次（重试/重启均不重复占用）
CREATE TABLE IF NOT EXISTS reservations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    drill_id        INTEGER NOT NULL REFERENCES drills(id) ON DELETE CASCADE,
    backup_id       INTEGER NOT NULL REFERENCES backups(id),
    created_at      TEXT NOT NULL,
    UNIQUE(drill_id, backup_id)
);
CREATE INDEX IF NOT EXISTS idx_res_backup ON reservations(backup_id);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    detail          TEXT NOT NULL
);
"""

ACTIVE_DRILL = "status IN ('queued','running','interrupted')"
TERMINAL_DRILL = "status IN ('partial','completed','failed')"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def backup_dict(r: sqlite3.Row, now: str, in_use: int | None = None,
                pending: int | None = None) -> dict:
    d = dict(r)
    d['expired'] = bool(d['expires_at'] and d['expires_at'] <= now)
    d['pending_delete'] = bool(d['pending_delete'])
    d['in_use'] = int(in_use) if in_use is not None else None
    return d
