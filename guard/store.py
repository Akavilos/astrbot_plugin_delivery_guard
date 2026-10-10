"""保存送达状态、缺席收件和待补发内容，并提供数量与到期查询。"""


from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

_SCHEMA = """
 
-- 持久化表与索引定义；字段名称供存储接口及既有数据读取使用。
CREATE TABLE IF NOT EXISTS state (
    umo   TEXT NOT NULL,
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (umo, key)
);


CREATE TABLE IF NOT EXISTS outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    umo         TEXT NOT NULL,
    content     TEXT NOT NULL,
    created_ts  REAL NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',    
    attempts    INTEGER NOT NULL DEFAULT 0,
    reason      TEXT NOT NULL DEFAULT '',
    last_err    TEXT NOT NULL DEFAULT ''
);


CREATE TABLE IF NOT EXISTS inbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    umo         TEXT NOT NULL,
    sender      TEXT NOT NULL DEFAULT '',
    text        TEXT NOT NULL,
    ts          REAL NOT NULL,
    delivered   INTEGER NOT NULL DEFAULT 0     
);


CREATE TABLE IF NOT EXISTS usage (
    day   TEXT NOT NULL,
    umo   TEXT NOT NULL,
    sent  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, umo)
);

CREATE INDEX IF NOT EXISTS idx_outbox_umo ON outbox(umo, status);
CREATE INDEX IF NOT EXISTS idx_inbox_umo  ON inbox(umo, delivered);
"""


def today_str() -> str:
    return time.strftime("%Y-%m-%d")


class Store:
    def __init__(self, path: Path) -> None:
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
        self._db.row_factory = sqlite3.Row
        try:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA busy_timeout=8000")
        except sqlite3.Error:
            pass
        with self._lock:
            self._db.executescript(_SCHEMA)
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._db.close()
            except Exception:
                pass


    def get_state(self, umo: str, key: str, default: str = "") -> str:
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM state WHERE umo=? AND key=?", (umo, key)
            ).fetchone()
        return row["value"] if row else default

    def set_state(self, umo: str, key: str, value: str) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO state(umo,key,value) VALUES(?,?,?) "
                "ON CONFLICT(umo,key) DO UPDATE SET value=excluded.value",
                (umo, key, str(value)),
            )
            self._db.commit()

    def list_umos(self) -> list[str]:
        with self._lock:
            rows = self._db.execute("SELECT DISTINCT umo FROM state").fetchall()
            rows += self._db.execute("SELECT DISTINCT umo FROM outbox").fetchall()
            rows += self._db.execute("SELECT DISTINCT umo FROM inbox").fetchall()
        return sorted({r["umo"] for r in rows})


    def get_status(self, umo: str) -> str:
        return self.get_state(umo, "status", "ok")

    def is_blocked(self, umo: str) -> bool:
        return self.get_status(umo) != "ok"

    def mark_blocked(self, umo: str, reason: str, mute_until: float = 0.0) -> None:
        was = self.get_status(umo)
        self.set_state(umo, "status", "muted" if mute_until else "unreachable")
        self.set_state(umo, "reason", reason)
        self.set_state(umo, "mute_until", repr(mute_until))
        self.set_state(umo, "blocked_since", repr(time.time()))
        self.set_state(umo, "recover_streak", "0")
        if was == "ok":
            self.set_state(umo, "need_brief", "1")

    def mark_ok(self, umo: str) -> None:
        self.set_state(umo, "status", "ok")
        self.set_state(umo, "reason", "")
        self.set_state(umo, "mute_until", "0")
        self.set_state(umo, "recover_streak", "0")

    def get_mute_until(self, umo: str) -> float:
        try:
            return float(self.get_state(umo, "mute_until", "0") or 0)
        except (TypeError, ValueError):
            return 0.0

    def bump_recover_streak(self, umo: str) -> int:
        n = int(self.get_state(umo, "recover_streak", "0") or 0) + 1
        self.set_state(umo, "recover_streak", str(n))
        return n


    def bump_sent(self, umo: str) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO usage(day,umo,sent) VALUES(?,?,1) "
                "ON CONFLICT(day,umo) DO UPDATE SET sent=sent+1",
                (today_str(), umo),
            )
            self._db.commit()

    def sent_today(self, umo: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT sent FROM usage WHERE day=? AND umo=?", (today_str(), umo)
            ).fetchone()
        return int(row["sent"]) if row else 0


    def push_inbox(self, umo: str, sender: str, text: str) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO inbox(umo,sender,text,ts) VALUES(?,?,?,?)",
                (umo, sender, text, time.time()),
            )
            self._db.commit()

    def peek_inbox(self, umo: str, limit: int = 8) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(
                "SELECT * FROM inbox WHERE umo=? AND delivered=0 ORDER BY id ASC LIMIT ?",
                (umo, limit),
            ).fetchall()

    def count_inbox(self, umo: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) c FROM inbox WHERE umo=? AND delivered=0", (umo,)
            ).fetchone()
        return int(row["c"]) if row else 0

    def mark_inbox_delivered(self, umo: str) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE inbox SET delivered=1 WHERE umo=? AND delivered=0", (umo,)
            )
            self._db.commit()


    def push_outbox(self, umo: str, content: str, reason: str = "") -> int:
        with self._lock:
            cur = self._db.execute(
                "INSERT INTO outbox(umo,content,created_ts,reason) VALUES(?,?,?,?)",
                (umo, content, time.time(), reason),
            )
            self._db.commit()
            return int(cur.lastrowid or -1)

    def due_outbox(self, umo: str, ttl_seconds: float, limit: int = 5) -> list[sqlite3.Row]:
        cutoff = time.time() - ttl_seconds
        with self._lock:
            self._db.execute(
                "UPDATE outbox SET status='expired' "
                "WHERE umo=? AND status='pending' AND created_ts < ?",
                (umo, cutoff),
            )
            self._db.commit()
            return self._db.execute(
                "SELECT * FROM outbox WHERE umo=? AND status='pending' "
                "ORDER BY id ASC LIMIT ?",
                (umo, limit),
            ).fetchall()

    def count_outbox(self, umo: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) c FROM outbox WHERE umo=? AND status='pending'", (umo,)
            ).fetchone()
        return int(row["c"]) if row else 0

    def mark_outbox(self, oid: int, status: str, last_err: str = "") -> None:
        with self._lock:
            self._db.execute(
                "UPDATE outbox SET status=?, attempts=attempts+1, last_err=? WHERE id=?",
                (status, last_err[:300], oid),
            )
            self._db.commit()

    def recent_outbox(self, umo: str, limit: int = 20) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(
                "SELECT * FROM outbox WHERE umo=? ORDER BY id DESC LIMIT ?", (umo, limit)
            ).fetchall()

    def list_blocked(self) -> list[tuple[str, str, str]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT umo, key, value FROM state WHERE key IN ('status','reason')"
            ).fetchall()
        agg: dict[str, dict] = {}
        for r in rows:
            agg.setdefault(r["umo"], {})[r["key"]] = r["value"]
        return [
            (umo, d.get("status", "ok"), d.get("reason", ""))
            for umo, d in agg.items()
            if d.get("status", "ok") != "ok"
        ]


def dump_rows(rows) -> str:
    return json.dumps([dict(r) for r in rows], ensure_ascii=False)
