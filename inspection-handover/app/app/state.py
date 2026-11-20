"""交接状态持久化。

接单库状态保存在 SQLite（挂载到持久卷），应用重启后沿用已生效的接单库。
状态取值：
  OLD        旧库接单（初始）
  SWITCHING  交接进行中：旧库已暂停接单，等待追平
  NEW        新库接单（已生效）
"""
import json
import sqlite3
import threading
import time
from pathlib import Path

VALID_STATES = ("OLD", "SWITCHING", "NEW")

# priority 字段一次性发布的阶段（只进不退，每步幂等，重启沿用）：
#   NOT_STARTED                 未开始，两库均无 priority 列，网页只能用旧写法
#   NEW_COLUMN_ADDED            订阅端（新库）已加可空 priority 列
#   CAUGHT_UP_AFTER_NEW         新库加列后复制已追平
#   PUBLISHER_COLUMN_ADDED      发布端（旧库）已加可空 priority 列
#   CAUGHT_UP_AFTER_PUBLISHER   发布端加列后复制再次追平，两库结构一致
#   COMPLETED                   发布完成，网页开放优先级填写
ROLLOUT_PHASES = (
    "NOT_STARTED",
    "NEW_COLUMN_ADDED",
    "CAUGHT_UP_AFTER_NEW",
    "PUBLISHER_COLUMN_ADDED",
    "CAUGHT_UP_AFTER_PUBLISHER",
    "COMPLETED",
)
# 阶段顺序索引，用于比较进度与计算未完成的库
_ROLLOUT_ORDER = {phase: idx for idx, phase in enumerate(ROLLOUT_PHASES)}


class StateStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts         REAL NOT NULL,
                    phase      TEXT NOT NULL,
                    message    TEXT NOT NULL,
                    detail     TEXT
                );
                """
            )
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            row = cur.fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO kv(key, value) VALUES ('state', 'OLD')"
                )
            self._conn.commit()

    def get_state(self) -> str:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            return cur.fetchone()["value"]

    def set_state(self, state: str) -> None:
        if state not in VALID_STATES:
            raise ValueError(f"invalid state {state}")
        with self._lock:
            self._conn.execute(
                "UPDATE kv SET value = ? WHERE key = 'state'", (state,)
            )
            self._conn.commit()

    # ---- priority 字段发布阶段（通用 kv，原子推进，不回退）--------------
    def get_kv(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,))
            row = cur.fetchone()
            return row["value"] if row else default

    def set_kv(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            self._conn.commit()

    def get_rollout_phase(self) -> str:
        phase = self.get_kv("rollout_phase", "NOT_STARTED")
        return phase if phase in _ROLLOUT_ORDER else "NOT_STARTED"

    def advance_rollout_phase(self, phase: str) -> None:
        """只允许沿阶段顺序向前推进；重复到达同一阶段视为幂等。"""
        if phase not in _ROLLOUT_ORDER:
            raise ValueError(f"invalid rollout phase {phase}")
        current = self.get_rollout_phase()
        if _ROLLOUT_ORDER[phase] >= _ROLLOUT_ORDER[current]:
            self.set_kv("rollout_phase", phase)

    def log_event(self, phase: str, message: str, detail: dict | None = None) -> dict:
        with self._lock:
            ts = time.time()
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ts, phase, message, json.dumps(detail, ensure_ascii=False) if detail else None),
            )
            self._conn.commit()
            return {"ts": ts, "phase": phase, "message": message, "detail": detail or {}}

    def list_events(self, limit: int = 100) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, phase, message, detail FROM events "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [
                {
                    "ts": r["ts"],
                    "phase": r["phase"],
                    "message": r["message"],
                    "detail": json.loads(r["detail"]) if r["detail"] else {},
                }
                for r in cur.fetchall()
            ]
