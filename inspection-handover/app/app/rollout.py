"""priority 字段的一次性发布状态机。

背景：PostgreSQL 逻辑复制只复制表数据（DML），不复制 DDL。给
work_orders 增加可空 priority(0~3) 列必须在两个库分别执行，且顺序有要求：

    先订阅端（新库）加列 -> 等复制追平 -> 再发布端（旧库）加列 -> 再等追平

原因：若发布端先加列，订阅端还没有该列，后续带新列的写入复制过来会在
订阅端报错并中断 apply；订阅端先有同名列后，发布端再加列，全程复制的
INSERT 都不包含 priority（可空列默认 NULL），旧写法在任何阶段都可用。

阶段只进不退、每步幂等并持久化到 SQLite：
  NOT_STARTED -> NEW_COLUMN_ADDED -> CAUGHT_UP_AFTER_NEW
  -> PUBLISHER_COLUMN_ADDED -> CAUGHT_UP_AFTER_PUBLISHER -> COMPLETED

任一步失败：不改变接单路径，网页继续按旧写法提交；记录失败阶段与未完成
的库，再次点击从已生效阶段继续。应用重启后同样从持久化阶段恢复。
"""
import json
import threading
import time

from . import db
from .config import Settings
from .state import ROLLOUT_PHASES, StateStore


class RolloutError(Exception):
    """字段发布失败（含等待追平超时、某库 DDL 失败）。旧写法不受影响。"""


# 与 state.ROLLOUT_PHASES 同序的索引表
_phase_order = {phase: idx for idx, phase in enumerate(ROLLOUT_PHASES)}


class RolloutManager:
    PHASE_LABELS = {
        "NOT_STARTED": "未开始",
        "NEW_COLUMN_ADDED": "订阅端（新库）已加 priority 列",
        "CAUGHT_UP_AFTER_NEW": "新库加列后复制已追平",
        "PUBLISHER_COLUMN_ADDED": "发布端（旧库）已加 priority 列",
        "CAUGHT_UP_AFTER_PUBLISHER": "发布端加列后复制已追平",
        "COMPLETED": "发布完成，网页已开放优先级填写",
    }

    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store
        # 发布串行化：进行中的再次触发返回冲突提示
        self.lock = threading.Lock()

    # ---- 状态视图 -----------------------------------------------------
    def is_completed(self) -> bool:
        return self.store.get_rollout_phase() == "COMPLETED"

    def overview(self) -> dict:
        phase = self.store.get_rollout_phase()
        new_column = self._safe_column_exists(self.settings.new_dsn)
        old_column = self._safe_column_exists(self.settings.old_dsn)
        unfinished = []
        if not new_column:
            unfinished.append("new")
        if not old_column:
            unfinished.append("old")
        last_error = self.store.get_kv("rollout_last_error")
        return {
            "phase": phase,
            "phase_label": self.PHASE_LABELS.get(phase, phase),
            "completed": phase == "COMPLETED",
            "new_db_has_column": new_column,
            "old_db_has_column": old_column,
            # 列都已存在但阶段未到 COMPLETED（如进程在两次追平之间重启）：
            # 没有缺列的库，但发布仍需重试走完追平与收尾
            "unfinished_dbs": unfinished,
            "in_progress": phase not in ("NOT_STARTED", "COMPLETED"),
            "last_error": json.loads(last_error) if last_error else None,
        }

    def _safe_column_exists(self, dsn: str) -> bool:
        try:
            return db.column_exists(dsn, "work_orders", db.PRIORITY_COLUMN)
        except Exception:
            # 状态查询不能因单库临时不可达而失败；按“未知=未就绪”处理
            return False

    # ---- 发布主流程 ---------------------------------------------------
    def start_rollout(self) -> dict:
        if self.is_completed():
            # 完成后重复点击：只回显现状，不重复执行
            return {
                "already_done": True,
                **self.overview(),
                "message": "priority 字段发布已生效，两库均具备可空 priority 列",
            }

        if not self.lock.acquire(blocking=False):
            raise RolloutError("已有字段发布操作进行中，请等待其结束后重试")

        try:
            return self._run_rollout()
        finally:
            self.lock.release()

    def _run_rollout(self) -> dict:
        s = self.settings
        deadline = time.monotonic() + s.cutover_timeout_seconds
        # 每个等待步骤共享同一个整体截止时间，与交接超时语义一致
        def remaining() -> float:
            return deadline - time.monotonic()

        try:
            self._step_new_column(remaining)
            self._step_catch_up_after_new(remaining)
            self._step_publisher_column(remaining)
            self._step_catch_up_after_publisher(remaining)
        except RolloutError as exc:
            self._record_failure(str(exc))
            raise
        except Exception as exc:  # 意外错误同样落记录并保持旧模式
            message = f"字段发布出错: {exc!r}"
            self._record_failure(message)
            raise RolloutError(message) from exc

        self.store.advance_rollout_phase("COMPLETED")
        self.store.set_kv("rollout_last_error", "")  # 成功后清空上次失败记录
        self.store.log_event(
            "ROLLOUT",
            "priority(0~3) 字段发布完成：两库均具备可空 priority 列，网页开放优先级填写",
        )
        return {"already_done": False, **self.overview(), "message": "发布成功"}

    # 步骤 1：订阅端（新库）先加列。DDL 幂等，已存在则只核对并推进阶段，
    # 因此“DDL 已生效但阶段尚未落盘”的中断场景可安全续跑。
    def _step_new_column(self, remaining) -> None:
        phase = self.store.get_rollout_phase()
        order = _phase_order
        if order[phase] < order["NEW_COLUMN_ADDED"]:
            db.add_priority_column(self.settings.new_dsn)
            if not db.column_exists(
                self.settings.new_dsn, "work_orders", db.PRIORITY_COLUMN
            ):
                raise RolloutError("订阅端（新库）priority 列添加后校验不存在")
            self.store.advance_rollout_phase("NEW_COLUMN_ADDED")
            self.store.log_event("ROLLOUT", "订阅端（新库）已添加可空 priority 列（0~3）")

    # 步骤 2：等订阅端加列后的复制状态追平，确认没有积压再动发布端结构
    def _step_catch_up_after_new(self, remaining) -> None:
        phase = self.store.get_rollout_phase()
        order = _phase_order
        if order[phase] < order["CAUGHT_UP_AFTER_NEW"]:
            self._wait_caught_up(remaining, "新库加列后")
            self.store.advance_rollout_phase("CAUGHT_UP_AFTER_NEW")
            self.store.log_event("ROLLOUT", "新库加列后逻辑复制已追平")

    # 步骤 3：发布端（旧库）后加列。此刻订阅端已具备同名列，复制不受影响
    def _step_publisher_column(self, remaining) -> None:
        phase = self.store.get_rollout_phase()
        order = _phase_order
        if order[phase] < order["PUBLISHER_COLUMN_ADDED"]:
            db.add_priority_column(self.settings.old_dsn)
            if not db.column_exists(
                self.settings.old_dsn, "work_orders", db.PRIORITY_COLUMN
            ):
                raise RolloutError("发布端（旧库）priority 列添加后校验不存在")
            self.store.advance_rollout_phase("PUBLISHER_COLUMN_ADDED")
            self.store.log_event("ROLLOUT", "发布端（旧库）已添加可空 priority 列（0~3）")

    # 步骤 4：发布端加列后再次确认追平，两库结构与数据位置一致后收尾
    def _step_catch_up_after_publisher(self, remaining) -> None:
        phase = self.store.get_rollout_phase()
        order = _phase_order
        if order[phase] < order["CAUGHT_UP_AFTER_PUBLISHER"]:
            self._wait_caught_up(remaining, "发布端加列后")
            self.store.advance_rollout_phase("CAUGHT_UP_AFTER_PUBLISHER")
            self.store.log_event("ROLLOUT", "发布端加列后逻辑复制已追平，两库结构一致")

    def _wait_caught_up(self, remaining, label: str) -> None:
        s = self.settings
        while True:
            pub = db.publisher_wal_positions(s.old_dsn)
            sub = db.subscriber_status(s.new_dsn, s.subscription_name)
            replay = pub.get("replay_lsn")
            current = pub["current_lsn"]
            if (
                sub.get("enabled")
                and replay is not None
                and pub.get("sub_state") == "streaming"
                and str(replay) == str(current)
            ):
                return
            if remaining() <= 0:
                raise RolloutError(
                    f"等待复制追平超时（{label}；current={current}，"
                    f"replay={replay}，sub_state={pub.get('sub_state')}，"
                    f"订阅启用={sub.get('enabled')}）"
                )
            time.sleep(s.poll_interval_seconds)

    def _record_failure(self, message: str) -> None:
        overview = self.overview()
        payload = {
            "ts": time.time(),
            "phase": overview["phase"],
            "unfinished_dbs": overview["unfinished_dbs"],
            "message": message,
        }
        self.store.set_kv("rollout_last_error", json.dumps(payload, ensure_ascii=False))
        self.store.log_event(
            "ROLLOUT",
            f"字段发布未完成，保持旧写法：{message}；未完成的库："
            f"{overview['unfinished_dbs'] or '无（待重试走完追平/收尾）'}",
            payload,
        )
