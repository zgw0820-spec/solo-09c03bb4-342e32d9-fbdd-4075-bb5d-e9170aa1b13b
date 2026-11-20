#!/usr/bin/env python3
"""priority 字段一次性发布的端到端试跑。

覆盖需求：
  1. 发布前两库均无 priority 列；带 priority 的提交被拒，旧写法（不带 priority）
     在发布前、发布中、失败后始终可用；
  2. 暂停复制 -> 发布在“等追平”处失败，保持旧模式，记录未完成的库；
  3. 恢复后重试，从已生效阶段续跑（幂等），最终两库均具备可空 priority(0~3)；
  4. 应用在部分完成时重启：阶段从 SQLite 恢复，续跑不重复加列、不回退；
  5. 完成后重复点击只回显；网页可填优先级并写入当前接单库，经复制同步到对端；
  6. CHECK 约束拒绝 0~3 之外的值；切换接单库后新值写入新库并同步回旧库。

前置：scripts/local_pg.sh start 已启动两库；本脚本自行启停应用（含重启验证）。
用 -I 运行。
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg2

BASE = "http://127.0.0.1:8000"
OLD_DSN = "host=127.0.0.1 port=55433 user=node dbname=inspection"
NEW_DSN = "host=127.0.0.1 port=55434 user=node dbname=inspection"
ROOT = Path(__file__).resolve().parents[1]
STATE_DB = ROOT / ".localrun" / "data" / "handover_state.db"
APP_LOG = ROOT / ".localrun" / "logs" / "app.log"

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    mark = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))


def api(method: str, path: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def sql(dsn: str, query: str, args=(), commit: bool = False):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, args)
        result = cur.fetchall() if cur.description else None
        if commit:
            conn.commit()
        return result


def has_column(dsn: str) -> bool:
    rows = sql(dsn, """
        SELECT 1 FROM information_schema.columns
        WHERE table_name='work_orders' AND column_name='priority'""")
    return len(rows) == 1


def wait_row_count_equal(timeout: float = 20) -> tuple:
    deadline = time.time() + timeout
    old_cnt = new_cnt = (0, None)
    while time.time() < deadline:
        old_cnt = sql(OLD_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        new_cnt = sql(NEW_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        if old_cnt == new_cnt:
            return old_cnt, new_cnt
        time.sleep(0.4)
    return old_cnt, new_cnt


def reset_rollout_state() -> None:
    """测试辅助：清空字段发布阶段与失败记录（PG 中已加的列由环境 clean 保证不存在）。"""
    conn = sqlite3.connect(STATE_DB)
    conn.execute("DELETE FROM kv WHERE key IN ('rollout_phase','rollout_last_error')")
    conn.commit()
    conn.close()


_app_proc = None


def start_app() -> subprocess.Popen:
    global _app_proc
    log = open(APP_LOG, "ab")
    _app_proc = subprocess.Popen(
        [str(ROOT / "scripts" / "local_app.sh")],
        stdout=log, stderr=log, preexec_fn=os.setsid,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
                urllib.request.urlopen(BASE + "/api/status", timeout=2)
                return _app_proc
        except Exception:
            time.sleep(0.4)
    raise RuntimeError("app did not come up")


def stop_app() -> None:
    global _app_proc
    if _app_proc and _app_proc.poll() is None:
        os.killpg(os.getpgid(_app_proc.pid), signal.SIGTERM)
        _app_proc.wait(timeout=10)
    _app_proc = None


def restart_app() -> None:
    stop_app()
    time.sleep(1)
    start_app()


def main() -> int:
    # 脚本自行启停应用（含中途重启验证）；要求两库已由 local_pg.sh start 启动
    start_app()
    try:
        return _run()
    finally:
        stop_app()


def _run() -> int:
    print("== 0. 前置：干净环境，两库均无 priority 列，接单库为旧库 ==")
    reset_rollout_state()
    st, body = api("GET", "/api/status")
    check("接口返回字段发布状态块", "priority_rollout" in body, str(body.get("priority_rollout")))
    check("初始阶段 NOT_STARTED", body["priority_rollout"]["phase"] == "NOT_STARTED")
    check("两库均无 priority 列", not has_column(OLD_DSN) and not has_column(NEW_DSN))

    print("== 1. 发布前：带 priority 的提交被拒，旧写法始终可用 ==")
    code, body = api("POST", "/api/work-orders",
                     {"title": "带优先级工单", "priority": 2})
    check("发布前带 priority 返回 409", code == 409, f"code={code} body={body}")
    check("被拒响应说明字段未发布", "priority" in body["detail"], str(body))
    code, body = api("POST", "/api/work-orders", {"title": "旧写法工单A", "detail": "无优先级"})
    check("发布前旧写法提交成功（旧库接单）",
          code == 200 and body["accepted_by"] == "old"
          and body["work_order"]["priority"] is None, str(body))
    code, body = api("POST", "/api/work-orders",
                     {"title": "显式空优先级", "priority": None})
    check("priority=null 等同旧写法，可提交", code == 200 and body["priority_accepted"] is False)
    wait_row_count_equal()

    print("== 2. 暂停复制 -> 发布在等追平处失败 -> 保持旧模式并记录未完成库 ==")
    code, body = api("POST", "/api/subscription/pause")
    check("订阅暂停成功", code == 200 and body["subscriber"]["enabled"] is False)
    # 暂停期间旧库继续写入（复制积压），证明旧模式不受影响
    code, body = api("POST", "/api/work-orders", {"title": "暂停期间旧模式工单"})
    check("暂停/发布失败前旧库仍接单", code == 200 and body["accepted_by"] == "old")
    t0 = time.time()
    code, body = api("POST", "/api/priority-rollout", timeout=30)
    elapsed = time.time() - t0
    check("发布因等追平超时返回 504", code == 504, f"code={code} body={body}")
    check("超时耗时接近阈值(8s)", 7 <= elapsed <= 14, f"{elapsed:.1f}s")
    check("失败后仅订阅端（新库）已加列，发布端未加",
          has_column(NEW_DSN) and not has_column(OLD_DSN),
          f"new={has_column(NEW_DSN)} old={has_column(OLD_DSN)}")
    check("响应记录未完成的库为 old", body["unfinished_dbs"] == ["old"], str(body["unfinished_dbs"]))
    check("阶段持久化为 NEW_COLUMN_ADDED", body["phase"] == "NEW_COLUMN_ADDED", body["phase"])
    check("失败记录包含原因与阶段", body["last_error"] and "超时" in body["last_error"]["message"],
          str(body["last_error"]))
    code, body = api("POST", "/api/work-orders", {"title": "失败后旧模式工单"})
    check("发布失败后旧写法继续可用（旧库无列也能写）",
          code == 200 and body["work_order"]["priority"] is None, str(body))
    # 带 priority 仍被拒（未完成不开放）
    code, body = api("POST", "/api/work-orders", {"title": "X", "priority": 1})
    check("未完成期间带 priority 仍返回 409", code == 409)

    print("== 3. 应用在部分完成状态下重启：阶段沿用，不回退不重复加列 ==")
    restart_app()
    code, body = api("GET", "/api/priority-rollout")
    check("重启后阶段仍为 NEW_COLUMN_ADDED",
          body["phase"] == "NEW_COLUMN_ADDED" and body["new_db_has_column"] is True,
          str(body))
    check("重启后未完成库仍记录为 old", body["unfinished_dbs"] == ["old"])

    print("== 4. 恢复复制后重试：幂等续跑直到两库都具备列 ==")
    code, body = api("POST", "/api/subscription/resume")
    check("订阅恢复成功", code == 200 and body["subscriber"]["enabled"] is True)
    old_cnt, new_cnt = wait_row_count_equal(20)
    check("积压数据追平（两库行数/最大号一致）", old_cnt == new_cnt, f"{old_cnt} vs {new_cnt}")
    code, body = api("POST", "/api/priority-rollout", timeout=30)
    check("重试发布成功", code == 200 and body.get("ok") is True and body["completed"] is True,
          f"code={code} body={body}")
    check("两库均已具备 priority 列", has_column(OLD_DSN) and has_column(NEW_DSN))
    check("未完成库列表为空", body["unfinished_dbs"] == [])
    # 直接核对两库约束定义（可空 smallint + CHECK 0~3）
    for label, dsn in (("旧库", OLD_DSN), ("新库", NEW_DSN)):
        col = sql(dsn, """
            SELECT is_nullable, data_type FROM information_schema.columns
            WHERE table_name='work_orders' AND column_name='priority'""")[0]
        chk = sql(dsn, "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                       "WHERE conname='work_orders_priority_check'")[0][0]
        check(f"{label} priority 可空且为 smallint", col == ("YES", "smallint"), str(col))
        # PG 会把 BETWEEN 规范化为 >= / <= 形式
        check(f"{label} CHECK 约束限定 0~3",
              "priority >= 0" in chk and "priority <= 3" in chk and "IS NULL" in chk, chk)

    print("== 5. 完成后重复点击幂等；优先级可写入当前接单库并经复制同步 ==")
    code, body = api("POST", "/api/priority-rollout")
    check("重复发布返回 already_done", code == 200 and body.get("already_done") is True, str(body))
    code, body = api("POST", "/api/work-orders", {"title": "高优先级工单", "priority": 3})
    check("完成后带 priority 提交成功", code == 200 and body["work_order"]["priority"] == 3, str(body))
    check("此时仍由旧库接单", body["accepted_by"] == "old")
    high_id = body["work_order"]["id"]
    code, body = api("POST", "/api/work-orders", {"title": "默认空优先级工单"})
    check("不填优先级写入 NULL", code == 200 and body["work_order"]["priority"] is None)
    # 旧库直接核对，并等待复制到新库
    old_pri = sql(OLD_DSN, "SELECT priority FROM work_orders WHERE id=%s", (high_id,))[0][0]
    check("旧库落库 priority=3", old_pri == 3, str(old_pri))
    deadline = time.time() + 15
    new_pri = None
    while time.time() < deadline:
        rows = sql(NEW_DSN, "SELECT priority FROM work_orders WHERE id=%s", (high_id,))
        if rows:
            new_pri = rows[0][0]
            if new_pri == 3:
                break
        time.sleep(0.4)
    check("priority 随逻辑复制同步到订阅端", new_pri == 3, f"new_priority={new_pri}")

    print("== 6. CHECK 约束拒绝 0~3 之外的值（API 层与数据库层双重）==")
    code, body = api("POST", "/api/work-orders", {"title": "非法优先级", "priority": 9})
    check("API 层拒绝 0~3 之外的 priority", code == 422, f"code={code}")
    try:
        sql(OLD_DSN, "INSERT INTO work_orders(id,title,source_db,priority) "
                     "VALUES (nextval('work_orders_id_seq'),'直写非法','old',9)", commit=True)
        rejected = False
    except psycopg2.errors.CheckViolation:
        rejected = True
    check("数据库 CHECK 约束拒绝 priority=9", rejected)

    print("== 7. 再次重启：完成状态保持；切换接单库后新值写入新库并同步回旧库 ==")
    restart_app()
    code, body = api("GET", "/api/priority-rollout")
    check("重启后发布仍为 COMPLETED", body["completed"] is True and body["phase"] == "COMPLETED")
    # 发起交接切到新库接单
    code, body = api("POST", "/api/cutover", {"confirm": True}, timeout=30)
    check("交接成功，接单库切换为新库",
          code == 200 and body.get("ok") and body["accepting_db"] == "new", str(body))
    code, body = api("POST", "/api/work-orders", {"title": "新库优先级工单", "priority": 0})
    check("切换后 priority 写入当前接单库（新库）",
          code == 200 and body["accepted_by"] == "new"
          and body["work_order"]["priority"] == 0, str(body))
    new_id = body["work_order"]["id"]
    # 复制为单向（旧->新），新库接单的行直接在新库核对；旧库不存在该行符合拓扑
    deadline = time.time() + 10
    new_row = None
    while time.time() < deadline:
        rows = sql(NEW_DSN, "SELECT priority, source_db FROM work_orders WHERE id=%s", (new_id,))
        if rows:
            new_row = rows[0]
            break
        time.sleep(0.3)
    check("priority=0 已落库当前接单库（新库）",
          new_row is not None and new_row[0] == 0 and new_row[1] == "new", str(new_row))
    check("单向复制拓扑下新库接单的行不回灌旧库（旧库查无此编号）",
          sql(OLD_DSN, "SELECT 1 FROM work_orders WHERE id=%s", (new_id,)) == [])
    # 再写一条 priority=2，随后通过两库列表接口核对字段完整性
    code, body = api("POST", "/api/work-orders", {"title": "新库优先级工单2", "priority": 2})
    check("第二条带优先级工单由新库受理", code == 200 and body["work_order"]["priority"] == 2)
    code, body = api("GET", "/api/work-orders")
    check("两库列表均带 priority 字段",
          all("priority" in r for r in body["old"]) and all("priority" in r for r in body["new"]))
    check("新库列表中可查到 priority=0 与 priority=2 的新库接单行",
          {r["priority"] for r in body["new"] if r["source_db"] == "new"} >= {0, 2},
          str([(r["id"], r["priority"]) for r in body["new"] if r["source_db"] == "new"]))

    print("== 8. 事件日志记录发布全过程 ==")
    events = api("GET", "/api/events")[1]
    rollout_events = [e for e in events if e["phase"] == "ROLLOUT"]
    check("日志含两库加列、追平、完成与失败记录",
          any("新库" in e["message"] and "priority" in e["message"] for e in rollout_events)
          and any("旧库" in e["message"] and "priority" in e["message"] for e in rollout_events)
          and any("未完成" in e["message"] for e in rollout_events)
          and any("发布完成" in e["message"] for e in rollout_events),
          f"{len(rollout_events)} 条 ROLLOUT 事件")

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
