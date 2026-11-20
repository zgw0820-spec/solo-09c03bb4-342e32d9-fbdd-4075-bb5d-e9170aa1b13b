"""PostgreSQL 访问层：连接管理、工单写入、序列/复制状态查询。"""
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor


@contextmanager
def connect(dsn: str, autocommit: bool = False) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(dsn)
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()


def ping(dsn: str) -> bool:
    try:
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


# priority 列定义（可空 + 0~3 约束）。逻辑复制不同步 DDL，由应用按
# “先订阅端、后发布端”的顺序在两库分别执行，见 rollout.py。
PRIORITY_COLUMN = "priority"
PRIORITY_CHECK_NAME = "work_orders_priority_check"


def column_exists(dsn: str, table: str, column: str) -> bool:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            """,
            (table, column),
        )
        return cur.fetchone() is not None


def add_priority_column(dsn: str) -> None:
    """幂等地给 work_orders 增加可空 priority(smallint, 0~3)。

    列与约束分两步、均带 IF NOT EXISTS/存在性判断：重复执行、进程重启后
    重入都安全。可空列不重写表、不影响复制中的旧写法 INSERT。
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE work_orders ADD COLUMN IF NOT EXISTS priority smallint"
        )
        cur.execute(
            "SELECT 1 FROM pg_constraint WHERE conname = %s",
            (PRIORITY_CHECK_NAME,),
        )
        if cur.fetchone() is None:
            cur.execute(
                f"ALTER TABLE work_orders ADD CONSTRAINT {PRIORITY_CHECK_NAME} "
                "CHECK (priority IS NULL OR priority BETWEEN 0 AND 3)"
            )
        conn.commit()


def insert_work_order(
    dsn: str,
    title: str,
    detail: str,
    source_db: str,
    priority: int | None = None,
) -> dict[str, Any]:
    """在指定库写入工单，编号由该库序列 nextval 生成并随事务一起提交。

    priority 仅在该库已具备 priority 列且调用方给出非空值时才写入；
    字段发布完成前、或调用方不传 priority 时，语句与旧写法完全一致，
    因此发布过程中两个库任意阶段都能正常受理工单。
    """
    include_priority = (
        priority is not None and column_exists(dsn, "work_orders", PRIORITY_COLUMN)
    )
    columns = ["title", "detail", "source_db"]
    params: list[Any] = [title, detail, source_db]
    returning = ["id", "title", "detail", "source_db", "created_at"]
    if include_priority:
        columns.append(PRIORITY_COLUMN)
        params.append(priority)
        returning.append(PRIORITY_COLUMN)
    placeholders = ", ".join(["%s"] * len(params))
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            INSERT INTO work_orders ({", ".join(columns)})
            VALUES ({placeholders})
            RETURNING {", ".join(returning)}
            """,
            params,
        )
        row = dict(cur.fetchone())
        conn.commit()
        # 响应结构保持稳定：列尚不存在或未传值时显式补 None
        row.setdefault(PRIORITY_COLUMN, None)
        return row


def list_work_orders(dsn: str, limit: int = 100) -> list[dict[str, Any]]:
    # 列未发布前用 NULL 占位，保证两库任意阶段响应结构一致
    if column_exists(dsn, "work_orders", PRIORITY_COLUMN):
        priority_select = PRIORITY_COLUMN
    else:
        priority_select = f"NULL::smallint AS {PRIORITY_COLUMN}"
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, title, detail, source_db, created_at, {priority_select}
            FROM work_orders
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def count_and_max(dsn: str) -> tuple[int, int | None]:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(id) FROM work_orders")
        count, max_id = cur.fetchone()
        return count, max_id


def sequence_snapshot(dsn: str) -> dict[str, Any]:
    """读取序列内部状态（不取号、不推进序列），并计算旧库预留上界。

    PostgreSQL 序列的 CACHE 机制：后端取块内第一个号时，整块 cache 个号被
    预留，且块上界会写入序列关系，因此 pg_sequence_last_value() 返回的是
    “已预留到的最高号”，而不是实际发出的最大号。以下两类空号都已包含在内：
      - 缓存未用号：后端预取整块后只用了前几个，连接断开后剩余号永久作废；
      - 回滚空号：事务中 nextval 取号后回滚，号不回收。
    所以首个新编号 = last_value + increment，严格大于旧库所有预留号。
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT seqstart, seqincrement, seqcache,
                   pg_sequence_last_value('work_orders_id_seq') AS last_value
            FROM pg_sequence WHERE seqrelid = 'work_orders_id_seq'::regclass
            """
        )
        start, increment, cache, last_value = cur.fetchone()
        if last_value is None:
            # 序列从未 nextval：起始号即下一个待发号，尚无预留
            return {
                "start": start,
                "increment": increment,
                "cache": cache,
                "last_value": None,
                "highest_reserved": start - increment,
                "first_new_id": start,
            }
        return {
            "start": start,
            "increment": increment,
            "cache": cache,
            "last_value": last_value,
            "highest_reserved": last_value,
            "first_new_id": last_value + increment,
        }


def count_active_app_transactions(dsn: str) -> int:
    """统计旧库上应用自身仍在进行中的写事务（等待在途写入结束用）。

    事务在语句之间（如应用取号后等待其它处理）状态为 idle in transaction，
    因此不能只看 state='active' 或当前 query 文本；凡是非系统后端、
    已分配事务 ID（说明发生过写操作）且未提交的事务都计入。
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND backend_type = 'client backend'
              AND xact_start IS NOT NULL
              AND backend_xid IS NOT NULL
            """
        )
        return cur.fetchone()[0]


def publisher_wal_positions(dsn: str) -> dict[str, Any]:
    """发布端 WAL 位置：当前写入位置 write_lsn，以及订阅端已确认回放到的 replay_lsn。"""
    result: dict[str, Any] = {}
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()")
        result["current_lsn"] = cur.fetchone()[0]
        cur.execute(
            """
            SELECT application_name, state, sent_lsn, replay_lsn
            FROM pg_stat_replication
            WHERE application_name = %s
            """,
            ("inspection_sub",),
        )
        row = cur.fetchone()
        if row:
            result["sub_state"] = row[1]
            result["sent_lsn"] = row[2]
            result["replay_lsn"] = row[3]
        else:
            result["sub_state"] = None
    return result


def subscriber_status(dsn: str, subscription_name: str) -> dict[str, Any]:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT oid, subenabled, subconninfo
            FROM pg_subscription WHERE subname = %s
            """,
            (subscription_name,),
        )
        row = cur.fetchone()
        if not row:
            return {"exists": False, "enabled": False, "conninfo": None}
        oid, enabled, conninfo = row
        cur.execute(
            """
            SELECT pid, received_lsn, latest_end_lsn, latest_end_time
            FROM pg_stat_subscription WHERE subid = %s
            """,
            (oid,),
        )
        stat = cur.fetchone()
        return {
            "exists": True,
            "enabled": enabled,
            "conninfo": conninfo,
            "pid": stat[0] if stat else None,
            "received_lsn": str(stat[1]) if stat and stat[1] is not None else None,
            "latest_end_lsn": str(stat[2]) if stat and stat[2] is not None else None,
            "latest_end_time": stat[3].isoformat() if stat and stat[3] else None,
        }


def alter_subscription(dsn: str, subscription_name: str, enable: bool) -> None:
    """暂停/恢复订阅。ALTER SUBSCRIPTION 不能在事务块中执行，需 autocommit。"""
    action = "ENABLE" if enable else "DISABLE"
    with connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"ALTER SUBSCRIPTION {subscription_name} {action}")


def align_sequence(dsn: str, first_new_id: int) -> None:
    """把新库序列拨到 first_new_id-1 且标记 is_called=true，
    使下一次 nextval 恰好返回 first_new_id。"""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT setval('work_orders_id_seq', %s, true)", (first_new_id - 1,))
        conn.commit()
