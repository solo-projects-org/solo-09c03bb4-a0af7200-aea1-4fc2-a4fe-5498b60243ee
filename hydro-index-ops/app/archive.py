"""按站归档：界线推进、写入路由、跨表查询。

一致性设计（详细说明见 docs/archive.md）：

- **按站咨询锁读写锁协议**（多实例安全的关键）：
  - 写入路由取 **共享** 事务级咨询锁 `pg_advisory_xact_lock_shared`：
    同站并发写入互不阻塞，只与归档互斥；
  - 归档取 **排他** 事务级咨询锁 `pg_advisory_xact_lock`：
    等所有在途写入提交后才搬迁，持锁期间新写入被挡住，
    唤醒后读到的是最新界线——观测既不漏迁也不错放；
  - 锁键由站号哈希得出，不同站互不影响；锁随事务结束自动释放，
    进程崩溃/重启不会残留锁。
- **单事务搬迁**：`DELETE ... RETURNING` → `INSERT ... SELECT` 与界线推进
  在同一事务提交；归档失败或服务重启时整体回滚，
  数据与界线永远保持同一已提交状态。
- **界线只许前进**：cutoff 小于当前界线返回 409；等于当前界线为幂等重放
  （再扫一遍兜底搬迁，通常 0 行），重复提交绝不重复搬迁。
- **两表 id 全局唯一**：归档表 id 与热表共用 readings_id_seq 序列，
  跨表 UNION ALL 查询完整且不重复。
"""
from __future__ import annotations

import logging
from datetime import datetime

import psycopg

from .config import settings

log = logging.getLogger("hydro.archive")

# 咨询锁命名空间：锁键 = hashtextextended(f"{NS}:{station_id}", 0)
_LOCK_NS = "hydro-archive-station"


class ArchiveRegression(Exception):
    """请求的 cutoff 落后于当前界线（界线只许前进）。"""

    def __init__(self, current: datetime) -> None:
        self.current = current
        super().__init__(f"cutoff regresses below current boundary {current.isoformat()}")


class StationNotFound(Exception):
    """站号不存在。"""


class ArchiveLockTimeout(Exception):
    """等待按站咨询锁超时（同站另一归档进行中）。"""


def _lock_key(station_id: int) -> str:
    return f"{_LOCK_NS}:{station_id}"


async def _set_lock_timeout(conn: psycopg.AsyncConnection) -> None:
    # 事务级 lock_timeout：归档/写入路由等待按站锁的最长时间，
    # 防止异常情况下连接无限堆积；超时抛 LockNotAvailable，由 API 映射 409。
    await conn.execute(
        "SELECT set_config('lock_timeout', %s, true)",
        (str(settings.archive_lock_timeout_ms),),
    )


async def _take_lock(conn: psycopg.AsyncConnection, station_id: int, *, shared: bool) -> None:
    fn = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
    try:
        await conn.execute(
            f"SELECT {fn}(hashtextextended(%s, 0))", (_lock_key(station_id),)
        )
    except psycopg.errors.LockNotAvailable as exc:
        raise ArchiveLockTimeout(station_id) from exc


async def get_boundary(
    conn: psycopg.AsyncConnection, station_id: int
) -> datetime | None:
    row = await conn.execute(
        "SELECT cutoff FROM archive_boundaries WHERE station_id = %s", (station_id,)
    )
    rec = await row.fetchone()
    return rec["cutoff"] if rec else None


async def archive_station(
    conn: psycopg.AsyncConnection, station_id: int, cutoff: datetime
) -> dict:
    """把 station_id 站早于 cutoff 的热表观测迁入归档表，并推进界线。

    整个操作在调用方连接的事务内完成（排他咨询锁 → 校验 → 搬迁 → 推进界线），
    由 API 层统一 commit/rollback：任何失败或进程重启都会整体回滚，
    数据与界线保持同一已提交状态。
    """
    await _set_lock_timeout(conn)
    # 排他锁：等在途写入提交；持锁期间同站新写入/归档排队
    await _take_lock(conn, station_id, shared=False)

    existing = await get_boundary(conn, station_id)
    if existing is not None and cutoff < existing:
        raise ArchiveRegression(existing)
    replay = existing is not None and cutoff == existing

    if existing is None:
        # 首次归档才需要确认测站存在（界线表有外键，此后必然存在）
        row = await conn.execute(
            "SELECT 1 FROM stations WHERE station_id = %s", (station_id,)
        )
        if await row.fetchone() is None:
            raise StationNotFound(station_id)

    # 单语句原子搬迁：删出热表的同时按原 id 写入归档表。
    # 持排他锁期间同站不会有新提交的热表写入，DELETE 看到的是完整集合。
    cur = await conn.execute(
        """
        WITH moved AS (
            DELETE FROM readings
            WHERE station_id = %(station_id)s AND observed_at < %(cutoff)s
            RETURNING id, station_id, observed_at, water_level, discharge,
                      quality, ingested_at
        )
        INSERT INTO readings_archive
            (id, station_id, observed_at, water_level, discharge, quality, ingested_at)
        SELECT id, station_id, observed_at, water_level, discharge, quality,
               ingested_at
        FROM moved
        """,
        {"station_id": station_id, "cutoff": cutoff},
    )
    moved = cur.rowcount

    if not replay:
        await conn.execute(
            """
            INSERT INTO archive_boundaries (station_id, cutoff)
            VALUES (%s, %s)
            ON CONFLICT (station_id) DO UPDATE
                SET cutoff = EXCLUDED.cutoff, updated_at = now()
            """,
            (station_id, cutoff),
        )
    log.info(
        "station %s: archived %s rows with cutoff %s (replay=%s)",
        station_id, moved, cutoff.isoformat(), replay,
    )
    return {
        "moved_count": moved,
        "idempotent_replay": replay,
        "previous_cutoff": existing,
    }


async def insert_reading_routed(
    conn: psycopg.AsyncConnection, body: dict
) -> tuple[dict, str]:
    """按当前界线路由写入：晚于界线的旧观测直接写归档表，其余写热表。

    取共享咨询锁后与归档互斥：读到什么界线就用什么界线落库，
    归档要么等本次写入提交后把它一并迁走，要么先推进界线、
    本次写入被挡住后按新界线直接写归档——不会错放。

    返回 (row, source)：row 为落库行（归档路径含 archived_at），
    source 为 "hot" / "archive"。
    """
    station_id = body["station_id"]
    await _set_lock_timeout(conn)
    await _take_lock(conn, station_id, shared=True)
    cutoff = await get_boundary(conn, station_id)

    if cutoff is not None and body["observed_at"] < cutoff:
        cur = await conn.execute(
            """
            INSERT INTO readings_archive
                (station_id, observed_at, water_level, discharge, quality)
            VALUES (%(station_id)s, %(observed_at)s, %(water_level)s,
                    %(discharge)s, %(quality)s)
            RETURNING id, station_id, observed_at, water_level, discharge,
                      quality, ingested_at, archived_at
            """,
            body,
        )
        row = await cur.fetchone()
        return row, "archive"

    cur = await conn.execute(
        """
        INSERT INTO readings
            (station_id, observed_at, water_level, discharge, quality)
        VALUES (%(station_id)s, %(observed_at)s, %(water_level)s,
                %(discharge)s, %(quality)s)
        RETURNING id, station_id, observed_at, water_level, discharge,
                  quality, ingested_at
        """,
        body,
    )
    row = await cur.fetchone()
    return row, "hot"


def union_query(station_id: int, start: str | None, end: str | None) -> tuple[str, dict]:
    """跨热表+归档表的完整查询（UNION ALL：任一行在任一已提交状态下只存在于
    一张表，且两表 id 共用序列全局唯一，因此绝不重复）。"""
    clauses = ["station_id = %(station_id)s"]
    params: dict = {"station_id": station_id}
    if start:
        clauses.append("observed_at >= %(start)s")
        params["start"] = start
    if end:
        clauses.append("observed_at <= %(end)s")
        params["end"] = end
    where = " AND ".join(clauses)
    sql_text = f"""
        SELECT id, station_id, observed_at, water_level, discharge, quality,
               ingested_at, archived_at, source
        FROM (
            SELECT id, station_id, observed_at, water_level, discharge, quality,
                   ingested_at, NULL::timestamptz AS archived_at, 'hot'::text AS source
            FROM readings
            WHERE {where}
            UNION ALL
            SELECT id, station_id, observed_at, water_level, discharge, quality,
                   ingested_at, archived_at, 'archive'::text AS source
            FROM readings_archive
            WHERE {where}
        ) AS all_readings
        ORDER BY observed_at DESC, id DESC
        LIMIT %(limit)s
    """
    return sql_text, params


async def list_boundaries(conn: psycopg.AsyncConnection) -> list[dict]:
    row = await conn.execute(
        "SELECT station_id, cutoff, updated_at FROM archive_boundaries "
        "ORDER BY station_id"
    )
    return list(await row.fetchall())


async def boundary_detail(conn: psycopg.AsyncConnection, station_id: int) -> dict | None:
    row = await conn.execute(
        """
        SELECT b.station_id, b.cutoff, b.updated_at,
               (SELECT count(*) FROM readings
                 WHERE station_id = b.station_id) AS hot_rows,
               (SELECT count(*) FROM readings_archive
                 WHERE station_id = b.station_id) AS archived_rows
        FROM archive_boundaries b
        WHERE b.station_id = %s
        """,
        (station_id,),
    )
    return await row.fetchone()
