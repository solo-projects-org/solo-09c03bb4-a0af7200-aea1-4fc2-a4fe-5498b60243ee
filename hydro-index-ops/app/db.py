"""psycopg3 异步连接池与批次表 DDL。"""
from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import settings

log = logging.getLogger("hydro.db")

pool: AsyncConnectionPool | None = None

# 批次状态机：
#   PENDING    已受理，等待构建（含 CLEANING 清理失败后的待重试）
#   BUILDING   CREATE INDEX CONCURRENTLY 会话进行中
#   CANCELLING 已请求取消，等待构建会话结束并清理残件
#   CLEANING   构建失败/取消后正在 DROP 残件
#   SUCCEEDED  构建成功且索引有效（终态）
#   FAILED     构建失败且残件已清净（终态）
#   CANCELLED  取消完成且残件已清净（终态）
DDL_BATCH_TABLE = """
CREATE TABLE IF NOT EXISTS index_build_batches (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state            TEXT NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    build_pid        INTEGER,
    phase            TEXT,
    error            TEXT,
    cleanup_attempts INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_batch_state CHECK (
        state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING',
                  'SUCCEEDED', 'FAILED', 'CANCELLED')
    )
);

-- 同一时刻只允许一个非终态批次（从数据库层面兜底“只受理一批”）
CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_one_active
    ON index_build_batches ((1))
    WHERE state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING');
"""

# 归档表与界线表：
#   readings_archive  结构与热表一致；id 与热表共用 readings_id_seq 序列，
#                     搬迁保留原 id、迟到直写归档取新 id，两表 id 全局唯一，
#                     跨表 UNION ALL 查询绝不重复。
#   archive_boundaries 每站一条归档界线：observed_at < cutoff 的观测属于归档；
#                     界线只许前进（应用层在按站排他咨询锁内校验并推进）。
# 搬迁（DELETE→INSERT）与界线推进在同一事务提交：归档失败或服务重启时
# 两者一起回滚，数据与界线永远保持同一已提交状态。
DDL_ARCHIVE_TABLES = """
CREATE TABLE IF NOT EXISTS readings_archive (
    id           BIGINT NOT NULL DEFAULT nextval('readings_id_seq'),
    station_id   BIGINT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    water_level  NUMERIC(8, 3) NOT NULL,
    discharge    NUMERIC(12, 3),
    quality      TEXT NOT NULL DEFAULT 'GOOD',
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_readings_archive PRIMARY KEY (id),
    CONSTRAINT fk_readings_archive_station FOREIGN KEY (station_id)
        REFERENCES stations (station_id)
);

CREATE INDEX IF NOT EXISTS idx_readings_archive_station_observed
    ON readings_archive (station_id, observed_at);

CREATE TABLE IF NOT EXISTS archive_boundaries (
    station_id  BIGINT PRIMARY KEY
        REFERENCES stations (station_id),
    cutoff      TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_pool() -> AsyncConnectionPool:
    global pool
    pool = AsyncConnectionPool(
        conninfo=settings.pg_dsn,
        min_size=2,
        max_size=10,
        kwargs={"row_factory": dict_row, "autocommit": False},
        check=AsyncConnectionPool.check_connection,
        open=False,
    )
    await pool.open()
    async with pool.connection() as conn:
        await conn.execute(DDL_BATCH_TABLE)
        await conn.execute(DDL_ARCHIVE_TABLES)
        await conn.commit()
    log.info("connection pool ready, batch/archive tables ensured")
    return pool


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
        log.info("connection pool closed")
