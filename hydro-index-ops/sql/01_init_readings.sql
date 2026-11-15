-- ============================================================================
-- 水文数仓 · 水位明细样表初始化（容器首次启动时由 docker-entrypoint-initdb.d 执行）
-- 幂等：可重复执行（DROP CASCADE 仅用于初始化库，生产库请勿在已纳管库执行）
-- ============================================================================

-- 测站维表（样例）
CREATE TABLE IF NOT EXISTS stations (
    station_id   BIGINT PRIMARY KEY,
    station_code TEXT NOT NULL UNIQUE,
    station_name TEXT NOT NULL,
    basin        TEXT
);

-- 水位明细表
CREATE TABLE IF NOT EXISTS readings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY,
    station_id   BIGINT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    water_level  NUMERIC(8, 3) NOT NULL,          -- 水位，米
    discharge    NUMERIC(12, 3),                 -- 流量，立方米/秒（可空）
    quality      TEXT NOT NULL DEFAULT 'GOOD',   -- GOOD / SUSPECT / BAD
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_readings PRIMARY KEY (id),
    CONSTRAINT fk_readings_station FOREIGN KEY (station_id)
        REFERENCES stations (station_id)
);

COMMENT ON TABLE readings IS '水位明细（高频观测，持续写入）';

-- 归档表：结构与热表一致；id 与热表共用 readings_id_seq 序列，
-- 搬迁保留原 id、迟到直写归档取新 id，两表 id 全局唯一（跨表查询不重复）
CREATE TABLE IF NOT EXISTS readings_archive (
    id           BIGINT NOT NULL DEFAULT nextval('readings_id_seq'),
    station_id   BIGINT NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    water_level  NUMERIC(8, 3) NOT NULL,
    discharge    NUMERIC(12, 3),
    quality      TEXT NOT NULL DEFAULT 'GOOD',
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 何时迁入/直写归档
    CONSTRAINT pk_readings_archive PRIMARY KEY (id),
    CONSTRAINT fk_readings_archive_station FOREIGN KEY (station_id)
        REFERENCES stations (station_id)
);

COMMENT ON TABLE readings_archive IS '水位明细归档（按站+UTC 截止时刻从热表迁入）';

CREATE INDEX IF NOT EXISTS idx_readings_archive_station_observed
    ON readings_archive (station_id, observed_at);

-- 归档界线：每站一条，observed_at < cutoff 的观测属于归档；界线只许前进
CREATE TABLE IF NOT EXISTS archive_boundaries (
    station_id  BIGINT PRIMARY KEY
        REFERENCES stations (station_id),
    cutoff      TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE archive_boundaries IS '按站归档界线（只前进；与搬迁同事务提交）';

-- 10 个测站
INSERT INTO stations (station_id, station_code, station_name, basin)
SELECT s,
       'ST-' || lpad(s::text, 3, '0'),
       '水文站' || s,
       CASE WHEN s <= 5 THEN '长江流域' ELSE '黄河流域' END
FROM generate_series(1, 10) AS s
ON CONFLICT (station_id) DO NOTHING;

-- 样例明细：10 站 × 每站 10000 条 = 10 万行，约覆盖近 10000 小时
-- 用随机水位模拟观测序列，保证 CREATE INDEX 有可感知的构建时长
INSERT INTO readings (station_id, observed_at, water_level, discharge, quality)
SELECT s.station_id,
       now() - (g.t * INTERVAL '1 hour'),
       round((20 + 5 * sin(g.t / 24.0) + random() * 0.8)::numeric, 3),
       round((100 + random() * 900)::numeric, 3),
       CASE WHEN random() < 0.02 THEN 'SUSPECT' ELSE 'GOOD' END
FROM stations AS s
CROSS JOIN LATERAL generate_series(1, 10000) AS g(t)
WHERE NOT EXISTS (SELECT 1 FROM readings LIMIT 1);

-- 人工索引示例：验证恢复/清理流程绝不会误删非本系统创建的索引
CREATE INDEX IF NOT EXISTS idx_readings_ingested_manual
    ON readings (ingested_at);

ANALYZE readings;
