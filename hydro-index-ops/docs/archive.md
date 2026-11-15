# 按站归档：设计、一致性与并发保证

水文数仓的日常入库表 `readings`（热表）持续增长。归档功能按
**站号 + UTC 截止时刻（界线）** 把旧水位从热表搬到归档表 `readings_archive`，
缩小热表体积，同时保证历史追溯不漏数、不重复。

## 数据模型

```
readings            热表：id BIGSERIAL（readings_id_seq）
readings_archive    归档表：结构与热表一致 + archived_at；
                    id DEFAULT nextval('readings_id_seq') —— 与热表共用同一序列
archive_boundaries  界线表：station_id PK, cutoff TIMESTAMPTZ, updated_at
```

关键约定：

- **界线语义**：`observed_at < cutoff` 的观测属于归档（严格小于；
  等于界线的观测留在热表）。
- **两表 id 全局唯一**：归档表与热表共用 `readings_id_seq`。
  搬迁保留原 id（显式插入）；迟到直写归档的观测取序列新 id。
  因此跨表 `UNION ALL` 查询不可能出现重复 id。
- **任一已提交状态下，一行只存在于一张表**：搬迁是单事务
  `DELETE … RETURNING` → `INSERT … SELECT`，外界要么看到旧状态
  （行在热表、界线未推进），要么看到新状态（行在归档表、界线已推进）。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/archive` | 按站号+UTC 截止时刻归档；幂等；界线只许前进 |
| GET  | `/archive/boundaries` | 全部测站的归档界线 |
| GET  | `/archive/boundaries/{station_id}` | 单站界线 + 热表/归档表行数（核对不漏数） |
| POST | `/readings` | 写入观测；`observed_at < 界线` 的迟到观测**直接写归档表** |
| GET  | `/readings` | 按站号+时间查询，**跨热表+归档表** 返回完整记录（UNION ALL） |

`POST /archive` 的响应：

```json
{
  "station_id": 6,
  "cutoff": "2026-05-01T00:00:00Z",
  "previous_cutoff": null,
  "moved_count": 2500,
  "idempotent_replay": false,
  "executed_at": "2026-11-15T12:00:00Z"
}
```

- `cutoff` 小于当前界线 → **409**（界线只许前进），响应含当前界线；
- `cutoff` 等于当前界线 → **200 幂等重放**：`idempotent_replay=true`，
  仍会兜底扫描一遍（正常为 0 行），重复提交绝不重复搬迁；
- 站号不存在 → 404；`cutoff` 不带时区 → 422（界线是 UTC 时刻，必须明确时区）。

## 一致性保证如何落地

### 1. 搬迁与界线推进是同一事务

`archive_station()` 在调用方连接的一个事务里依次完成：

```
BEGIN
  SELECT set_config('lock_timeout', …, true)
  SELECT pg_advisory_xact_lock(<站号锁键>)          -- 排他
  校验界线只前进
  WITH moved AS (DELETE FROM readings WHERE station_id=:s AND observed_at < :c
                 RETURNING …)
  INSERT INTO readings_archive SELECT … FROM moved  -- 单语句原子搬迁
  UPSERT archive_boundaries                          -- 推进界线
COMMIT
```

- **归档失败**（如目标表触发器异常、磁盘错误）：事务回滚，
  热表行不动、归档表不进、界线不推进——数据与界线保持同一已提交状态，
  客户端可安全重试。
- **服务重启/进程崩溃**：连接断开，未提交事务被 PostgreSQL 回滚，
  效果同上。无需像 CIC 那样的恢复流程——单事务语义即恢复语义。

### 2. 按站咨询锁读写锁协议（多实例不漏迁、不错放）

两个 API 实例可能同时写入同一测站并推进其界线。锁协议：

| 操作 | 锁 | 说明 |
|---|---|---|
| 写入路由（POST /readings） | `pg_advisory_xact_lock_shared(站号锁键)` | 共享锁：同站并发写入互不阻塞 |
| 归档（POST /archive） | `pg_advisory_xact_lock(站号锁键)` | 排他锁：与在途写入互斥 |

锁键 = `hashtextextended('hydro-archive-station:' || station_id, 0)`，
**按站隔离**：站 A 的归档不影响站 B 的写入。事务级锁随 COMMIT/ROLLBACK
自动释放，进程崩溃不会残留锁。

正确性论证（对同一站）：

- 写入事务持共享锁期间读到界线 C 并落库。归档要推进界线必须先拿到排他锁，
  即等该写入事务结束：
  - 写入**提交** → 归档的 DELETE（READ COMMITTED 新快照）看到该行，
    若早于新界线则一并迁走 —— **不漏迁**；
  - 写入**回滚** → 无行可迁。
- 归档持排他锁期间，新写入在共享锁上排队；归档提交后写入被唤醒，
  读到的是**新界线**，若观测早于新界线则直接写归档表 —— **不错放**。
- 两个并发归档在排他锁上串行：先到者推进界线；后到者重新校验，
  界线倒退则 409，相同则幂等重放（搬迁 0 行）。
- PostgreSQL 锁队列按 FIFO 授权：排他请求排队后，后续共享请求排在其后，
  归档不会被写入流饿死。

### 3. 等待有界：lock_timeout

归档/写入路由等待按站锁的最长时间为
`HYDRO_ARCHIVE_LOCK_TIMEOUT_MS`（默认 10000ms，事务级 `lock_timeout`）。
超时返回 **409**（提示稍后重试），连接不会无限堆积。

### 4. 跨表查询不重复

`GET /readings` 对两表做 `UNION ALL`（不是 `UNION`，无需去重排序）：

- 一行在任一已提交快照下只存在于一张表（单事务搬迁）；
- 两表 id 共用序列，全局唯一；
- 归档表上有 `idx_readings_archive_station_observed(station_id, observed_at)`，
  与热表索引同形，跨表查询两分支都走索引。

### 5. 与索引构建（CIC）互不干扰

- 归档事务是短事务，只持有行锁与按站咨询锁，不阻塞
  `CREATE INDEX CONCURRENTLY`，也不被其阻塞；
- CIC 只管理热表上的 `idx_readings_station_observed`，归档表索引在
  初始化 DDL 中建好（空表上创建，瞬间完成）；
- 归档期间其他站的写入、查询、批次受理完全不受影响（按站锁隔离）。

## 多实例部署

docker-compose 提供 `api`（8000）与 `api2`（8001）两个实例：

- 归档一致性由**数据库咨询锁**保证，与实例数无关；
- 索引构建 worker 由全局咨询锁 `pg_try_advisory_lock(982451653)`
  保证单实例驱动，另一实例仅提供 API（既有设计）。

## 核对与排障

`sql/03_observe.sql` 提供速查：界线与两表行数核对、热表漏迁校验
（应为 0 行）、两表 id 重复校验（应为 0 行）、按站咨询锁持有情况。
