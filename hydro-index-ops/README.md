# 水文数仓 · 水位明细索引运维 + 按站归档服务

在持续接收观测的 PostgreSQL `readings` 水位明细表上，提供两类运维能力：

1. **索引构建**：以 **`CREATE INDEX CONCURRENTLY`** 构建普通复合索引
   `(station_id, observed_at)`，通过 FastAPI + psycopg3 提供
   **单批次受理、阶段查询、取消、失败/取消清理、重启恢复** 的完整运维闭环。
2. **按站归档**：按 **站号 + UTC 截止时刻** 把旧水位从热表搬到归档表
   `readings_archive`（连原 ID），缩小日常入库表；界线只许前进、
   重复提交幂等；迟到旧观测自动直写归档表；按站时查询跨两表完整返回。

## 为什么这样设计

- `CREATE INDEX CONCURRENTLY`（CIC）不持有阻止源表写入的长锁，构建期间
  `readings` 的普通读写照常；代价是失败后会留下 `indisvalid=false` 的索引残件，
  必须显式 `DROP INDEX CONCURRENTLY`，且 CIC 不能跑在事务块里。
- 因此系统用**持久化状态机 + 独立 autocommit 构建会话**驱动批次，保证：
  取消/失败后必须等构建会话结束、残件删净才进入终态，之后才接收新批；
  清理失败保持待处理状态自动重试；重启后先恢复再受理。
- 归档则用**单事务搬迁 + 按站咨询锁读写锁**保证：搬迁与界线推进同一事务
  提交（失败/重启整体回滚，数据与界线永远一致）；写入取共享锁、归档取
  排他锁，两个 API 实例并发下观测不漏迁、不错放，其他站写照常。
  详见 `docs/archive.md`。

## 目录结构

```
hydro-index-ops/
├── docker-compose.yml          # db(postgres:16) + api + api2（双实例）
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py                 # FastAPI 路由（Swagger: /docs）
│   ├── worker.py               # 批次状态机：构建/取消/清理/恢复
│   ├── archive.py              # 按站归档：界线推进、写入路由、跨表查询
│   ├── sql_helpers.py          # 索引状态、CIC 进度、会话终止、批次 CRUD
│   ├── db.py                   # psycopg 异步连接池 + 批次/归档表 DDL
│   ├── models.py               # Pydantic 模型
│   └── config.py               # 环境变量配置
├── sql/
│   ├── 01_init_readings.sql    # 水位样表 + 归档表 + 10 万行样例数据 + 人工索引
│   ├── 02_long_tx.sql          # 长事务样例（制造 CIC 构建等待）
│   └── 03_observe.sql          # 排障观察 SQL 速查（含归档核对）
├── demo/
│   ├── demo.sh                 # 端到端演示场景（索引 1~4 + 归档 5）
│   └── reset_index.sql
└── docs/
    ├── state-machine.md        # 索引批次状态机与关键保证说明
    └── archive.md              # 归档设计、一致性与并发保证
```

## 快速启动

```bash
cd hydro-index-ops
docker compose up -d --build
# 等待 healthcheck 通过后：
open http://localhost:8000/docs          # Swagger UI
curl -s http://localhost:8000/health | python3 -m json.tool
```

首次启动会自动建表并写入 10 个测站 × 1 万条/站 = 10 万行水位样例数据，
并创建一个**人工索引** `idx_readings_ingested_manual`，用于验证清理/恢复流程
绝不会误删非本系统创建的索引。

## API 一览（Swagger 中有完整 schema 与示例）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/health` | 健康检查、管理索引状态、是否可受理新批 |
| POST | `/index-batches` | 受理一个构建批次（活动批次存在或索引已 valid 时 409） |
| GET  | `/index-batches` | 批次历史（状态全部持久化在 `index_build_batches` 表） |
| GET  | `/index-batches/{id}` | 查状态/构建阶段（pg_stat_progress_create_index）/错误 |
| POST | `/index-batches/{id}/cancel` | 请求取消，终态前不允许再受理 |
| POST | `/readings` | 写入水位观测；**晚于归档界线的迟到观测自动直写归档表** |
| GET  | `/readings?station_id=&start=&end=` | 按站号+时间查询，**跨热表+归档表完整返回，不重复** |
| GET  | `/readings/explain?station_id=` | EXPLAIN 验证复合索引是否生效 |
| POST | `/archive` | 按站号+UTC 截止时刻归档：早于界线的热表观测连原 ID 迁入归档表 |
| GET  | `/archive/boundaries` | 全部测站的归档界线 |
| GET  | `/archive/boundaries/{station_id}` | 单站界线 + 热表/归档表行数（核对不漏数） |
| POST | `/demo/long-tx?seconds=` | 开长事务制造 CIC 构建等待 |
| DELETE | `/demo/long-tx/{pid}` | 结束演示长事务（只认 `hydro-demo-longtx-*` 会话） |

compose 同时启动 **两个 API 实例**（`api`:8000、`api2`:8001）：
归档一致性由数据库按站咨询锁保证，多实例并发写入/推进界线不漏迁、不错放；
索引构建 worker 由全局咨询锁保证单实例驱动。

## 按站归档（详见 docs/archive.md）

```bash
# 把站 1 早于 2026-05-01T00:00:00Z 的观测迁入归档表（连原 ID）
curl -X POST http://localhost:8000/archive \
  -H 'Content-Type: application/json' \
  -d '{"station_id": 1, "cutoff": "2026-05-01T00:00:00Z"}'
# → {"moved_count": 2160, "idempotent_replay": false, ...}
# 重复同一提交：moved_count=0, idempotent_replay=true（幂等，不重复搬迁）
# cutoff 落后于当前界线：409（界线只许前进）

curl http://localhost:8000/archive/boundaries/1   # 界线 + 热表/归档表行数核对
```

- **搬迁与界线推进同一事务提交**：归档失败或服务重启时整体回滚，
  数据与界线永远保持同一已提交状态，重试安全。
- **迟到观测直写归档**：界线生效后，`POST /readings` 收到
  `observed_at < 界线` 的观测直接写入归档表（响应 `source: "archive"`）。
- **跨表查询完整不重复**：`GET /readings` 对两表 `UNION ALL`；
  两表 id 共用同一序列，全局唯一。
- **多实例安全**：写入取按站共享咨询锁、归档取排他锁；
  两个 API 实例并发下观测不漏迁、不错放，其他站写入与索引构建照常。

## 演示场景

```bash
chmod +x demo/demo.sh
./demo/demo.sh all       # 启动并依次跑场景 1~5
# 或单独执行：./demo/demo.sh 1 | 2 | 3 | 4 | 5
```

1. **构建等待 → 成功**：先开持有 `readings` 快照的长事务，再受理批次；
   观察 CIC 停在等待阶段（`wait=Lock...`），期间读写照常；结束长事务后
   构建完成，EXPLAIN 显示走 `idx_readings_station_observed`。
2. **构建中取消**：等待阶段发起 cancel，状态经 `CANCELLING → CLEANING → CANCELLED`；
   期间受理新批返回 409；残件删净后才能受理新批；取消后的迟到完成同样删索引、
   绝不报成功。
3. **清理失败自动重试**：以 `HYDRO_CLEANUP_FAIL_FIRST=2` 注入前两次 DROP 失败
   （compose override 注入），观察批次停留在 `CLEANING`、`cleanup_attempts`
   递增、错误信息可查，源表读写与状态查询全程正常，重试成功后自动进入终态。
4. **重启恢复**：构建等待中 `docker compose restart api`；恢复在后台执行、
   不阻塞 API 就绪，先终止遗留构建会话（按 pid + application_name 双重校验，
   不断其他会话）、清净残件再进入终态；另演示孤儿构建迟到成功时索引被采纳
   保留，以及成功索引与人工索引在重启后均不受影响。
5. **按站归档**：归档搬迁与界线推进、重复提交幂等、界线倒退 409、
   迟到观测直写归档、跨两表查询完整不重复，并演示经第二个 API 实例（8001）
   并发推进界线。

手动排障见 `sql/03_observe.sql`：

```bash
docker exec -it hydro-db psql -U hydro -d hydro_warehouse
```

## 关键安全边界

- **只管理一个固定名称的索引** `idx_readings_station_observed`，所有
  DROP 都带该名称且 `IF EXISTS`，不触碰任何人工索引。
- **只终止自有构建会话**：`pg_cancel_backend` / `pg_terminate_backend`
  均带 `application_name LIKE 'hydro-cic-builder-%'` 双重身份校验。
- 批次表上有部分唯一索引，数据库层面兜底“同一时刻只有一个活动批次”。
- worker 用 PostgreSQL 咨询锁保证多副本部署时只有一个实例驱动状态机；
  锁由专用长连接持有并在同一会话释放。
- 状态全部落库（`index_build_batches`），进程重启不丢状态。
- 归档搬迁与界线推进在**同一事务**提交；按站咨询锁（写入共享/归档排他）
  保证多实例并发下不漏迁、不错放，且只影响目标测站。

## 配置项（api 环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HYDRO_PG_DSN` | 指向 compose 内 db | PostgreSQL 连接串 |
| `HYDRO_WORK_POLL_INTERVAL` | `1.0` | 调度/阶段刷新间隔（秒） |
| `HYDRO_CLEANUP_RETRY_INTERVAL` | `3.0` | 清理失败重试间隔（秒） |
| `HYDRO_CLEANUP_FAIL_FIRST` | `0` | 演示用：前 N 次清理故意失败，生产必须为 0 |
| `HYDRO_ARCHIVE_LOCK_TIMEOUT_MS` | `10000` | 归档/写入路由等待按站咨询锁的最长时间（毫秒），超时 409 可重试 |
| `HYDRO_LOG_LEVEL` | `INFO` | 日志级别 |

## 停止与清理

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 连同样例数据卷一并删除
docker exec -i hydro-db psql -U hydro -d hydro_warehouse < demo/reset_index.sql
```

## 集成测试（真实 PostgreSQL，非模拟）

`tests/integration_test.py` 启动**真实 PG 实例 + 真实 uvicorn 子进程**
（归档场景再拉起第二个实例），覆盖：索引构建的正常构建与执行计划、长事务
等待、取消与残件清理、清理失败自动重试、SIGKILL 崩溃后的后台恢复（含孤儿
构建迟到成功的采纳）、成功/人工索引保留、批次持久化；以及归档的界线只前进、
重复提交幂等、原 ID 保留、迟到观测直写归档、跨表查询完整不重复、**两实例
并发写入与推进界线不漏迁/不错放**、归档失败与 SIGKILL 重启后数据与界线
保持同一已提交状态、其他站写入隔离等。

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt httpx
# 准备一个可用的 PostgreSQL，并执行 sql/01_init_readings.sql，然后：
HYDRO_TEST_PG_DSN=postgresql://user:pass@host:5432/hydro_warehouse \
  python tests/integration_test.py
```

测试默认连接 `127.0.0.1:55432/hydro_warehouse`（用 `HYDRO_TEST_PG_DSN` 覆盖）。

