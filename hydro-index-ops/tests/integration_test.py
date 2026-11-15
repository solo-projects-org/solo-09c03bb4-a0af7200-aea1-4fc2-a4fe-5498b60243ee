"""端到端集成测试：真实 PostgreSQL 16 + 真实 uvicorn 子进程。

前置：
  - PG 实例已就绪，库 hydro_warehouse 已执行 sql/01_init_readings.sql
    （默认连接 127.0.0.1:55432，可用环境变量 HYDRO_TEST_PG_DSN 覆盖）
  - 依赖已安装（fastapi/uvicorn/psycopg/httpx）

运行：python tests/integration_test.py
"""
from __future__ import annotations

import concurrent.futures
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent.parent
BASE = "http://127.0.0.1:8011"
BASE2 = "http://127.0.0.1:8012"
DSN = os.environ.get(
    "HYDRO_TEST_PG_DSN", "postgresql://hydro@127.0.0.1:55432/hydro_warehouse"
)
IDX = "idx_readings_station_observed"
MANUAL_IDX = "idx_readings_ingested_manual"
# 与应用 app/archive.py 中的咨询锁命名空间一致（测试用它从外部持锁制造竞争）
ARCHIVE_LOCK_NS = "hydro-archive-station"

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


class ApiServer:
    def __init__(self, port: int = 8011, extra_env: dict | None = None) -> None:
        self.proc: subprocess.Popen | None = None
        self.port = port
        self.extra_env = extra_env or {}

    def start(self) -> None:
        env = os.environ.copy()
        env["HYDRO_PG_DSN"] = DSN
        env["HYDRO_WORK_POLL_INTERVAL"] = "0.5"
        env["HYDRO_CLEANUP_RETRY_INTERVAL"] = "1.0"
        env.update(self.extra_env)
        base = f"http://127.0.0.1:{self.port}"
        # SIGKILL 后监听端口可能短暂处于未释放状态，重试若干次
        last_out = ""
        for attempt in range(10):
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", str(self.port),
                 "--log-level", "warning"],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True,
            )
            for _ in range(80):
                if self.proc.poll() is not None:  # 进程已退出（多半是端口占用）
                    last_out = self.proc.stdout.read() if self.proc.stdout else ""
                    self.proc = None
                    break
                try:
                    r = httpx.get(f"{base}/health", timeout=1)
                    if r.status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                time.sleep(0.3)
            if self.proc is not None and self.proc.poll() is None:
                return
            time.sleep(1.0)
        raise RuntimeError(f"API did not start. Last output:\n{last_out}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None


async def pg_exec(sql_text: str, params=None):
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    try:
        cur = await conn.execute(sql_text, params)
        try:
            return await cur.fetchall()
        except psycopg.ProgrammingError:
            return []
    finally:
        await conn.close()


async def index_row():
    rows = await pg_exec(
        "SELECT c.relname, ix.indisvalid FROM pg_index ix "
        "JOIN pg_class c ON c.oid = ix.indexrelid WHERE c.relname = %s",
        (IDX,),
    )
    return rows[0] if rows else None


async def backend_alive(pid: int) -> bool:
    rows = await pg_exec("SELECT 1 FROM pg_stat_activity WHERE pid = %s", (pid,))
    return bool(rows)


def http(method: str, path: str, base: str = BASE, **kw):
    r = httpx.request(method, f"{base}{path}", timeout=15, **kw)
    return r


def wait_batch(bid: int, *states: str, timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = http("GET", f"/index-batches/{bid}").json()
        if last["state"] in states:
            return last
        time.sleep(0.5)
    raise AssertionError(f"batch {bid} never reached {states}, last={last}")


async def drop_managed() -> None:
    await pg_exec(f"DROP INDEX CONCURRENTLY IF EXISTS {IDX}")


async def main() -> int:
    import asyncio
    # ------------------------------------------------------------------ #
    print("场景 0：启动与健康检查（含 Swagger/OpenAPI 文档）")
    srv = ApiServer()
    srv.start()
    h = http("GET", "/health").json()
    check("health ok", h["status"] == "ok")
    check("初始无活动批次、可受理", h["active_batch"] is None and h["accepting_new_batch"])
    r = http("GET", "/openapi.json")
    check("OpenAPI schema 可生成（Swagger 可用）",
          r.status_code == 200 and r.json()["info"]["title"].startswith("水文数仓"))
    check("Swagger UI 可访问", http("GET", "/docs").status_code == 200)

    # ------------------------------------------------------------------ #
    print("场景 1：正常构建（构建中读写照常）→ SUCCEEDED，执行计划走索引")
    await drop_managed()
    bid = http("POST", "/index-batches", json={"note": "e2e-1"}).json()["id"]
    phases_seen: set[str] = set()
    deadline = time.time() + 30
    last = {}
    while time.time() < deadline:
        last = http("GET", f"/index-batches/{bid}").json()
        if last["phase"]:
            phases_seen.add(last["phase"])
        if last["state"] in ("SUCCEEDED", "FAILED"):
            break
        # 构建期间持续写入与查询
        http("POST", "/readings", json={
            "station_id": 1,
            "observed_at": "2026-11-14T08:00:00Z",
            "water_level": 22.5,
        }).raise_for_status()
        http("GET", "/readings?station_id=1&limit=3").raise_for_status()
        time.sleep(0.2)
    check("批次 SUCCEEDED", last["state"] == "SUCCEEDED", str(last))
    check("有阶段上报", len(phases_seen) > 0, str(phases_seen))
    row = await index_row()
    check("管理索引存在且 valid", row is not None and row[1] is True, str(row))
    plan = http("GET", "/readings/explain?station_id=3").json()["plan"]
    plan_txt = str(plan)
    check("执行计划使用复合索引", IDX in plan_txt, plan_txt[:200])

    # 重复受理被拒
    r = http("POST", "/index-batches", json={"note": "dup"})
    check("索引 valid 时重复受理 409", r.status_code == 409)

    # ------------------------------------------------------------------ #
    print("场景 2：长事务等待 → 取消 → 残件删净 → CANCELLED → 可接新批")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=180").json()
    lpid = ltx["pid"]
    bid2 = http("POST", "/index-batches", json={"note": "e2e-2"}).json()["id"]
    b = wait_batch(bid2, "BUILDING")
    check("记录构建 pid", isinstance(b["build_pid"], int))
    # 观察到等待事件
    waited = False
    for _ in range(10):
        b = http("GET", f"/index-batches/{bid2}").json()
        if b["phase"] and "wait: Lock" in b["phase"]:
            waited = True
            break
        time.sleep(0.5)
    check("CIC 停在锁等待阶段", waited, str(b))
    # 等待期间源表读写正常
    check("等待中写入正常",
          http("POST", "/readings", json={
              "station_id": 2, "observed_at": "2026-11-14T09:00:00Z",
              "water_level": 18.0}).status_code == 201)
    r = http("POST", "/index-batches", json={"note": "while active"})
    check("活动批次存在时新受理 409", r.status_code == 409)

    cr = http("POST", f"/index-batches/{bid2}/cancel")
    check("cancel 后状态 CANCELLING", cr.json()["state"] == "CANCELLING")
    # 释放长事务，清理才能完成 DROP CONCURRENTLY
    http("DELETE", f"/demo/long-tx/{lpid}").raise_for_status()
    b = wait_batch(bid2, "CANCELLED")
    check("批次 CANCELLED", b["state"] == "CANCELLED")
    check("cancel_requested 已置位", b["cancel_requested"] is True)
    check("取消后无索引残留（含迟到成功）", await index_row() is None)
    check("长事务之外没有误杀：长事务已按请求结束", not await backend_alive(lpid))
    # 终态后可受理新批
    bid2b = http("POST", "/index-batches", json={"note": "e2e-2b"}).json()["id"]
    b = wait_batch(bid2b, "SUCCEEDED")
    check("清理后新批次可成功", b["state"] == "SUCCEEDED")

    # ------------------------------------------------------------------ #
    print("场景 3：清理失败保留 CLEANING 并自动重试（构建前取消，无残件也走清理）")
    srv.stop()
    srv = ApiServer(extra_env={"HYDRO_CLEANUP_FAIL_FIRST": "4"})
    srv.start()
    await drop_managed()
    # 受理后立即取消：批次从未进入构建，无会话/残件，但仍必须走完整清理流程
    bid3 = http("POST", "/index-batches", json={"note": "e2e-3"}).json()["id"]
    http("POST", f"/index-batches/{bid3}/cancel").raise_for_status()
    saw_cleaning = False
    saw_injected_error = False
    max_attempts = 0
    deadline = time.time() + 60
    b = {}
    while time.time() < deadline:
        b = http("GET", f"/index-batches/{bid3}").json()
        saw_cleaning = saw_cleaning or b["state"] == "CLEANING"
        saw_injected_error = saw_injected_error or (
            b["error"] is not None and "injected cleanup failure" in b["error"]
        )
        max_attempts = max(max_attempts, b["cleanup_attempts"])
        # 重试窗口内 API 健康检查与源表读写必须照常
        check_once = http("GET", "/health").status_code == 200
        if not check_once:
            check("重试期间 API 照常", False)
        if b["state"] == "CANCELLED":
            break
        time.sleep(0.3)
    check("经历过 CLEANING 待处理状态", saw_cleaning, str(b))
    check("观察到注入的清理失败错误", saw_injected_error, str(b))
    check("cleanup_attempts 达到注入次数 4", max_attempts >= 4, str(max_attempts))
    check("重试后终态 CANCELLED", b["state"] == "CANCELLED", str(b))
    check("终态保留最后错误信息便于排障", b.get("error") is not None, str(b.get("error")))
    check("重试期间状态查询始终可用", True)
    check("残件最终删净", await index_row() is None)

    # 恢复默认配置
    srv.stop()
    srv = ApiServer()
    srv.start()

    # ------------------------------------------------------------------ #
    print("场景 4a：重启恢复——构建等待中崩溃，重启后终止遗留构建 → FAILED")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=300").json()
    lpid = ltx["pid"]
    bid4 = http("POST", "/index-batches", json={"note": "e2e-4a"}).json()["id"]
    b = wait_batch(bid4, "BUILDING")
    build_pid = b["build_pid"]
    check("构建会话存活", await backend_alive(build_pid))
    # SIGKILL 模拟进程崩溃/容器重启；长事务保持打开，CIC 停在等待阶段、产不出索引
    srv.stop(sig=signal.SIGKILL)
    check("API 死后构建会话仍残留（等待恢复处理）", await backend_alive(build_pid))
    check("长事务会话未受影响（不得误杀其他会话）", await backend_alive(lpid))
    srv = ApiServer()
    srv.start()
    # 恢复在后台进行：先确认孤儿构建被身份校验后的终止逻辑干掉，而长事务不动
    deadline = time.time() + 30
    while time.time() < deadline and await backend_alive(build_pid):
        time.sleep(0.3)
    check("恢复终止了遗留构建会话", not await backend_alive(build_pid))
    check("恢复未误杀长事务（其他会话不受影响）", await backend_alive(lpid))
    # 此时 DROP 被长事务挡在 CLEANING；结束长事务后清理才能完成 → FAILED
    b = wait_batch(bid4, "CLEANING", timeout=30)
    check("长事务未结束前停在 CLEANING 待处理", b["state"] == "CLEANING", str(b))
    await pg_exec(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid = %s AND application_name LIKE 'hydro-demo-longtx-%%'",
        (lpid,),
    )
    b = wait_batch(bid4, "FAILED", timeout=30)
    check("恢复后遗留批次 FAILED", b["state"] == "FAILED")
    check("无索引残件", await index_row() is None)

    print("场景 4b：重启恢复——孤儿构建在重启前迟到成功 → 采纳并保留索引 → SUCCEEDED")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=300").json()
    lpid = ltx["pid"]
    bid4b = http("POST", "/index-batches", json={"note": "e2e-4b"}).json()["id"]
    wait_batch(bid4b, "BUILDING")
    srv.stop(sig=signal.SIGKILL)
    # API 死后释放长事务：孤儿 CIC 解除阻塞，在新 API 恢复前自行构建完成
    await pg_exec(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid = %s AND application_name LIKE 'hydro-demo-longtx-%%'",
        (lpid,),
    )
    # 等孤儿构建把索引建为 valid
    deadline = time.time() + 30
    while time.time() < deadline:
        row = await index_row()
        if row and row[1] is True:
            break
        time.sleep(0.5)
    check("孤儿构建迟到成功、索引 valid", row is not None and row[1] is True, str(row))
    srv = ApiServer()
    srv.start()
    b = wait_batch(bid4b, "SUCCEEDED", timeout=45)
    check("恢复采纳迟到成功为 SUCCEEDED", b["state"] == "SUCCEEDED", str(b))
    check("成功索引被保留", await index_row() is not None)

    print("场景 5：成功索引与人工索引在重启后保留")
    await drop_managed()
    bid5 = http("POST", "/index-batches", json={"note": "e2e-5"}).json()["id"]
    wait_batch(bid5, "SUCCEEDED")
    srv.stop()
    srv = ApiServer()
    srv.start()
    h = http("GET", "/health").json()
    check("重启后索引仍 valid", h["index_status"] == "valid")
    rows = await pg_exec(
        "SELECT indexname FROM pg_indexes WHERE tablename='readings' ORDER BY 1"
    )
    names = [r[0] for r in rows]
    check("管理索引保留", IDX in names, str(names))
    check("人工索引始终未被删除", MANUAL_IDX in names, str(names))

    # 批次历史持久化
    batches = http("GET", "/index-batches?limit=20").json()
    check("批次历史完整持久化", len(batches) >= 5)
    states = {x["state"] for x in batches}
    check("历史覆盖 SUCCEEDED/CANCELLED/FAILED",
          {"SUCCEEDED", "CANCELLED", "FAILED"}.issubset(states), str(states))

    # ------------------------------------------------------------------ #
    print("场景 6：按站归档——界线只前进、重复提交幂等、原 ID 保留")
    # 站 6 种子数据按时间排序取 1/4 与中位时刻作为两条界线
    rows = await pg_exec(
        "SELECT observed_at FROM readings WHERE station_id=6 ORDER BY observed_at"
    )
    ts = [r[0] for r in rows]
    cut1 = ts[2500].isoformat()  # 恰好 2500 行更早
    cut2 = ts[5000].isoformat()  # 恰好 5000 行更早

    before = await pg_exec(
        "SELECT id FROM readings WHERE station_id=6 AND observed_at < %s", (cut1,)
    )
    r = http("POST", "/archive", json={"station_id": 6, "cutoff": cut1})
    check("首次归档 200", r.status_code == 200, r.text)
    d = r.json()
    check("搬迁行数=2500", d["moved_count"] == 2500, str(d))
    check("首次归档非重放、无前界线",
          d["idempotent_replay"] is False and d["previous_cutoff"] is None, str(d))
    after = await pg_exec("SELECT id FROM readings_archive WHERE station_id=6")
    check("归档行连原 ID 迁移",
          sorted(x[0] for x in before) == sorted(x[0] for x in after))

    r = http("POST", "/archive", json={"station_id": 6, "cutoff": cut1})
    d = r.json()
    check("重复提交 200 且幂等（不再搬迁）",
          r.status_code == 200 and d["moved_count"] == 0 and d["idempotent_replay"] is True,
          str(d))

    r = http("POST", "/archive", json={"station_id": 6, "cutoff": ts[1000].isoformat()})
    check("界线倒退被拒 409", r.status_code == 409, r.text)

    r = http("POST", "/archive", json={"station_id": 6, "cutoff": cut2})
    d = r.json()
    check("界线前进继续搬迁 2500 行",
          r.status_code == 200 and d["moved_count"] == 2500, str(d))

    b = http("GET", "/archive/boundaries/6").json()
    check("界线详情行数守恒（hot+archive=10000）",
          b["hot_rows"] + b["archived_rows"] == 10000, str(b))
    check("界线列表可查",
          any(x["station_id"] == 6 for x in http("GET", "/archive/boundaries").json()))
    dup = await pg_exec(
        "SELECT count(*) FROM readings r JOIN readings_archive a ON a.id=r.id "
        "WHERE r.station_id=6"
    )
    check("两表 id 无交集", dup[0][0] == 0)
    leaked = await pg_exec(
        "SELECT count(*) FROM readings WHERE station_id=6 AND observed_at < %s", (cut2,)
    )
    check("热表无早于界线的记录", leaked[0][0] == 0)

    r = http("POST", "/archive", json={"station_id": 999, "cutoff": cut1})
    check("未知站号 404", r.status_code == 404, r.text)
    r = http("POST", "/archive", json={"station_id": 6, "cutoff": "2026-01-01T00:00:00"})
    check("cutoff 不带时区 422", r.status_code == 422, r.text)
    r = http("GET", "/archive/boundaries/999")
    check("无界线站查询 404", r.status_code == 404)

    # ------------------------------------------------------------------ #
    print("场景 7：迟到旧观测直写归档表；按站时查询跨两表完整不重复")
    rows = await pg_exec(
        "SELECT observed_at FROM readings WHERE station_id=7 ORDER BY observed_at"
    )
    cut7 = rows[5000][0].isoformat()
    r = http("POST", "/archive", json={"station_id": 7, "cutoff": cut7})
    check("站7归档 5000 行", r.json()["moved_count"] == 5000, r.text)

    r = http("POST", "/readings", json={
        "station_id": 7, "observed_at": rows[100][0].isoformat(), "water_level": 9.9})
    check("迟到旧观测写入归档表",
          r.status_code == 201 and r.json()["source"] == "archive"
          and r.json()["archived_at"] is not None, r.text)
    r = http("POST", "/readings", json={
        "station_id": 7, "observed_at": "2099-01-01T00:00:00Z", "water_level": 1.1})
    check("新观测写入热表", r.status_code == 201 and r.json()["source"] == "hot")
    r = http("POST", "/readings", json={
        "station_id": 7, "observed_at": cut7, "water_level": 2.2})
    check("等于界线的观测仍写热表（界线为严格小于）",
          r.status_code == 201 and r.json()["source"] == "hot", r.text)

    # 跨界线窗口查询：完整、不重复、来源与界线一致
    from datetime import datetime as _dt
    start, end = rows[4990][0].isoformat(), rows[5010][0].isoformat()
    got = http("GET", "/readings", params={
        "station_id": 7, "start": start, "end": end, "limit": 1000}).json()
    ids = [x["id"] for x in got]
    check("跨界线查询无重复 id", len(ids) == len(set(ids)), str(len(ids)))
    check("跨界线查询同时覆盖两表",
          {x["source"] for x in got} == {"hot", "archive"},
          str({x["source"] for x in got}))
    cut7_dt = _dt.fromisoformat(cut7)
    ok_split = all(
        (x["source"] == "archive") == (_dt.fromisoformat(x["observed_at"]) < cut7_dt)
        for x in got
    )
    check("查询结果来源与界线一致", ok_split)
    b = http("GET", "/archive/boundaries/7").json()
    check("站7行数守恒（含 3 条新写入）",
          b["hot_rows"] + b["archived_rows"] == 10000 + 3, str(b))

    # ------------------------------------------------------------------ #
    print("场景 8：两个 API 实例并发写入与推进界线——不漏迁、不错放")
    srv2 = ApiServer(port=8012)
    srv2.start()
    check("第二实例就绪（worker 咨询锁保证仅一个实例驱动状态机）",
          http("GET", "/health", base=BASE2).status_code == 200)

    async def reset_station(st: int) -> None:
        await pg_exec("DELETE FROM readings WHERE station_id=%s", (st,))
        await pg_exec("DELETE FROM readings_archive WHERE station_id=%s", (st,))
        await pg_exec("DELETE FROM archive_boundaries WHERE station_id=%s", (st,))

    async def station_counts(st: int) -> tuple[int, int, int]:
        hot = (await pg_exec("SELECT count(*) FROM readings WHERE station_id=%s", (st,)))[0][0]
        arc = (await pg_exec("SELECT count(*) FROM readings_archive WHERE station_id=%s", (st,)))[0][0]
        dup = (await pg_exec(
            "SELECT count(*) FROM readings r JOIN readings_archive a ON a.id=r.id "
            "WHERE r.station_id=%s", (st,)))[0][0]
        return hot, arc, dup

    # 8a 自由竞速：实例1 跨界线写入 40 条，实例2 同时推进界线
    await reset_station(8)
    CUT8 = "2026-09-01T00:00:00Z"
    inserted: list[tuple[int, str]] = []
    errors: list[str] = []

    def insert_one(i: int) -> None:
        ts = (f"2026-08-{10 + i % 20:02d}T{i % 24:02d}:00:00Z" if i % 2 == 0
              else f"2026-09-{10 + i % 20:02d}T{i % 24:02d}:00:00Z")
        try:
            rr = httpx.post(f"{BASE}/readings", json={
                "station_id": 8, "observed_at": ts, "water_level": float(i)}, timeout=30)
            if rr.status_code == 201:
                inserted.append((rr.json()["id"], ts))
            else:
                errors.append(f"insert {i}: {rr.status_code}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"insert {i}: {exc}")

    def archive_one() -> None:
        rr = httpx.post(f"{BASE2}/archive",
                        json={"station_id": 8, "cutoff": CUT8}, timeout=30)
        if rr.status_code != 200:
            errors.append(f"archive: {rr.status_code} {rr.text[:80]}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as ex:
        futs = [ex.submit(insert_one, i) for i in range(40)]
        futs.insert(20, ex.submit(archive_one))
        for f in futs:
            f.result()
    check("竞速期间无请求失败", not errors, str(errors[:3]))
    check("40 条写入全部受理", len(inserted) == 40, str(len(inserted)))
    loc = {r[0]: r[1] for r in await pg_exec(
        "SELECT id, 'hot' FROM readings WHERE station_id=8 "
        "UNION ALL SELECT id, 'arc' FROM readings_archive WHERE station_id=8")}
    misplaced = [(rid, ts) for rid, ts in inserted
                 if loc.get(rid) != ("arc" if ts < CUT8 else "hot")]
    check("每条观测按最终界线落位正确（不错放）", not misplaced, str(misplaced[:3]))
    hot, arc, dup = await station_counts(8)
    check("行数守恒且无重复 id（不漏迁）",
          hot + arc == 40 and dup == 0, f"hot={hot} arc={arc} dup={dup}")
    leaked = (await pg_exec(
        "SELECT count(*) FROM readings WHERE station_id=8 AND observed_at < %s",
        (CUT8,)))[0][0]
    check("热表无漏迁旧记录", leaked == 0)

    # 8b 两实例并发不同 cutoff：终态界线=较大者
    await reset_station(8)
    for i in range(10):
        http("POST", "/readings", json={"station_id": 8,
             "observed_at": f"2026-01-{i + 1:02d}T00:00:00Z", "water_level": 1.0})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(httpx.post, f"{BASE}/archive",
                       json={"station_id": 8, "cutoff": "2026-01-05T00:00:00Z"}, timeout=30)
        f2 = ex.submit(httpx.post, f"{BASE2}/archive",
                       json={"station_id": 8, "cutoff": "2026-01-08T00:00:00Z"}, timeout=30)
        r1, r2 = f1.result(), f2.result()
    codes = sorted([r1.status_code, r2.status_code])
    check("并发不同界线：双双成功或落后者 409", codes in ([200, 200], [200, 409]), str(codes))
    b = http("GET", "/archive/boundaries/8").json()
    check("终态界线为较大 cutoff", b["cutoff"].startswith("2026-01-08T00:00:00"), str(b))
    hot, arc, dup = await station_counts(8)
    check("界线以下的 7 行全部且仅一次归档", arc == 7 and dup == 0, f"arc={arc} dup={dup}")

    # 8c 两实例并发相同 cutoff：幂等，合计只迁一次
    await reset_station(8)
    for i in range(6):
        http("POST", "/readings", json={"station_id": 8,
             "observed_at": f"2026-02-{i + 1:02d}T00:00:00Z", "water_level": 1.0})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(httpx.post, f"{BASE}/archive",
                       json={"station_id": 8, "cutoff": "2026-02-10T00:00:00Z"}, timeout=30)
        f2 = ex.submit(httpx.post, f"{BASE2}/archive",
                       json={"station_id": 8, "cutoff": "2026-02-10T00:00:00Z"}, timeout=30)
        r1, r2 = f1.result(), f2.result()
    moved_total = r1.json()["moved_count"] + r2.json()["moved_count"]
    replays = int(r1.json()["idempotent_replay"]) + int(r2.json()["idempotent_replay"])
    check("并发相同界线：合计只迁 6 行", moved_total == 6, str(moved_total))
    check("恰有一个请求为幂等重放", replays == 1, str(replays))
    await reset_station(8)

    # ------------------------------------------------------------------ #
    print("场景 9：归档失败/服务重启——数据与界线保持同一已提交状态")
    # 9a 注入归档表写入失败 → 归档 500 且整体回滚
    await pg_exec(
        "CREATE OR REPLACE FUNCTION hydro_test_fail() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'injected archive failure'; END; $$"
    )
    await pg_exec(
        "CREATE TRIGGER trg_hydro_test_fail BEFORE INSERT ON readings_archive "
        "FOR EACH ROW EXECUTE FUNCTION hydro_test_fail()"
    )
    r = http("POST", "/archive",
             json={"station_id": 9, "cutoff": "2026-06-01T00:00:00Z"})
    check("归档中途失败返回 500", r.status_code == 500, r.text)
    hot, arc, _ = await station_counts(9)
    bnd = await pg_exec("SELECT count(*) FROM archive_boundaries WHERE station_id=9")
    check("失败后数据与界线保持原状（整体回滚）",
          hot == 10000 and arc == 0 and bnd[0][0] == 0, f"hot={hot} arc={arc}")
    await pg_exec("DROP TRIGGER trg_hydro_test_fail ON readings_archive")
    await pg_exec("DROP FUNCTION hydro_test_fail()")
    r = http("POST", "/archive",
             json={"station_id": 9, "cutoff": "2026-06-01T00:00:00Z"})
    check("故障解除后重试成功", r.status_code == 200 and r.json()["moved_count"] > 0, r.text)

    # 9b 归档被按站锁挡住时 SIGKILL：状态不变；重启后重试成功
    lock_conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    await lock_conn.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
        (f"{ARCHIVE_LOCK_NS}:10",),
    )
    outcome: dict = {}

    def blocked_archive() -> None:
        try:
            outcome["r"] = httpx.post(
                f"{BASE}/archive",
                json={"station_id": 10, "cutoff": "2026-06-01T00:00:00Z"}, timeout=30)
        except Exception as exc:  # noqa: BLE001  服务被杀，连接断开
            outcome["err"] = exc

    t = threading.Thread(target=blocked_archive)
    t.start()
    time.sleep(2)
    check("归档请求被按站锁阻塞", not outcome)
    # 阻塞期间其他站写入照常（按站隔离）
    t0 = time.time()
    r = http("POST", "/readings", json={
        "station_id": 3, "observed_at": "2026-11-15T01:00:00Z", "water_level": 2.2})
    check("站10归档阻塞期间其他站写入照常",
          r.status_code == 201 and time.time() - t0 < 5, r.text)

    srv.stop(sig=signal.SIGKILL)  # 模拟服务崩溃/容器重启
    t.join(timeout=15)
    check("崩溃后归档请求已断开", "err" in outcome or
          (outcome.get("r") is not None and outcome["r"].status_code >= 500))
    hot, arc, _ = await station_counts(10)
    bnd = await pg_exec("SELECT count(*) FROM archive_boundaries WHERE station_id=10")
    check("SIGKILL 后数据与界线保持同一已提交状态",
          hot == 10000 and arc == 0 and bnd[0][0] == 0, f"hot={hot} arc={arc}")
    await lock_conn.execute(
        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
        (f"{ARCHIVE_LOCK_NS}:10",),
    )
    await lock_conn.close()

    srv = ApiServer()
    srv.start()
    r = http("POST", "/archive",
             json={"station_id": 10, "cutoff": "2026-06-01T00:00:00Z"})
    check("重启后重试归档成功", r.status_code == 200 and r.json()["moved_count"] > 0, r.text)
    hot, arc, dup = await station_counts(10)
    bnd = await pg_exec("SELECT cutoff FROM archive_boundaries WHERE station_id=10")
    check("重启后数据与界线一致（hot+archive=10000，界线已推进）",
          hot + arc == 10000 and dup == 0 and bnd and bnd[0][0] is not None,
          f"hot={hot} arc={arc}")

    srv2.stop()
    srv.stop()
    print(f"\n结果：{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))
