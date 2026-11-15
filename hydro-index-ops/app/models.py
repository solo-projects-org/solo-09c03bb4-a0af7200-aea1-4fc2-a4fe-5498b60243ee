"""Pydantic 出入参模型。"""
from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class BatchCreate(BaseModel):
    note: str | None = Field(default=None, max_length=500, description="备注（可选）")


class BatchOut(BaseModel):
    id: int
    state: str
    cancel_requested: bool
    build_pid: int | None
    phase: str | None
    error: str | None
    cleanup_attempts: int
    created_at: datetime
    updated_at: datetime


class ReadingIn(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    observed_at: datetime = Field(
        description="观测时间（建议带时区；不带时区按 UTC 处理）"
    )
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")

    @field_validator("observed_at")
    @classmethod
    def _naive_means_utc(cls, v: datetime) -> datetime:
        # 数据库会话时区为 UTC，朴素时间按 UTC 解释，与落库语义保持一致，
        # 保证“是否早于归档界线”的路由判断与库存储比较结果一致。
        if v.tzinfo is None or v.utcoffset() is None:
            return v.replace(tzinfo=timezone.utc)
        return v


class ReadingOut(ReadingIn):
    id: int
    ingested_at: datetime
    source: str | None = Field(
        default=None, description="记录所在表：hot=热表 / archive=归档表"
    )
    archived_at: datetime | None = Field(
        default=None, description="归档时间（热表记录为 null）"
    )


class ArchiveRequest(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    cutoff: datetime = Field(
        description="UTC 截止时刻（必须带时区）：早于该时刻的观测迁入归档表"
    )

    @field_validator("cutoff")
    @classmethod
    def _require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError(
                "cutoff 必须携带时区（建议 UTC，如 2026-01-01T00:00:00Z）"
            )
        return v


class ArchiveOut(BaseModel):
    station_id: int
    cutoff: datetime = Field(description="本次生效的归档界线")
    previous_cutoff: datetime | None = Field(description="之前的界线（首次归档为 null）")
    moved_count: int = Field(description="本次从热表迁入归档表的行数")
    idempotent_replay: bool = Field(
        description="是否重复提交（界线未推进，仅幂等兜底扫描）"
    )
    executed_at: datetime


class BoundaryOut(BaseModel):
    station_id: int
    cutoff: datetime
    updated_at: datetime
    hot_rows: int | None = Field(default=None, description="热表当前行数（仅详情接口）")
    archived_rows: int | None = Field(default=None, description="归档表当前行数（仅详情接口）")


class LongTxOut(BaseModel):
    pid: int
    seconds: int
    state: str
    message: str
