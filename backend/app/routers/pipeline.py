"""跨模块数据流管道API：暴露完整业务链路端点。

将领域服务的能力串联为可测试的数据流管道，每条管道对应
一个跨模块数据流链路。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/pipeline", tags=["跨模块数据流"])


class WebhookPayload(BaseModel):
    event_type: str = "default_event"
    data: dict[str, Any] = {}
    headers: dict[str, str] | None = None
    signature: str | None = None


class BatchExecuteRequest(BaseModel):
    task_id: int
    max_pages: int = 5


class DlqReplayRequest(BaseModel):
    dlq_ids: list[int]


class BackfillExecuteRequest(BaseModel):
    job_id: int


class EndpointTestRequest(BaseModel):
    endpoint_id: int


class TransformRequest(BaseModel):
    rule_id: int
    source_data: list[dict[str, Any]]


class ValidationRequest(BaseModel):
    records: list[dict[str, Any]]
    source_module: str = ""


class ArchiveRestoreRequest(BaseModel):
    archive_file: str = ""
    credential_no: str | None = None


@router.post("/webhook-to-lake")
def pipeline_webhook_to_lake(channel_id: int, payload: WebhookPayload) -> dict[str, Any]:
    """链路1: Webhook接收 → 事件解析 → 去重 → 分发路由 → 数据落湖。

    完整覆盖：webhook_channel → event_schema → dedup_filter → dispatch_rule → data_lake_sink。
    """
    from app.services.webhook_channel import WebhookChannelService
    svc = WebhookChannelService()
    result = svc.receive_webhook(
        channel_id=channel_id,
        payload={"type": payload.event_type, **payload.data},
        signature=payload.signature,
        headers=payload.headers,
    )
    return result


@router.post("/batch-ingest-to-lake")
def pipeline_batch_ingest_to_lake(request: BatchExecuteRequest) -> dict[str, Any]:
    """链路2: 批量拉取 → 数据转换 → 质量校验 → 数据落湖。

    完整覆盖：batch_ingest → data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.batch_ingest import BatchIngestService
    svc = BatchIngestService()
    result = svc.execute_batch(task_id=request.task_id, max_pages=request.max_pages)
    return result


@router.post("/dlq-replay")
def pipeline_dlq_replay(request: DlqReplayRequest) -> dict[str, Any]:
    """链路3: 死信队列 → 分析 → 重放 → 追踪。

    完整覆盖：dlq_handler → mq_consumer → call_trace_log。
    """
    from app.services.dlq_handler import DlqHandlerService
    svc = DlqHandlerService()

    if len(request.dlq_ids) == 1 and request.dlq_ids[0] == -1:
        # 全部待处置记录重放
        analysis = svc.analyze_failure_reasons()
        if analysis["total"] == 0:
            return {"ok": False, "message": "无死信记录", "replayed": 0}
        from app.persistence import json_store
        all_dlq = json_store.get_all("dlq_handler")
        pending_ids = [int(r["id"]) for r in all_dlq if r.get("status") in ("草稿", "待处置")]
        return svc.batch_replay(pending_ids)

    return svc.batch_replay(request.dlq_ids)


@router.post("/backfill-execute")
def pipeline_backfill_execute(request: BackfillExecuteRequest) -> dict[str, Any]:
    """链路4: 历史回补 → 水位查询 → 批量拉取 → 数据落湖。

    完整覆盖：backfill_job → incre_cursor → batch_ingest → data_lake_sink。
    """
    from app.services.backfill_job import BackfillJobService
    svc = BackfillJobService()
    result = svc.execute_backfill(job_id=request.job_id)
    return result


@router.post("/endpoint-test")
def pipeline_endpoint_test(request: EndpointTestRequest) -> dict[str, Any]:
    """链路5: 端点连接测试 → 认证验证 → 调用追踪。

    完整覆盖：api_endpoint → api_auth_store → call_trace_log。
    """
    from app.services.api_endpoint import ApiEndpointService
    svc = ApiEndpointService()
    result = svc.test_connection(endpoint_id=request.endpoint_id)
    return result


@router.post("/transform-validate")
def pipeline_transform_validate(request: TransformRequest) -> dict[str, Any]:
    """链路6: 数据转换 → 质量校验 → 结果追踪。

    完整覆盖：data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.data_transform import DataTransformService
    transform_svc = DataTransformService()
    batch_result = transform_svc.batch_transform(rule_id=request.rule_id, source_records=request.source_data)
    return batch_result


@router.post("/batch-validate")
def pipeline_validate_batch(request: ValidationRequest) -> dict[str, Any]:
    """链路7: 质量批量校验 → 统计报告。

    完整覆盖：data_quality_rule → integration_monitor。
    """
    from app.services.data_quality_rule import DataQualityRuleService
    svc = DataQualityRuleService()
    result = svc.validate_batch(records=request.records, source_module=request.source_module)
    return result


@router.get("/health-summary")
def pipeline_health_summary() -> dict[str, Any]:
    """链路8: 全局监控指标聚合 → 健康度评分。

    完整覆盖：integration_monitor → call_trace_log → dlq_handler → data_lake_sink。
    """
    from app.services.integration_monitor import IntegrationMonitorService
    svc = IntegrationMonitorService()
    return svc.collect_global_metrics()


@router.get("/dedup-stats")
def pipeline_dedup_stats() -> dict[str, Any]:
    """链路9: 去重统计查询。"""
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    return svc.get_dedup_stats()


@router.post("/archive-execute")
def pipeline_archive_execute(policy_id: int) -> dict[str, Any]:
    """链路10: 归档策略执行 → 压缩 → 登记归档清单 → 同步清理台账 → 过期清理。

    归档过程按事件类型从落湖事件分区读取数据，专用策略只覆盖配置范围内的分区。
    """
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.execute_archive(policy_id=policy_id)


@router.get("/archive-manifest")
def pipeline_archive_manifest(
    event_type: str | None = None,
    policy_id: int | None = None,
    active_only: bool = False,
) -> dict[str, Any]:
    """链路10-1: 事件归档清单查询（每份有效归档凭证一条）。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    items = svc.get_archive_manifest(
        event_type=event_type, policy_id=policy_id, active_only=active_only)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/archive-ledger")
def pipeline_archive_ledger(
    credential_no: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """链路10-2: 清理台账查询（源分区版本与保留日期取自归档清单）。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    items = svc.get_cleanup_ledger(credential_no=credential_no, status=status)
    return {"ok": True, "items": items, "total": len(items)}


@router.post("/archive-cleanup")
def pipeline_archive_cleanup(retention_days: int | None = None) -> dict[str, Any]:
    """链路10-3: 到期清理，按每条有效凭证自己的保留期执行。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    result = svc.cleanup_expired_archives(retention_days=retention_days)
    return {"ok": True, **result}


@router.post("/archive-restore")
def pipeline_archive_restore(request: ArchiveRestoreRequest) -> dict[str, Any]:
    """链路10-4: 按有效凭证恢复，并核对来源分区与落湖版本。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.restore_from_archive(
        archive_file=request.archive_file, credential_no=request.credential_no)