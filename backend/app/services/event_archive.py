"""事件归档清理领域服务：历史事件归档、压缩与过期清理。

管理历史事件数据的归档策略，支持按条件归档、
数据压缩和过期数据的清理调度。

数据流：
``data_lake_sink`` 的事件分区 → 按事件类型归档 → 事件归档清单（凭证）
→ 清理台账（源分区版本 + 保留日期）→ 恢复时按同一凭证核对来源。

跨模块关联：data_lake_sink（归档来源）、integration_monitor（归档统计）。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any

from app import persistence
from app.services.base import BaseModuleService
from app.services.data_lake_sink import DataLakeSinkService

# 事件归档清单（每份有效凭证一条）与清理台账使用独立分区，便于跨模块消费。
MANIFEST_KEY = "event_archive_manifest"
LEDGER_KEY = "event_archive_ledger"

GENERIC_EVENT_TYPES = {"", "ALL", "all", "*", "全部", "通用"}
DEFAULT_RETENTION_DAYS = 90
ACTIVE_STATUS = "有效"
SUPERSEDED_STATUS = "已替代"
CLEANED_STATUS = "已清理"

# 同一进程内并发执行归档时，保证“检查 + 写文件 + 登记凭证”是原子的。
_ARCHIVE_LOCK = threading.RLock()


def _store():
    """始终使用当前共享存储实例（测试会替换 ``persistence.json_store``）。"""
    return persistence.json_store


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


class EventArchiveService(BaseModuleService):
    MODULE = "event_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略编号", "事件类型", "归档条件", "归档路径", "压缩格式", "保留天数", "清理方式", "恢复超时"]
    REQUIRED_FIELDS = ["策略编号", "事件类型", "归档条件"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def __init__(self) -> None:
        super().__init__()
        self._archive_base_override: str | None = None
        os.makedirs(self._archive_base, exist_ok=True)
        self._archive_history: list[dict[str, Any]] = []

    @property
    def _archive_base(self) -> str:
        """归档根目录：跟随共享存储根目录，可用 ``_archive_base_override`` 重定向。"""
        if self._archive_base_override:
            return self._archive_base_override
        return os.path.join(_store().base_dir, "archive")

    @_archive_base.setter
    def _archive_base(self, value: str) -> None:
        self._archive_base_override = value

    # ------------------------------------------------------------------
    # 归档执行
    # ------------------------------------------------------------------
    def execute_archive(self, policy_id: int) -> dict[str, Any]:
        """执行事件归档任务。

        流程：
        1. 加载归档策略，解析其事件类型范围（专用策略只覆盖范围内的分区）
        2. 通过 data_lake_sink 跨模块读取落湖事件分区及分区版本
        3. 按归档条件筛选分区，压缩为按事件类型隔离的归档文件
        4. 登记事件归档清单（同一落湖版本只保留一份有效凭证）
        5. 同步清理台账，并按每条凭证自身的保留期清理到期归档
        """
        policy = _store().get_by_id(self.MODULE, policy_id)
        if policy is None:
            return {"ok": False, "message": f"归档策略 {policy_id} 不存在", "archived": 0}

        if policy.get("status") != "已生效":
            return {"ok": False, "message": f"归档策略未生效({policy['status']})", "archived": 0}

        scope = self._policy_event_scope(policy)
        event_type_label = str(policy.get("事件类型", "ALL") or "ALL")
        archive_condition = str(policy.get("归档条件", "days>30") or "days>30")
        compress_format = str(policy.get("压缩格式", "zlib") or "zlib")
        retention_days = self._policy_retention(policy)
        threshold = self._parse_days_threshold(archive_condition)

        # 跨模块读取落湖事件分区（同一份落湖记录只经由 DataLakeSinkService 访问）。
        lake_svc = DataLakeSinkService()
        partitions = lake_svc.list_event_partitions(scope)

        now = _now()
        archive_date = now.strftime("%Y%m%d")
        batch_id = f"arch_{archive_date}"

        archived = 0
        deduped = 0
        skipped = 0
        total_size = 0
        credentials: list[dict[str, Any]] = []

        for partition in partitions:
            days_old = int((now - datetime.fromtimestamp(partition["latest_mtime"], tz=timezone.utc)).days)
            if threshold is not None and days_old < threshold:
                skipped += 1
                continue

            # 跨模块读取落湖记录，归档内容以读取到的版本为准。
            partition_data = lake_svc.read_partition_files(partition)
            if not partition_data["files"]:
                skipped += 1
                continue

            with _ARCHIVE_LOCK:
                outcome = self._register_archive(
                    policy=policy,
                    partition_data=partition_data,
                    compress_format=compress_format,
                    retention_days=retention_days,
                    now=now,
                    batch_id=batch_id,
                )

            credential = outcome["credential"]
            if credential:
                credentials.append(credential)
            if outcome["status"] == "archived":
                archived += 1
                total_size += int(credential.get("source_size_bytes", 0))
            else:
                deduped += 1

        cleanup = self.cleanup_expired_archives()

        self._archive_history.append({
            "policy_id": policy_id,
            "event_type": event_type_label,
            "scope": scope or ["*"],
            "archived": archived,
            "deduped": deduped,
            "skipped": skipped,
            "size_bytes": total_size,
            "cleaned": cleanup["cleaned"],
            "timestamp": now.isoformat(),
        })

        return {
            "ok": True,
            "message": (
                f"归档完成: {archived} 个分区, {round(total_size/1024, 2)} KB"
                f"{', 复用有效凭证 ' + str(deduped) + ' 个' if deduped else ''}"
            ),
            "archived": archived,
            "deduped": deduped,
            "skipped": skipped,
            "total_size_bytes": total_size,
            "cleaned_old": cleanup["cleaned"],
            "cleanup": cleanup,
            "credentials": credentials,
            "manifest": [self._public_credential(item) for item in credentials],
            "retention_days": retention_days,
        }

    def _register_archive(
        self,
        *,
        policy: dict[str, Any],
        partition_data: dict[str, Any],
        compress_format: str,
        retention_days: int,
        now: datetime,
        batch_id: str,
    ) -> dict[str, Any]:
        """在锁内完成单个分区的归档登记。

        同一 ``source_partition + lake_version`` 只允许一份有效凭证：
        - 已有同等或更高专用程度的有效凭证：直接复用，不再登记；
        - 已有更泛化的有效凭证、当前为专用策略：旧凭证标记已替代，
          由专用策略接管（仍然只有一份有效凭证）。
        """
        partition_key = str(partition_data["partition"])
        lake_version = str(partition_data["lake_version"])
        event_type = str(partition_data.get("event_type", ""))
        specificity = self._policy_specificity(policy)

        manifest = _store().get_all(MANIFEST_KEY)
        existing = next(
            (
                row for row in manifest
                if row.get("active")
                and str(row.get("lake_version")) == lake_version
                and str(row.get("source_partition")) == partition_key
            ),
            None,
        )

        if existing is not None and int(existing.get("scope_specificity", 0)) >= specificity:
            # 同一落湖版本已登记有效凭证，直接复用。
            return {"status": "deduped", "credential": self._public_credential(existing)}

        archive_path, archive_rel, archive_size, checksum = self._write_archive_file(
            event_type=event_type,
            lake_version=lake_version,
            compress_format=compress_format,
            partition_data=partition_data,
        )
        if archive_path is None:
            return {"status": "failed", "credential": None}

        if existing is not None:
            # 专用策略接管同一落湖版本：旧凭证标记已替代，归档文件按版本命名复用。
            _store().update(MANIFEST_KEY, int(existing["id"]), {
                "status": SUPERSEDED_STATUS,
                "凭证状态": SUPERSEDED_STATUS,
                "active": False,
                "superseded_at": _iso(now),
                "superseded_by": f"policy:{policy.get('id')}",
            })
            self._touch_ledger(
                credential_no=str(existing.get("credential_no") or existing.get("凭证编号")),
                values={
                    "状态": SUPERSEDED_STATUS,
                    "清理结果": "已替代",
                    "更新时间": _iso(now),
                },
            )

        retention_until_dt = now + timedelta(days=retention_days)
        source_names = [item["name"] for item in partition_data["files"]]
        # 凭证编号带上策略归属，替代后的新旧凭证在清单/台账中互不覆盖。
        credential_no = f"CRED-{lake_version}-P{policy.get('id')}"
        record = {
            # 中文业务字段
            "凭证编号": credential_no,
            "策略编号": policy.get("id"),
            "策略归属": f"policy:{policy.get('id')}",
            "事件类型": event_type,
            "源分区": partition_key,
            "源分区版本": lake_version,
            "归档文件": archive_rel,
            "归档路径": archive_path,
            "压缩格式": compress_format,
            "源文件数": len(partition_data["files"]),
            "源大小字节": int(partition_data.get("size_bytes") or sum(i["size"] for i in partition_data["files"])),
            "归档大小字节": archive_size,
            "保留天数": retention_days,
            "保留日期": retention_until_dt.strftime("%Y-%m-%d"),
            "归档时间": _iso(now),
            "凭证状态": ACTIVE_STATUS,
            # 结构化字段（跨模块消费 / 测试核对）
            "credential_no": credential_no,
            "policy_id": policy.get("id"),
            "event_type": event_type,
            "source_partition": partition_key,
            "lake_version": lake_version,
            "archive_file": archive_rel,
            "archive_path": archive_path,
            "compress_format": compress_format,
            "file_count": len(partition_data["files"]),
            "source_files": source_names,
            "source_size_bytes": int(partition_data.get("size_bytes") or sum(i["size"] for i in partition_data["files"])),
            "archive_size_bytes": archive_size,
            "checksum": checksum,
            "archived_at": _iso(now),
            "archived_ts": now.timestamp(),
            "retention_days": retention_days,
            "retention_until": _iso(retention_until_dt),
            "retention_date": retention_until_dt.strftime("%Y-%m-%d"),
            "status": ACTIVE_STATUS,
            "active": True,
            "scope_specificity": specificity,
            "batch": batch_id,
        }
        credential = _store().insert(MANIFEST_KEY, record)

        # 归档结果同步到清理台账：源分区版本与保留日期直接取自归档清单记录。
        self._touch_ledger(
            credential_no=str(credential["credential_no"]),
            values=self._ledger_values_from_credential(credential),
        )
        return {"status": "archived", "credential": self._public_credential(credential)}

    @staticmethod
    def _ledger_values_from_credential(credential: dict[str, Any]) -> dict[str, Any]:
        """由归档清单记录派生清理台账字段，保证两边来源一致。"""
        credential_no = str(credential.get("credential_no") or credential.get("凭证编号") or "")
        return {
            "台账编号": f"LEDGER-{credential_no}",
            "凭证编号": credential_no,
            "策略归属": credential.get("策略归属"),
            "策略编号": credential.get("策略编号"),
            "事件类型": credential.get("事件类型"),
            "源分区": credential.get("源分区"),
            "源分区版本": credential.get("源分区版本"),
            "归档文件": credential.get("归档文件"),
            "归档路径": credential.get("归档路径"),
            "保留天数": credential.get("保留天数"),
            "保留日期": credential.get("保留日期"),
            "归档时间": credential.get("归档时间"),
            "清理时间": None,
            "清理结果": "待到期",
            "状态": credential.get("凭证状态"),
            "来源": "事件归档清单",
            "凭证状态": credential.get("凭证状态"),
        }

    def _write_archive_file(
        self,
        *,
        event_type: str,
        lake_version: str,
        compress_format: str,
        partition_data: dict[str, Any],
    ) -> tuple[str | None, str, int, str]:
        """写出归档文件，文件按事件类型隔离、按落湖版本命名（幂等）。"""
        safe_type = re.sub(r"[^0-9A-Za-z_.-]+", "_", event_type).strip("._") or "all"
        suffix = ".zlib" if compress_format == "zlib" else ".json"
        directory = os.path.join(self._archive_base, safe_type)
        filepath = os.path.join(directory, f"{lake_version}{suffix}")

        bundle = {
            "partition": partition_data["partition"],
            "event_type": event_type,
            "date": partition_data.get("date", ""),
            "lake_version": lake_version,
            "files": [
                {"name": item["name"], "content": item["content"].decode("utf-8", errors="replace")}
                for item in partition_data["files"]
            ],
        }
        payload = json.dumps(bundle, ensure_ascii=False, default=str).encode("utf-8")
        if compress_format == "zlib":
            payload = zlib.compress(payload)

        try:
            os.makedirs(directory, exist_ok=True)
            with open(filepath, "wb") as fh:
                fh.write(payload)
        except OSError:
            return None, "", 0, ""

        return (
            filepath,
            os.path.relpath(filepath, self._archive_base).replace(os.sep, "/"),
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        )

    # ------------------------------------------------------------------
    # 到期清理
    # ------------------------------------------------------------------
    def cleanup_expired_archives(self, retention_days: int | None = None) -> dict[str, Any]:
        """按归档清单中每条凭证自己的保留期清理到期归档。

        ``retention_days`` 只作为历史凭证无法回查策略时的兜底值，
        不再用“本次执行策略”的保留期覆盖其它策略的归档。
        """
        now = _now()
        cleaned = 0
        kept = 0
        freed = 0
        details: list[dict[str, Any]] = []

        with _ARCHIVE_LOCK:
            manifest = _store().get_all(MANIFEST_KEY)
            active = [row for row in manifest if row.get("active")]

            for credential in active:
                retention, retention_source = self._credential_retention(credential, retention_days)
                archived_at = self._credential_archived_at(credential)
                expire_at = archived_at + timedelta(days=retention)
                if now < expire_at:
                    kept += 1
                    continue

                archive_path = str(credential.get("archive_path") or "")
                shared = any(
                    other.get("id") != credential.get("id")
                    and other.get("active")
                    and os.path.normpath(str(other.get("archive_path") or "")) == os.path.normpath(archive_path)
                    for other in active
                )
                file_size = 0
                removed = False
                if archive_path and not shared and self._is_inside_archive_base(archive_path):
                    try:
                        file_size = os.path.getsize(archive_path)
                        os.remove(archive_path)
                        removed = True
                    except OSError:
                        removed = False

                updated = _store().update(MANIFEST_KEY, int(credential["id"]), {
                    "status": CLEANED_STATUS,
                    "凭证状态": CLEANED_STATUS,
                    "active": False,
                    "cleaned_at": _iso(now),
                    "cleanup_reason": f"保留期 {retention} 天已到期",
                })
                self._touch_ledger(
                    credential_no=str(credential.get("credential_no") or credential.get("凭证编号")),
                    values={
                        # 先按清单记录刷新来源字段，再写入清理结果，保证台账与清单一致。
                        **self._ledger_values_from_credential(updated or credential),
                        "清理时间": _iso(now),
                        "清理结果": "已清理",
                        "状态": CLEANED_STATUS,
                        "释放字节": file_size,
                        "清理原因": f"保留期 {retention} 天已到期",
                        "保留来源": retention_source,
                    },
                )
                cleaned += 1
                freed += file_size
                details.append({
                    "credential_no": credential.get("credential_no"),
                    "source_partition": credential.get("source_partition"),
                    "lake_version": credential.get("lake_version"),
                    "retention_days": retention,
                    "retention_source": retention_source,
                    "status": "cleaned" if removed else "cleaned_file_kept",
                })

            # 兼容历史凭证：归档目录里没有被清单登记的旧文件按策略归属回查保留期。
            referenced = {
                os.path.normpath(str(row.get("archive_path") or ""))
                for row in manifest
                if row.get("archive_path")
            }
            historical = self._cleanup_historical_files(
                referenced=referenced,
                fallback_retention=retention_days,
                now=now,
            )
            cleaned += historical["cleaned"]
            freed += historical["freed_bytes"]
            kept += historical["kept"]
            details.extend(historical["details"])

        return {
            "cleaned": cleaned,
            "kept": kept,
            "freed_bytes": freed,
            "details": details,
        }

    def _clean_expired_archives(self, retention_days: int = DEFAULT_RETENTION_DAYS) -> int:
        """兼容旧调用：返回本次清理掉的文件数。"""
        return int(self.cleanup_expired_archives(retention_days).get("cleaned", 0))

    def _cleanup_historical_files(
        self,
        *,
        referenced: set[str],
        fallback_retention: int | None,
        now: datetime,
    ) -> dict[str, Any]:
        """清理没有清单记录的历史归档文件，并回查其策略归属与保留期。"""
        cleaned = 0
        kept = 0
        freed = 0
        details: list[dict[str, Any]] = []

        if not os.path.exists(self._archive_base):
            return {"cleaned": 0, "kept": 0, "freed_bytes": 0, "details": []}

        policies = _store().get_all(self.MODULE)

        for root, _dirs, files in os.walk(self._archive_base):
            for name in sorted(files):
                filepath = os.path.join(root, name)
                if not os.path.isfile(filepath):
                    continue
                if os.path.normpath(filepath) in referenced:
                    continue

                policy = self._resolve_policy_for_archive_file(filepath, policies)
                policy_retention = _parse_int(policy.get("保留天数")) if policy is not None else None
                if policy_retention is not None and policy_retention >= 0:
                    retention, retention_source = policy_retention, "policy"
                else:
                    retention = fallback_retention if fallback_retention is not None else DEFAULT_RETENTION_DAYS
                    retention_source = "fallback"

                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                    size = os.path.getsize(filepath)
                except OSError:
                    continue

                rel = os.path.relpath(filepath, self._archive_base).replace(os.sep, "/")
                legacy_no = "LEGACY-" + hashlib.sha256(rel.encode("utf-8")).hexdigest()[:12]
                expire_at = mtime + timedelta(days=retention)
                common = {
                    "台账编号": f"LEDGER-LEGACY-{hashlib.sha256(rel.encode('utf-8')).hexdigest()[:12]}",
                    "凭证编号": legacy_no,
                    "策略归属": f"policy:{policy.get('id')}" if policy else "未匹配",
                    "策略编号": policy.get("id") if policy else None,
                    "事件类型": str(policy.get("事件类型", "")) if policy else "",
                    "源分区": rel,
                    "源分区版本": "historical",
                    "归档文件": rel,
                    "归档路径": filepath,
                    "保留天数": retention,
                    "保留日期": expire_at.strftime("%Y-%m-%d"),
                    "来源": "历史凭证",
                    "保留来源": retention_source,
                }

                if now >= expire_at:
                    try:
                        os.remove(filepath)
                        removed = True
                    except OSError:
                        removed = False
                    if removed:
                        cleaned += 1
                        freed += size
                        self._touch_ledger(credential_no=legacy_no, values={
                            **common,
                            "清理时间": _iso(now),
                            "清理结果": "已清理",
                            "状态": CLEANED_STATUS,
                            "释放字节": size,
                        })
                        details.append({
                            "credential_no": legacy_no,
                            "archive_file": rel,
                            "retention_days": retention,
                            "retention_source": retention_source,
                            "source": "historical",
                            "status": "cleaned",
                        })
                else:
                    kept += 1
                    self._touch_ledger(credential_no=legacy_no, values={
                        **common,
                        "清理时间": None,
                        "清理结果": "保留中",
                        "状态": "保留中",
                    })

        return {"cleaned": cleaned, "kept": kept, "freed_bytes": freed, "details": details}

    # ------------------------------------------------------------------
    # 清单 / 台账查询
    # ------------------------------------------------------------------
    def get_archive_manifest(
        self,
        *,
        event_type: str | None = None,
        policy_id: int | None = None,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        """读取事件归档清单。"""
        rows = _store().get_all(MANIFEST_KEY)
        if event_type:
            rows = [row for row in rows if str(row.get("event_type")) == event_type]
        if policy_id is not None:
            rows = [row for row in rows if _parse_int(row.get("policy_id")) == int(policy_id)]
        if active_only:
            rows = [row for row in rows if row.get("active")]
        return [dict(row) for row in rows]

    def list_archive_manifest(self, **filters: Any) -> list[dict[str, Any]]:
        """``get_archive_manifest`` 的别名，便于调用方按清单语义读取。"""
        return self.get_archive_manifest(**filters)

    def get_valid_credentials(self) -> list[dict[str, Any]]:
        """返回当前有效的归档凭证（同一落湖版本最多一份）。"""
        return self.get_archive_manifest(active_only=True)

    def get_cleanup_ledger(
        self,
        *,
        credential_no: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """读取清理台账。"""
        rows = _store().get_all(LEDGER_KEY)
        if credential_no:
            rows = [row for row in rows if str(row.get("凭证编号")) == credential_no]
        if status:
            rows = [row for row in rows if str(row.get("状态")) == status]
        return [dict(row) for row in rows]

    def list_cleanup_ledger(self, **filters: Any) -> list[dict[str, Any]]:
        """``get_cleanup_ledger`` 的别名。"""
        return self.get_cleanup_ledger(**filters)

    def _touch_ledger(self, *, credential_no: str, values: dict[str, Any]) -> None:
        """按凭证编号登记/更新清理台账，保留清单中的源分区版本和保留日期。"""
        if not credential_no:
            return
        rows = _store().get_all(LEDGER_KEY)
        for row in rows:
            if str(row.get("凭证编号")) == credential_no:
                _store().update(LEDGER_KEY, int(row["id"]), values)
                return
        record = {"凭证编号": credential_no, **values}
        _store().insert(LEDGER_KEY, record)

    # ------------------------------------------------------------------
    # 恢复
    # ------------------------------------------------------------------
    def restore_from_archive(self, archive_file: str = "", credential_no: str | None = None) -> dict[str, Any]:
        """从归档恢复数据。

        提供凭证编号时，用该凭证核对来源分区、落湖版本与归档校验和；
        归档已被到期清理时返回清单中的来源信息，保证归档与恢复详情一致。
        """
        manifest = _store().get_all(MANIFEST_KEY)
        credential = self._find_credential(manifest, archive_file, credential_no)

        if credential is None:
            return self._restore_unregistered(archive_file)

        archive_path = str(credential.get("archive_path") or "")
        source_info = {
            "credential_no": credential.get("credential_no") or credential.get("凭证编号"),
            "source_partition": credential.get("source_partition") or credential.get("源分区"),
            "lake_version": credential.get("lake_version") or credential.get("源分区版本"),
            "retention_date": credential.get("retention_date") or credential.get("保留日期"),
            "status": credential.get("status"),
        }

        if not archive_path or not os.path.exists(archive_path):
            return {
                "ok": False,
                "message": (
                    "归档已到期清理，无法恢复"
                    if credential.get("status") == CLEANED_STATUS
                    else "归档文件未找到"
                ),
                "credential": credential,
                "source": source_info,
                "restored": False,
            }

        try:
            with open(archive_path, "rb") as fh:
                payload = fh.read()
        except OSError:
            return {"ok": False, "message": "归档文件损坏或不存在", "credential": credential, "source": source_info}

        checksum = hashlib.sha256(payload).hexdigest()
        expected = str(credential.get("checksum") or "")
        if expected and checksum != expected:
            return {
                "ok": False,
                "message": "归档文件与凭证记录不一致",
                "credential": credential,
                "source": source_info,
                "restored": False,
                "checksum": checksum,
                "expected_checksum": expected,
            }

        if str(credential.get("compress_format")) == "zlib":
            try:
                payload = zlib.decompress(payload)
            except zlib.error:
                return {"ok": False, "message": "归档文件损坏或不存在", "credential": credential, "source": source_info}

        text = payload.decode("utf-8", errors="replace")
        bundle = self._parse_bundle(text)
        return {
            "ok": True,
            "data": text,
            "files": bundle["files"] if bundle else [],
            "credential": credential,
            "source": source_info,
            "source_partition": source_info["source_partition"],
            "source_version": source_info["lake_version"],
            "verified": True,
        }

    def _restore_unregistered(self, archive_file: str) -> dict[str, Any]:
        """兼容旧归档：没有清单记录时按文件名直接恢复。"""
        for root, _dirs, files in os.walk(self._archive_base):
            for name in files:
                if name == archive_file or name.startswith(archive_file):
                    filepath = os.path.join(root, name)
                    try:
                        with open(filepath, "rb") as src:
                            payload = src.read()
                        if name.endswith(".zlib"):
                            payload = zlib.decompress(payload)
                        return {"ok": True, "data": payload.decode("utf-8", errors="replace"), "files": []}
                    except (OSError, zlib.error, UnicodeDecodeError):
                        return {"ok": False, "message": "归档文件损坏或不存在"}
        return {"ok": False, "message": "归档文件未找到"}

    def _find_credential(
        self,
        manifest: list[dict[str, Any]],
        archive_file: str,
        credential_no: str | None,
    ) -> dict[str, Any] | None:
        target = str(credential_no or archive_file or "")
        normalized = os.path.normpath(target) if target else ""

        matches: list[dict[str, Any]] = []
        for row in manifest:
            candidates = {
                str(row.get("credential_no") or ""),
                str(row.get("凭证编号") or ""),
                str(row.get("lake_version") or ""),
                str(row.get("archive_file") or ""),
                os.path.normpath(str(row.get("archive_path") or "")) if row.get("archive_path") else "",
            }
            matched = bool(target and target in candidates) or bool(normalized and normalized in candidates)
            if not matched and target:
                matched = os.path.basename(target) == os.path.basename(str(row.get("archive_file") or ""))
            if matched:
                matches.append(dict(row))

        if not matches:
            return None
        # 优先返回有效凭证，其次按归档时间取最新，避免恢复落到已替代的旧凭证。
        matches.sort(
            key=lambda row: (bool(row.get("active")), str(row.get("archived_at") or "")),
            reverse=True,
        )
        return matches[0]

    @staticmethod
    def _parse_bundle(text: str) -> dict[str, Any] | None:
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return None
        if isinstance(data, dict) and isinstance(data.get("files"), list):
            return data
        return None

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------
    def get_archive_stats(self) -> dict[str, Any]:
        """获取归档存储统计（含清单凭证与清理台账）。"""
        total_files = 0
        total_size = 0
        for root, _dirs, files in os.walk(self._archive_base):
            for f in files:
                total_files += 1
                try:
                    total_size += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass

        manifest = _store().get_all(MANIFEST_KEY)
        active = [row for row in manifest if row.get("active")]
        by_event_type: dict[str, int] = {}
        by_policy: dict[str, int] = {}
        for row in active:
            etype = str(row.get("event_type", ""))
            by_event_type[etype] = by_event_type.get(etype, 0) + 1
            policy_key = str(row.get("policy_id"))
            by_policy[policy_key] = by_policy.get(policy_key, 0) + 1

        return {
            "base_dir": self._archive_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "archive_batches": len([d for d in os.listdir(self._archive_base)
                                    if os.path.isdir(os.path.join(self._archive_base, d))]) if os.path.exists(self._archive_base) else 0,
            "manifest_count": len(manifest),
            "valid_credentials": len(active),
            "ledger_count": len(_store().get_all(LEDGER_KEY)),
            "by_event_type": by_event_type,
            "by_policy": by_policy,
            "history": self._archive_history[-10:],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件归档清理的数据质量快照。"""
        rows = _store().get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        archive_stats = self.get_archive_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件归档清理数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "archive_files": archive_stats.get("total_files", 0),
            "archive_size_mb": archive_stats.get("total_size_mb", 0),
            "valid_credentials": archive_stats.get("valid_credentials", 0),
            "cleanup_ledger_rows": archive_stats.get("ledger_count", 0),
            "operation": self.OPERATION_TERM,
        }

    # ------------------------------------------------------------------
    # 策略解析
    # ------------------------------------------------------------------
    def _policy_event_scope(self, policy: dict[str, Any]) -> list[str] | None:
        """解析策略的事件类型范围；None 表示覆盖全部事件分区。"""
        raw = str(policy.get("事件类型") or "").strip()
        tokens = [token for token in re.split(r"[,，;；\s]+", raw) if token]
        if not tokens:
            return None
        if any(token in GENERIC_EVENT_TYPES for token in tokens):
            return None
        return tokens

    def _policy_specificity(self, policy: dict[str, Any]) -> int:
        scope = self._policy_event_scope(policy)
        return len(scope) if scope else 0

    @staticmethod
    def _policy_retention(policy: dict[str, Any] | None) -> int:
        if policy is None:
            return DEFAULT_RETENTION_DAYS
        value = _parse_int(policy.get("保留天数"))
        if value is None or value < 0:
            return DEFAULT_RETENTION_DAYS
        return value

    def _credential_retention(
        self,
        credential: dict[str, Any],
        fallback: int | None,
    ) -> tuple[int, str]:
        """取凭证自身保留期；历史凭证回查其策略归属的保留期。"""
        value = _parse_int(credential.get("retention_days", credential.get("保留天数")))
        if value is not None and value >= 0:
            return value, "credential"

        policy_id = _parse_int(credential.get("policy_id", credential.get("策略编号")))
        if policy_id is not None:
            policy = _store().get_by_id(self.MODULE, policy_id)
            if policy is not None:
                return self._policy_retention(policy), "policy"

        if fallback is not None:
            return int(fallback), "fallback"
        return DEFAULT_RETENTION_DAYS, "default"

    @staticmethod
    def _credential_archived_at(credential: dict[str, Any]) -> datetime:
        raw = credential.get("archived_at") or credential.get("归档时间")
        if raw:
            try:
                return datetime.fromisoformat(str(raw)).astimezone(timezone.utc)
            except ValueError:
                pass
        ts = credential.get("archived_ts")
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        path = str(credential.get("archive_path") or "")
        try:
            return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
        except OSError:
            return _now()

    def _resolve_policy_for_archive_file(
        self,
        filepath: str,
        policies: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """为历史归档文件回查策略归属：优先匹配更长的专用事件类型。"""
        rel = os.path.relpath(filepath, self._archive_base).replace(os.sep, "/")
        haystack = rel.casefold()

        best: tuple[int, dict[str, Any]] | None = None
        generic: dict[str, Any] | None = None
        for policy in policies:
            scope = self._policy_event_scope(policy)
            if scope is None:
                if generic is None:
                    generic = policy
                continue
            for token in scope:
                if token.casefold() in haystack:
                    length = len(token)
                    if best is None or length > best[0]:
                        best = (length, policy)
        if best is not None:
            return best[1]
        if generic is not None:
            return generic
        return None

    def _is_inside_archive_base(self, path: str) -> bool:
        try:
            return os.path.commonpath([os.path.realpath(path), os.path.realpath(self._archive_base)]) == os.path.realpath(self._archive_base)
        except (ValueError, OSError):
            return False

    @staticmethod
    def _parse_days_threshold(condition: str) -> int | None:
        match = re.search(r"days\s*>\s*(\d+)", condition or "")
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def _public_credential(credential: dict[str, Any] | None) -> dict[str, Any] | None:
        if credential is None:
            return None
        return dict(credential)
