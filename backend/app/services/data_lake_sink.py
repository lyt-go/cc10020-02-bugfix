"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。
跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、event_archive（归档接口）。
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import zlib
from datetime import datetime, timezone
from typing import Any

from app import persistence
from app.services.base import BaseModuleService


def _store():
    """始终使用当前共享存储实例（测试会替换 ``persistence.json_store``）。"""
    return persistence.json_store


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def __init__(self) -> None:
        super().__init__()
        self._lake_base_override: str | None = None
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    @property
    def _lake_base(self) -> str:
        """数据湖根目录：跟随共享存储根目录，可用 ``_lake_base_override`` 重定向。"""
        if self._lake_base_override:
            return self._lake_base_override
        return os.path.join(_store().base_dir, "lake")

    @_lake_base.setter
    def _lake_base(self, value: str) -> None:
        self._lake_base_override = value

    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 确定分区路径（按日期/事件类型）
        2. 序列化数据（默认JSON格式）
        3. 写入分区文件
        4. 更新写入统计

        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件数据
            source_channel_id: 来源通道ID

        Returns:
            { ok: bool, message: str, partition: str, file_path: str }
        """
        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        # 构建数据记录
        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "record_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
        }

        # 写入分区文件
        file_name = f"{now.strftime('%H%M%S')}_{event_id[:8]}.json"
        file_path = os.path.join(partition_path, file_name)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1

            return {
                "ok": True,
                "message": f"数据已写入数据湖分区 {date_partition}",
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
                "record_hash": record["record_hash"],
            }
        except OSError as e:
            return {"ok": False, "message": f"写入失败: {e}", "partition": "", "file_path": ""}

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0) -> dict[str, Any]:
        """批量写入多条记录到数据湖。

        按每小时一个文件聚合写入。
        """
        if not records:
            return {"ok": True, "message": "无数据需要写入", "written": 0}

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        batch_file = f"batch_{now.strftime('%H%M%S')}.json"
        file_path = os.path.join(partition_path, batch_file)

        batch_records = []
        for i, record in enumerate(records):
            batch_records.append({
                "event_id": f"batch_{now.strftime('%Y%m%d%H%M%S')}_{i:04d}",
                "event_type": event_type,
                "source_channel_id": source_channel_id,
                "payload": record,
                "ingested_at": now.isoformat(),
            })

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(batch_records, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)

            return {
                "ok": True,
                "message": f"批量 {len(batch_records)} 条写入完成",
                "written": len(batch_records),
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
            }
        except OSError as e:
            return {"ok": False, "message": f"批量写入失败: {e}", "written": 0}

    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_str)

        if not os.path.exists(partition_path):
            return {"ok": True, "records": [], "total": 0, "partition": f"events/{event_type}/{date_str}"}

        records = []
        for fname in sorted(os.listdir(partition_path))[:limit]:
            if fname.endswith(".json"):
                try:
                    with open(os.path.join(partition_path, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            records.extend(data)
                        else:
                            records.append(data)
                except (json.JSONDecodeError, OSError):
                    continue

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": f"events/{event_type}/{date_str}",
        }

    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                # 按事件类型统计
                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def list_event_partitions(self, event_types: list[str] | None = None) -> list[dict[str, Any]]:
        """列出数据湖中的事件分区（供事件归档清理等模块跨模块消费）。

        每个分区按 ``events/<事件类型>/<日期层级>`` 组织，返回文件清单元数据、
        分区内容版本号 ``lake_version`` 以及事件类型/日期，归档模块据此按事件
        类型限定归档范围，并以版本号识别同一落湖版本。
        """
        events_dir = os.path.join(self._lake_base, "events")
        if not os.path.exists(events_dir):
            return []

        wanted = None
        if event_types:
            wanted = [str(item).strip() for item in event_types if str(item).strip()]

        partitions: list[dict[str, Any]] = []
        for root, dirs, files in os.walk(events_dir):
            json_files = sorted(f for f in files if f.endswith(".json"))
            if not json_files:
                continue

            rel_path = os.path.relpath(root, events_dir).replace(os.sep, "/")
            rel_path = "" if rel_path == "." else rel_path
            parts = rel_path.split("/") if rel_path else []
            event_type = parts[0] if parts else ""
            date_partition = "/".join(parts[1:]) if len(parts) > 1 else ""

            if wanted is not None and not self._event_type_in_scope(event_type, wanted):
                continue

            file_metas: list[dict[str, Any]] = []
            for name in json_files:
                filepath = os.path.join(root, name)
                try:
                    stat = os.stat(filepath)
                except OSError:
                    continue
                file_metas.append({
                    "name": name,
                    "path": filepath,
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                })
            if not file_metas:
                continue

            partition_key = f"events/{rel_path}" if rel_path else "events"
            partitions.append({
                "partition": partition_key,
                "event_type": event_type,
                "date": date_partition,
                "path": root,
                "files": file_metas,
                "file_count": len(file_metas),
                "size_bytes": sum(item["size"] for item in file_metas),
                "latest_mtime": max(item["mtime"] for item in file_metas),
                "lake_version": self.build_partition_version(
                    {"partition": partition_key, "path": root},
                    [item["name"] for item in file_metas],
                ),
            })

        partitions.sort(key=lambda item: item["partition"])
        return partitions

    @staticmethod
    def _event_type_in_scope(event_type: str, scope: list[str]) -> bool:
        """判断事件类型是否落在策略范围，支持显式列表与 ``prefix*`` 通配。"""
        for pattern in scope:
            if pattern == event_type:
                return True
            if "*" in pattern and fnmatch.fnmatchcase(event_type, pattern):
                return True
        return False

    def build_partition_version(self, partition: dict[str, Any], file_names: list[str] | None = None) -> str:
        """计算落湖分区内容版本号。

        版本号由分区路径和文件内容共同决定，相同版本代表同一份落湖数据，
        归档模块可据此保证同一落湖版本只登记一份有效归档凭证。
        """
        base_path = str(partition.get("path", ""))
        partition_key = str(partition.get("partition", ""))
        names = sorted(file_names if file_names is not None else [])
        if not names:
            try:
                names = sorted(f for f in os.listdir(base_path) if f.endswith(".json"))
            except OSError:
                names = []

        digest = hashlib.sha256()
        digest.update(partition_key.encode("utf-8"))
        digest.update(b"\0")
        for name in names:
            digest.update(name.encode("utf-8"))
            digest.update(b"\0")
            try:
                with open(os.path.join(base_path, name), "rb") as fh:
                    digest.update(fh.read())
            except OSError:
                continue
            digest.update(b"\0")
        return "lv_" + digest.hexdigest()[:16]

    def read_partition_files(
        self,
        partition: dict[str, Any],
        file_names: list[str] | None = None,
    ) -> dict[str, Any]:
        """读取事件分区内的落湖记录，供归档等跨模块流程消费。"""
        base_path = str(partition.get("path", ""))
        names = sorted(file_names if file_names is not None else [])
        if not names:
            try:
                names = sorted(f for f in os.listdir(base_path) if f.endswith(".json"))
            except OSError:
                names = []

        files: list[dict[str, Any]] = []
        for name in names:
            filepath = os.path.join(base_path, name)
            try:
                stat = os.stat(filepath)
                with open(filepath, "rb") as fh:
                    content = fh.read()
            except OSError:
                continue
            files.append({
                "name": name,
                "path": filepath,
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "content": content,
            })

        partition_key = str(partition.get("partition", ""))
        return {
            "ok": True,
            "partition": partition_key,
            "event_type": partition.get("event_type", ""),
            "date": partition.get("date", ""),
            "lake_version": self.build_partition_version(partition, [item["name"] for item in files]),
            "files": files,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = _store().get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "operation": self.OPERATION_TERM,
        }