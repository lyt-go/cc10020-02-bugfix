"""事件归档清理数据流测试：落湖分区 → 事件归档清单 → 清理台账 → 恢复核对。

覆盖：
1. 订单专用策略只归档配置范围内的事件分区，不夹带库存事件数据；
2. 通用策略与订单专用策略同时命中同一订单分区时，同一落湖版本只留一份有效凭证；
3. 到期清理按每条凭证自己的保留期执行，兼容历史凭证并回查策略归属；
4. 清理台账的源分区版本与保留日期取自事件归档清单，恢复时可用同一凭证核对来源。
"""
from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
import unittest
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore
import app.services.data_lake_sink as sink_module
import app.services.event_archive as archive_module
from app.services.data_lake_sink import DataLakeSinkService
from app.services.event_archive import EventArchiveService, MANIFEST_KEY, LEDGER_KEY


class TestEventArchiveDataFlow(unittest.TestCase):
    """沿落湖数据流验证事件归档、清单登记、到期清理与恢复。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._previous_store = app.persistence.json_store
        app.persistence.json_store = JsonStore(base_dir=self._tmp.name)
        importlib.reload(sink_module)
        importlib.reload(archive_module)
        self.store = app.persistence.json_store
        self.globals = archive_module

        self.lake = DataLakeSinkService()
        self.archive = EventArchiveService()

        # 两个事件分区：订单与库存，均早于归档阈值。
        old = time.time() - 10 * 86400
        for event_type in ("order_created", "inventory_changed"):
            partition = os.path.join(
                self.lake._lake_base, "events", event_type, "2026", "11", "01")
            os.makedirs(partition, exist_ok=True)
            filepath = os.path.join(partition, "000000_event.json")
            with open(filepath, "w", encoding="utf-8") as fh:
                fh.write('{"event_type": "%s"}' % event_type)
            os.utime(filepath, (old, old))

    def tearDown(self) -> None:
        app.persistence.json_store = self._previous_store
        self._tmp.cleanup()

    def _policy(self, number: str, event_type: str, retention_days: int) -> int:
        entry = self.store.insert("event_archive", {
            "策略编号": number,
            "事件类型": event_type,
            "归档条件": "days>1",
            "归档路径": "/archive",
            "压缩格式": "zlib",
            "保留天数": str(retention_days),
            "清理方式": "自动删除",
            "恢复超时": "60",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        return int(entry["id"])

    def _valid(self):
        return [row for row in self.store.get_all(MANIFEST_KEY) if row.get("active")]

    def test_dedicated_policy_only_archives_its_scope(self) -> None:
        policy_id = self._policy("ORD-001", "order_created", 30)
        result = self.archive.execute_archive(policy_id)

        self.assertTrue(result["ok"])
        valid = self._valid()
        self.assertEqual([row["event_type"] for row in valid], ["order_created"])
        self.assertEqual(valid[0]["source_partition"], "events/order_created/2026/11/01")

        with open(valid[0]["archive_path"], "rb") as fh:
            content = zlib.decompress(fh.read()).decode("utf-8")
        self.assertNotIn("inventory_changed", content)
        self.assertNotIn("库存", content)

        # 库存分区没有被专用策略归档，也没有出现在清单里。
        self.assertFalse(any("inventory" in row["archive_file"] for row in self.store.get_all(MANIFEST_KEY)))

    def test_same_lake_version_keeps_single_valid_credential(self) -> None:
        generic_id = self._policy("GEN-001", "ALL", 90)
        order_id = self._policy("ORD-001", "order_created", 30)

        first = self.archive.execute_archive(generic_id)
        second = self.archive.execute_archive(order_id)
        self.assertTrue(first["ok"] and second["ok"])

        valid = self._valid()
        order_credentials = [row for row in valid if row["source_partition"].startswith("events/order_created")]
        self.assertEqual(len(order_credentials), 1, "同一落湖版本只能有一份有效凭证")
        self.assertEqual(order_credentials[0]["retention_days"], 30)
        self.assertEqual(order_credentials[0]["policy_id"], order_id)

        # 旧凭证保留为已替代，便于追溯但不是有效凭证。
        superseded = [row for row in self.store.get_all(MANIFEST_KEY)
                      if row["source_partition"] == order_credentials[0]["source_partition"]
                      and not row.get("active")]
        self.assertTrue(superseded)
        self.assertTrue(all(row["status"] != "有效" for row in superseded))

        # 通用策略覆盖的库存分区仍然各有一份有效凭证。
        inventory = [row for row in valid if row["source_partition"].startswith("events/inventory_changed")]
        self.assertEqual(len(inventory), 1)

    def test_idempotent_across_repeated_execution(self) -> None:
        policy_id = self._policy("GEN-001", "ALL", 90)
        self.archive.execute_archive(policy_id)
        second = self.archive.execute_archive(policy_id)

        self.assertEqual(second["archived"], 0)
        self.assertEqual(second["deduped"], 2)
        self.assertEqual(len(self._valid()), 2)

    def test_ledger_matches_manifest_and_restore_verifies_source(self) -> None:
        policy_id = self._policy("ORD-001", "order_created", 45)
        self.archive.execute_archive(policy_id)

        credential = self._valid()[0]
        ledger_rows = self.store.get_all(LEDGER_KEY)
        self.assertEqual(len(ledger_rows), 1)
        ledger = ledger_rows[0]
        self.assertEqual(ledger["凭证编号"], credential["credential_no"])
        self.assertEqual(ledger["源分区版本"], credential["lake_version"])
        self.assertEqual(ledger["保留日期"], credential["retention_date"])
        self.assertEqual(ledger["来源"], "事件归档清单")

        restored = self.archive.restore_from_archive(
            archive_file=credential["archive_file"],
            credential_no=credential["credential_no"],
        )
        self.assertTrue(restored["ok"])
        self.assertTrue(restored["verified"])
        self.assertEqual(restored["source"]["source_partition"], credential["source_partition"])
        self.assertEqual(restored["source"]["lake_version"], credential["lake_version"])

    def test_cleanup_uses_each_credential_retention(self) -> None:
        order_id = self._policy("ORD-001", "order_created", 5)
        generic_id = self._policy("GEN-001", "ALL", 90)
        self.archive.execute_archive(generic_id)
        self.archive.execute_archive(order_id)

        # 把两条有效凭证的归档时间统一推到 40 天前。
        aged = time.time() - 40 * 86400
        aged_iso = self.globals._iso(
            self.globals.datetime.fromtimestamp(aged, tz=self.globals.timezone.utc))
        for row in self._valid():
            self.store.update(MANIFEST_KEY, int(row["id"]), {
                "archived_at": aged_iso,
                "archived_ts": aged,
            })

        cleanup = self.archive.cleanup_expired_archives()
        self.assertEqual(cleanup["cleaned"], 1)

        remaining = self._valid()
        self.assertEqual([row["event_type"] for row in remaining], ["inventory_changed"])
        self.assertEqual(remaining[0]["retention_days"], 90)

        # 归档已到期清理时，恢复仍能给出清单中的来源信息。
        cleaned_rows = [row for row in self.store.get_all(MANIFEST_KEY)
                        if row["status"] == "已清理"]
        self.assertEqual(len(cleaned_rows), 1)
        restored = self.archive.restore_from_archive(
            credential_no=cleaned_rows[0]["credential_no"])
        self.assertFalse(restored["ok"])
        self.assertEqual(restored["source"]["lake_version"], cleaned_rows[0]["lake_version"])

        # 台账同步记录了清理结果，保留日期仍等于清单里的保留日期。
        ledger = [row for row in self.store.get_all(LEDGER_KEY)
                  if row["凭证编号"] == cleaned_rows[0]["credential_no"]][0]
        self.assertEqual(ledger["清理结果"], "已清理")
        self.assertEqual(ledger["保留日期"], cleaned_rows[0]["retention_date"])

    def test_historical_credential_looks_up_policy_retention(self) -> None:
        policy_id = self._policy("ORD-001", "order_created", 7)

        # 历史归档文件：没有清单记录，只有按事件类型分目录的旧文件。
        legacy_dir = os.path.join(self.archive._archive_base, "order_created")
        os.makedirs(legacy_dir, exist_ok=True)
        legacy_file = os.path.join(legacy_dir, "arch_20260101_order.json.zlib")
        with open(legacy_file, "wb") as fh:
            fh.write(zlib.compress(b"legacy"))
        old = time.time() - 30 * 86400
        os.utime(legacy_file, (old, old))

        cleanup = self.archive.cleanup_expired_archives(retention_days=365)

        # 保留期回到策略归属（7 天），而不是执行本次清理时传入的兜底值。
        self.assertEqual(cleanup["cleaned"], 1)
        self.assertFalse(os.path.exists(legacy_file))
        details = [item for item in cleanup["details"] if item.get("source") == "historical"]
        self.assertEqual(details[0]["retention_days"], 7)
        self.assertEqual(details[0]["retention_source"], "policy")

        ledger = [row for row in self.store.get_all(LEDGER_KEY)
                  if row["来源"] == "历史凭证"]
        self.assertTrue(ledger)
        self.assertEqual(ledger[0]["策略归属"], f"policy:{policy_id}")
        self.assertEqual(ledger[0]["清理结果"], "已清理")


if __name__ == "__main__":
    unittest.main()
