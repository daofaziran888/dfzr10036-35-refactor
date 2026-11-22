"""小文件合并启动登记回归测试：直接跑领域服务，不依赖额外测试依赖。

运行：.venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest

from app.services.compaction import CompactionService
from app.services.orchestrator import flow_sink_block_to_compaction
from app.services.sink_writer import SinkWriterService
from app.store import store


def reset_store() -> None:
    from app.seed import SEED_ROWS

    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._ledgers = {}


def add_blocked_sink(sink_id: int, table: str) -> dict:
    row = {
        "id": sink_id, "status": "写入阻塞", "pending": True, "abnormal": True,
        "管道编号": f"SK-{sink_id:04d}", "数据流": f"stream_{sink_id}",
        "目标载体": table, "写入模式": "APPEND",
    }
    store.rows("sink_writer").append(row)
    return row


class CompactionStartupTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = CompactionService()
        self.sink = SinkWriterService()

    def test_block_cleanup_keeps_original_fields_and_starts_once(self) -> None:
        response = flow_sink_block_to_compaction(2)

        self.assertTrue(response["ok"])
        self.assertEqual(response["trace"], ["sink_writer#2 -> compaction#3"])
        self.assertEqual(response["call_count"], 1)
        self.assertEqual(response["state"], "started")
        self.assertEqual(response["entry"]["status"], "合并中")
        self.assertEqual(response["entry"]["目标表"], "ods.payment_log_iceberg")
        self.assertEqual(response["entry"]["文件数量"], 85)
        self.assertEqual(response["entry"]["合并策略"], "BinPack-按大小合并")

        resent = flow_sink_block_to_compaction(2)
        self.assertEqual(resent["state"], "reused")
        self.assertEqual(resent["task_id"], response["task_id"])
        self.assertEqual(
            [row["id"] for row in store.rows("compaction")
             if row.get("关联键") == "sink_block:sink_writer#2"],
            [response["task_id"]],
        )

    def test_non_blocked_sink_is_not_accepted(self) -> None:
        before = len(store.rows("compaction"))
        self.assertEqual(flow_sink_block_to_compaction(1)["call_count"], 0)
        self.assertEqual(len(store.rows("compaction")), before)
        self.assertIsNone(self.sink.trigger_block_cleanup(1))

    def test_workbench_start_keeps_original_behavior(self) -> None:
        entry, message = self.service.run_action(1, "启动合并")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "合并中")
        self.assertEqual(message, "合并任务已启动合并")

        failed, message = self.service.run_action(2, "标记失败")
        self.assertEqual(failed["status"], "合并失败")
        self.assertFalse(failed["abnormal"])
        cancelled, message = self.service.run_action(2, "取消合并")
        self.assertEqual(cancelled["status"], "已完成")

    def test_block_cleanup_then_workbench_start_is_atomic(self) -> None:
        accepted = self.service.accept_block_cleanup(self.sink.get_entry(2))
        task_id = accepted["task"]["id"]
        self.assertEqual(accepted["task"]["status"], "待执行")
        self.assertEqual(accepted["registration"]["status"], "待确认")

        entry, message, receipt = self.service.start_manual(task_id)
        self.assertEqual(entry["status"], "合并中")
        self.assertEqual(receipt["state"], "started")
        self.assertEqual(receipt["confirmed_by"], "workbench")

        late = flow_sink_block_to_compaction(2)
        self.assertEqual(late["state"], "reused")
        self.assertEqual(late["确认方"], "workbench")
        self.assertEqual(late["未写入方"], ["sink_block"])
        self.assertEqual(late["任务状态"], "合并中")
        self.assertEqual(store.find("compaction", task_id)["status"], "合并中")

        state = self.service.startup_state(task_id)
        self.assertEqual(state["confirmed_by"], "workbench")
        self.assertEqual(state["not_written_by"], ["sink_block"])
        self.assertEqual(
            state["confirm_entry"]["body"], {"action": "启动合并"})

    def test_concurrent_start_confirms_exactly_once(self) -> None:
        add_blocked_sink(77, "ods.concurrent_iceberg")
        accepted = self.service.accept_block_cleanup(self.sink.get_entry(77))
        task_id = accepted["task"]["id"]

        receipts = []
        barrier = threading.Barrier(2)

        def from_block() -> None:
            barrier.wait()
            _, _, receipt = self.service.confirm_startup(
                store.find("compaction", task_id),
                actor="sink_block", key="sink_block:sink_writer#77")
            receipts.append(receipt)

        def from_workbench() -> None:
            barrier.wait()
            _, _, receipt = self.service.start_manual(task_id)
            receipts.append(receipt)

        threads = [threading.Thread(target=from_block), threading.Thread(target=from_workbench)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        states = sorted(receipt["state"] for receipt in receipts)
        self.assertEqual(states, ["reused", "started"])
        started = next(receipt for receipt in receipts if receipt["state"] == "started")
        reused = next(receipt for receipt in receipts if receipt["state"] == "reused")
        self.assertEqual(reused["confirmed_by"], started["本侧"])
        self.assertEqual(reused["not_written_by"], [reused["本侧"]])
        self.assertEqual(store.find("compaction", task_id)["status"], "合并中")
        self.assertEqual(
            [row for row in store.ledger("compaction_start_registrations")
             if row["task_id"] == task_id and row["status"] == "已确认"],
            [self.service.startup_state(task_id)["registration"]],
        )

    def test_pending_registration_exposes_resume_entry(self) -> None:
        add_blocked_sink(41, "tbl.a")
        accepted = self.service.accept_block_cleanup(self.sink.get_entry(41))
        task_id = accepted["task"]["id"]

        state = self.service.startup_state(task_id)
        self.assertEqual(state["start_state"], "pending")
        self.assertEqual(state["resume_entry"]["path"], "/api/flows/sink-block-to-compaction")
        self.assertEqual(state["resume_entry"]["params"], {"sink_entry_id": 41})

        resumed = self.service.start_from_block_cleanup(self.sink.get_entry(41))
        self.assertEqual(resumed["state"], "started")
        self.assertEqual(resumed["task"]["id"], task_id)
        self.assertIsNone(self.service.startup_state(task_id)["resume_entry"])

    def test_late_resume_does_not_override_failure(self) -> None:
        add_blocked_sink(41, "tbl.a")
        accepted = self.service.accept_block_cleanup(self.sink.get_entry(41))
        task_id = accepted["task"]["id"]

        self.service.run_action(task_id, "标记失败")
        late = self.service.start_from_block_cleanup(self.sink.get_entry(41))
        self.assertEqual(late["state"], "reused")
        self.assertEqual(store.find("compaction", task_id)["status"], "合并失败")

        state = self.service.startup_state(task_id)
        self.assertEqual(state["start_state"], "reused")
        self.assertEqual(state["registration"]["后续登记"], "合并失败")

    def test_pending_resume_does_not_override_cancel(self) -> None:
        add_blocked_sink(41, "tbl.a")
        accepted = self.service.accept_block_cleanup(self.sink.get_entry(41))
        task_id = accepted["task"]["id"]

        self.service.run_action(task_id, "取消合并")
        resumed = self.service.start_from_block_cleanup(self.sink.get_entry(41), confirm=False)
        self.assertEqual(resumed["state"], "pending")
        # 续办重放也不能把已取消的任务写回合并中
        replayed = flow_sink_block_to_compaction(41)
        self.assertEqual(replayed["state"], "reused")
        self.assertEqual(store.find("compaction", task_id)["status"], "已完成")

    def test_same_table_records_stay_independent(self) -> None:
        add_blocked_sink(41, "tbl.a")
        add_blocked_sink(42, "tbl.a")

        first = flow_sink_block_to_compaction(41)
        second = flow_sink_block_to_compaction(42)

        self.assertEqual(first["state"], "started")
        self.assertEqual(second["state"], "started")
        self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertEqual(
            [row["id"] for row in store.rows("compaction")
             if row.get("目标表") == "tbl.a"],
            [first["task_id"], second["task_id"]],
        )
        # 重送第一条仍指向第一条，不会串到第二条
        self.assertEqual(flow_sink_block_to_compaction(41)["task_id"], first["task_id"])

    def test_no_correlation_does_not_borrow_same_table_task(self) -> None:
        # 种子里 ods.payment_log_iceberg 已有 id=2 的手工任务
        self.assertIsNone(self.service.recognize_task("sink_block:sink_writer#2"))
        first = flow_sink_block_to_compaction(2)
        self.assertNotEqual(first["task_id"], 2)
        self.assertEqual(self.sink.get_entry(2)["status"], "写入阻塞")

    def test_existing_manual_task_remains_startable(self) -> None:
        entry, message, receipt = self.service.start_manual(2)
        self.assertEqual(entry["status"], "合并中")
        self.assertEqual(receipt["state"], "started")
        self.assertEqual(receipt["confirm_entry"]["body"], {"action": "启动合并"})
        # 手工任务没有关联键，同表阻塞清理不会认领它
        started = flow_sink_block_to_compaction(2)
        self.assertNotEqual(started["task_id"], 2)


if __name__ == "__main__":
    unittest.main()
