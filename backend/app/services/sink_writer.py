"""结果落盘服务：写入完成后触发 iceberg_commit 原子提交和 data_diff 比对。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkWriterService(BaseModuleService):
    MODULE = "sink_writer"
    ENTITY = "落盘管道"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["管道编号", "数据流", "目标载体", "写入模式", "分区列", "批次大小", "压缩格式", "写入速率"]
    REQUIRED_FIELDS = ["管道编号", "数据流", "目标载体"]
    STATUS_ORDER = ["草稿", "写入中", "写入阻塞", "已停止"]
    ACTION_RULES = {"启动写入": "写入中", "解除阻塞": "写入阻塞", "停止写入": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_iceberg_commit(self, entry_id: int) -> dict[str, Any] | None:
        """落盘管道写入中时，触发 iceberg_commit 原子提交快照。

        数据流链路: sink_writer -> iceberg_commit
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入中":
            return None

        commit_rows = store.rows("iceberg_commit")
        new_id = max((int(r.get("id", 0)) for r in commit_rows), default=0) + 1
        commit = {
            "id": new_id,
            "快照编号": f"ICEB-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "数据文件": f"s3://warehouse/data/{entry.get('管道编号', 'default')}/part-00001.parquet",
            "清单文件": f"metadata/manifest-{new_id}.avro",
            "快照ID": f"{new_id * 1000 + 500}",
            "提交状态": "待提交",
            "影响行数": 128340,
            "提交时间": "2026-11-18 10:32:45",
            "status": "提交中",
            "pending": True,
            "abnormal": False,
        }
        commit_rows.append(commit)
        return commit

    def trigger_block_cleanup(self, entry_id: int) -> dict[str, Any] | None:
        """写入阻塞时触发 compaction 合并小文件。

        数据流链路: sink_writer(写入阻塞) -> compaction
        启动登记与确认收拢在 CompactionService，这里只保留受理条件与原返回结果
        （仍返回合并任务本身）。
        """
        started = self.start_block_cleanup(entry_id)
        return started["task"] if started else None

    def start_block_cleanup(self, entry_id: int, *,
                            confirm: bool = True) -> dict[str, Any] | None:
        """阻塞清理受理：返回任务、启动登记和确认回执，供结果落盘识别受理结果。"""
        from app.services.compaction import CompactionService

        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入阻塞":
            return None
        return CompactionService().start_from_block_cleanup(entry, confirm=confirm)