"""小文件合并服务：合并后级联Iceberg过期清理和Metric聚合更新。

工作台手工「启动合并」与结果落盘阻塞清理的启动登记都收拢到这里：
先按可核对的关联识别任务，再写一条统一的启动登记，最后原子确认启动。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

REGISTRY = "compaction_start_registrations"

ACTOR_NAMES = {
    "workbench": "工作台",
    "sink_block": "结果落盘阻塞清理",
}

TERMINAL_STATUSES = ("已完成", "合并失败")


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存", "合并耗时", "释放空间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "合并中", "取消合并": "已完成", "标记失败": "合并失败"}
    NEGATIVE_ACTIONS = []

    # 启动登记的确认必须是原子的：并发的手工启动和阻塞续办只能有一侧写入。
    _start_lock = threading.RLock()

    # ---- 共用：任务识别与登记写法 ----

    @staticmethod
    def block_correlation_key(sink_entry: dict[str, Any]) -> str:
        """阻塞清理与合并任务之间唯一可核对的关联键。

        只用落盘记录自身编号做关联，绝不按目标表猜测先前任务。
        """
        return f"sink_block:sink_writer#{int(sink_entry.get('id', 0))}"

    def _registrations(self) -> list[dict[str, Any]]:
        return store.ledger(REGISTRY)

    def _next_registration_id(self) -> int:
        return max((int(row.get("id", 0)) for row in self._registrations()), default=0) + 1

    def _find_registration(self, *, task_id: int | None = None,
                           key: str | None = None) -> dict[str, Any] | None:
        """按关联键或任务编号找最近一笔启动登记，两条线索都必须是可核对的。"""
        if key:
            matched = [row for row in self._registrations() if row.get("关联键") == key]
            return matched[-1] if matched else None
        if task_id is not None:
            matched = [row for row in self._registrations() if int(row.get("task_id", 0)) == task_id]
            return matched[-1] if matched else None
        return None

    def recognize_task(self, key: str) -> dict[str, Any] | None:
        """按关联键识别任务：先看登记，再看任务上留存的关联键。

        缺少可核对的关联时返回 None，不把任意同表任务当作先前那条。
        """
        registration = self._find_registration(key=key)
        if registration is not None:
            task = self.get_entry(int(registration.get("task_id", 0)))
            if task is not None:
                return task
        for row in store.rows(self.MODULE):
            if row.get("关联键") == key:
                return row
        return None

    def _write_registration(self, task: dict[str, Any], *, source: str,
                            key: str | None) -> dict[str, Any]:
        """统一启动登记写法：待确认登记 + 续办入口 + 后续登记留痕。"""
        registrations = self._registrations()
        new_id = self._next_registration_id()
        registration = {
            "id": new_id,
            "登记编号": f"CMPREG-{new_id:04d}",
            "task_id": int(task.get("id", 0)),
            "任务编号": task.get("任务编号"),
            "目标表": task.get("目标表"),
            "来源": source,
            "关联键": key,
            "status": "待确认",
            "确认方": None,
            "确认入口": None,
            "确认时间": None,
            "未写入方": [],
            "后续登记": None,
            "后续登记时间": None,
            "续办入口": self._resume_entry(task, source, key),
            "登记时间": self._now(),
        }
        registrations.append(registration)
        return registration

    def _resume_entry(self, task: dict[str, Any], source: str,
                      key: str | None) -> dict[str, Any]:
        """未确认登记仍可续办的入口：原入口、原参数。"""
        if source == "sink_block" and key:
            sink_id = key.rsplit("#", 1)[-1]
            return {
                "method": "POST",
                "path": "/api/flows/sink-block-to-compaction",
                "params": {"sink_entry_id": int(sink_id)},
                "说明": "重送阻塞清理请求，按原关联找到本条任务继续确认",
            }
        return {
            "method": "POST",
            "path": f"/api/compaction/{int(task.get('id', 0))}/actions",
            "body": {"action": "启动合并"},
            "说明": "对原合并任务再次执行启动合并以确认启动",
        }

    def _confirm_entry(self, task: dict[str, Any], actor: str,
                       key: str | None = None) -> dict[str, Any]:
        """实际确认启动所走的入口，供事后反查。"""
        if actor == "sink_block" and key:
            sink_id = key.rsplit("#", 1)[-1]
            return {
                "method": "POST",
                "path": "/api/flows/sink-block-to-compaction",
                "params": {"sink_entry_id": int(sink_id)},
            }
        return {
            "method": "POST",
            "path": f"/api/compaction/{int(task.get('id', 0))}/actions",
            "body": {"action": "启动合并"},
        }

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _receipt(self, task: dict[str, Any], registration: dict[str, Any] | None,
                 *, state: str, actor: str, reason: str | None = None,
                 confirmable: bool = True) -> dict[str, Any]:
        """确认回执：实际确认启动的一侧、未写入的一侧、续办入口。"""
        confirmed_by = registration.get("确认方") if registration else None
        not_written_by = list(registration.get("未写入方", [])) if registration else []
        # 未写入方指本次没有真正写入的一侧；已确认方自己的重送不算另一侧。
        if state == "reused" and actor != confirmed_by and actor not in not_written_by:
            not_written_by.append(actor)
            if registration is not None:
                registration["未写入方"] = list(not_written_by)
        resume_entry = None
        if state == "pending" and confirmable and registration is not None:
            resume_entry = registration.get("续办入口")
        return {
            "登记编号": registration.get("登记编号") if registration else None,
            "task_id": int(task.get("id", 0)),
            "任务编号": task.get("任务编号"),
            "关联键": registration.get("关联键") if registration else None,
            "state": state,
            "受理结果": {"started": "实际启动", "reused": "复用", "pending": "待续"}[state],
            "本侧": actor,
            "本侧名称": ACTOR_NAMES.get(actor, actor),
            "确认方": confirmed_by,
            "confirmed_by": confirmed_by,
            "确认方名称": ACTOR_NAMES.get(confirmed_by, confirmed_by) if confirmed_by else None,
            "确认入口": registration.get("确认入口") if registration else None,
            "confirm_entry": registration.get("确认入口") if registration else None,
            "未写入方": not_written_by,
            "not_written_by": not_written_by,
            "登记状态": registration.get("status") if registration else None,
            "任务状态": task.get("status"),
            "后续登记": registration.get("后续登记") if registration else None,
            "续办入口": resume_entry,
            "resume_entry": resume_entry,
            "说明": reason,
        }

    def _mark_confirmed(self, task: dict[str, Any], registration: dict[str, Any],
                        *, actor: str, key: str | None = None) -> None:
        """原子确认一笔启动登记：登记先落已确认，再写任务状态。"""
        registration["status"] = "已确认"
        registration["确认方"] = actor
        registration["确认时间"] = self._now()
        registration["确认入口"] = self._confirm_entry(task, actor, key or registration.get("关联键"))
        registration["续办入口"] = None
        task["status"] = self.ACTION_RULES["启动合并"]
        task["pending"] = True
        task["abnormal"] = False

    def _mark_unconfirmed(self, registration: dict[str, Any], reason: str) -> None:
        registration["status"] = "已失效"
        registration["续办入口"] = None
        registration["失效原因"] = reason

    def _record_followup(self, task: dict[str, Any], action: str) -> None:
        """取消/标记失败也算后续登记，晚到的续办不得覆盖。"""
        registration = self._find_registration(task_id=int(task.get("id", 0)))
        if registration is None:
            return
        registration["后续登记"] = self.ACTION_RULES.get(action)
        registration["后续登记时间"] = self._now()

    # ---- 阻塞清理受理（两段式，便于断点续办） ----

    def accept_block_cleanup(self, sink_entry: dict[str, Any]) -> dict[str, Any]:
        """落盘阻塞清理的第一步：识别/登记合并任务并写待确认启动登记。

        只做受理，不确认启动；确认由 confirm_startup 完成。
        """
        with self._start_lock:
            key = self.block_correlation_key(sink_entry)
            task = self.recognize_task(key)
            created = False
            if task is None:
                comp_rows = store.rows(self.MODULE)
                new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
                task = {
                    "id": new_id,
                    "任务编号": f"COMP-{new_id:04d}",
                    "目标表": str(sink_entry.get("目标载体", "unknown")),
                    "文件数量": 85,
                    "合并策略": "BinPack-按大小合并",
                    "触发阈值": "文件数 >= 50",
                    "执行内存": "2GB",
                    "合并耗时": "未执行",
                    "释放空间": "未执行",
                    "status": self.STATUS_ORDER[0],
                    "pending": True,
                    "abnormal": False,
                    "关联键": key,
                }
                comp_rows.append(task)
                created = True

            registration = self._find_registration(key=key)
            if registration is None or int(registration.get("task_id", 0)) != int(task.get("id", 0)):
                # 同一任务若已有一笔确认过的启动登记，重送只做复用，不再写待确认登记。
                by_task = self._find_registration(task_id=int(task.get("id", 0)))
                if by_task is not None and by_task.get("status") == "已确认":
                    registration = by_task
                else:
                    registration = self._write_registration(task, source="sink_block", key=key)
            return {"task": task, "registration": registration, "created": created}

    def confirm_startup(self, task: dict[str, Any], *,
                        actor: str = "sink_block",
                        key: str | None = None) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
        """原子确认启动。同一笔登记只会有一侧真正写入「合并中」。

        - 待确认：可确认，返回 started；
        - 已确认/已失效：复用原任务，不覆盖已确认启动或随后登记的失败；
        - 任务已取消/失败：登记失效，晚到续办不写入。
        """
        with self._start_lock:
            task_id = int(task.get("id", 0))
            current = self.get_entry(task_id)
            if current is None:
                receipt = self._receipt(task, None, state="reused", actor=actor,
                                        reason=f"合并任务 {task_id} 不存在或已归档")
                return None, receipt["说明"], receipt
            if key and current.get("关联键") != key:
                receipt = self._receipt(current, None, state="reused", actor=actor,
                                        reason="任务关联与本次登记不一致")
                return current, receipt["说明"], receipt

            registration = self._find_registration(key=key) if key else None
            if registration is None:
                registration = self._find_registration(task_id=task_id)
            if registration is not None and int(registration.get("task_id", 0)) != task_id:
                receipt = self._receipt(current, None, state="reused", actor=actor,
                                        reason="登记的关联任务与本次不一致")
                return current, receipt["说明"], receipt

            # 已确认：复用，另一侧记为未写入；绝不改动任务状态。
            if registration is not None and registration.get("status") == "已确认":
                receipt = self._receipt(current, registration, state="reused", actor=actor,
                                        reason="该笔启动登记已由"
                                               f"{ACTOR_NAMES.get(registration.get('确认方'), registration.get('确认方'))}确认")
                return current, f"{self.ENTITY}已启动合并（复用已确认的启动，本次未写入）", receipt

            # 已失效：晚到续办不再写入，避免覆盖随后的取消/失败。
            if registration is not None and registration.get("status") == "已失效":
                receipt = self._receipt(current, registration, state="reused", actor=actor,
                                        reason=registration.get("失效原因") or "启动登记已失效")
                return current, f"{self.ENTITY}启动登记已失效，本次未写入", receipt

            # 有未确认登记但任务已结束：登记失效，不覆盖随后的取消/失败登记。
            # 没有任何登记时，手工启动仍按原有口径直接受理。
            if current.get("status") in TERMINAL_STATUSES and registration is not None:
                self._mark_unconfirmed(
                    registration,
                    f"任务已{current.get('status')}，晚到续办不得覆盖")
                receipt = self._receipt(current, registration, state="reused", actor=actor,
                                        reason=registration["失效原因"])
                return current, f"{self.ENTITY}已{current.get('status')}，本次未写入", receipt

            if registration is None:
                registration = self._write_registration(current, source=actor, key=key)
            self._mark_confirmed(current, registration, actor=actor, key=key)
            receipt = self._receipt(current, registration, state="started", actor=actor)
            return current, f"{self.ENTITY}已启动合并", receipt

    def start_from_block_cleanup(self, sink_entry: dict[str, Any],
                                 *, confirm: bool = True) -> dict[str, Any] | None:
        """阻塞清理完整受理：受理登记 + 原子确认启动。

        confirm=False 时只完成受理与登记，返回「待续」回执；续办入口按原参数重放。
        """
        accepted = self.accept_block_cleanup(sink_entry)
        key = self.block_correlation_key(sink_entry)
        if not confirm:
            receipt = self._receipt(accepted["task"], accepted["registration"],
                                    state="pending", actor="sink_block")
            return {
                "task": accepted["task"],
                "registration": accepted["registration"],
                "receipt": receipt,
                "state": receipt["state"],
                "message": "合并任务已受理，启动待确认",
            }
        task, message, receipt = self.confirm_startup(
            accepted["task"], actor="sink_block", key=key)
        if task is None:
            return None
        return {
            "task": task,
            "registration": accepted["registration"],
            "receipt": receipt,
            "state": receipt["state"],
            "message": message,
        }

    # ---- 工作台手工启动 ----

    def start_manual(self, entry_id: int
                     ) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        """工作台「启动合并」：对指定任务确认启动，遇到同一笔登记则原子合并。

        返回 (entry, message, receipt)；失败时回执放在第三个位置。
        """
        with self._start_lock:
            entry = self.get_entry(entry_id)
            if entry is None:
                receipt = self._receipt(entry or {}, None, state="reused", actor="workbench",
                                        reason=f"{self.ENTITY} {entry_id} 不存在或已归档")
                return None, receipt["说明"], receipt
            task, message, receipt = self.confirm_startup(entry, actor="workbench")
            return task, message, receipt

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message, _ = self.run_action_detail(entry_id, action)
        return entry, message

    def run_action_detail(self, entry_id: int, action: str
                          ) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        if action not in self.ACTION_RULES:
            entry, message = super().run_action(entry_id, action)
            return entry, message, None
        with self._start_lock:
            if action == "启动合并":
                return self.start_manual(entry_id)
            entry, message = super().run_action(entry_id, action)
            if entry is not None:
                self._record_followup(entry, action)
            return entry, message, None

    # ---- 登记查询（续办与反查） ----

    def startup_state(self, entry_id: int) -> dict[str, Any] | None:
        task = self.get_entry(entry_id)
        if task is None:
            return None
        registration = self._find_registration(task_id=entry_id)
        state = "none"
        if registration is not None:
            status = registration.get("status")
            if status == "已确认":
                state = "started"
            elif status == "待确认":
                state = "pending"
            else:
                state = "reused"
        return {
            "task": task,
            "registration": registration,
            "start_state": state,
            "confirmed_by": registration.get("确认方") if registration else None,
            "confirm_entry": registration.get("确认入口") if registration else None,
            "not_written_by": registration.get("未写入方", []) if registration else [],
            "resume_entry": (
                registration.get("续办入口")
                if registration is not None
                and registration.get("status") == "待确认"
                and task.get("status") not in TERMINAL_STATUSES
                else None
            ),
        }

    # ---- 合并完成后的既有后续处理 ----

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """合并完成后触发expire_snap清理和metric_aggr聚合更新。

        数据流链路: compaction -> expire_snap + metric_aggr
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"expire_snap": None, "metric_aggr": None}

        # 触发 expire_snap
        for exp in store.rows("expire_snap"):
            if str(exp.get("目标表", "")) == table:
                exp["status"] = "预演完成"
                exp["pending"] = True
                exp["abnormal"] = False
                exp["预演结果"] = f"可清理{entry.get('文件数量', 0) * 3}个历史快照"
                results["expire_snap"] = exp
                break

        # 创建 metric_aggr 聚合
        metric_rows = store.rows("metric_aggr")
        new_metric_id = max((int(r.get("id", 0)) for r in metric_rows), default=0) + 1
        metric = {
            "id": new_metric_id,
            "窗口编号": f"METR-{new_metric_id:04d}",
            "数据流": table,
            "聚合键": "合并批次",
            "聚合函数": "SUM(file_count), AVG(file_size)",
            "窗口宽度": "1h",
            "输出频率": "每次合并后",
            "延迟更新": "实时",
            "最后写入": "2026-11-18 10:42:00",
            "status": "聚合中",
            "pending": True,
            "abnormal": False,
        }
        metric_rows.append(metric)
        results["metric_aggr"] = metric

        return results
