"""Persist the identities and user order of task control operations."""

import asyncio

from fastapi import HTTPException
from prefect.variables import Variable


class Operations:
    def __init__(self):
        # The isolated control service uses one API process. Do not hold this
        # across dependency checks or the execution of a Prefect flow.
        self.lock = asyncio.Lock()
        # Only submission bookkeeping for this single API process. Native flow
        # states and the durable recovery manifest remain authoritative.
        self.recovery_dispatches = {}
        self.deleting = set()
        self.delete_requests = set()
        self.active = {}

    async def read(self, request_id):
        return await Variable.aget("paperflow_v3_operation_" + str(request_id).replace("-", "_"))

    async def save(self, operation):
        target = operation.get("target") or {}
        if target.get("task_id") in self.deleting and operation["action"] != "delete":
            return
        await Variable.aset("paperflow_v3_operation_" + operation["request_id"].replace("-", "_"),
                            operation, overwrite=True)

    async def latest(self, task_id):
        return await Variable.aget("paperflow_v3_intent_" + str(task_id).replace("-", "_"))

    async def begin_control(self, action, request_id, intent_seq, task_id):
        """Register inside lock; a repeated identity never repeats its effect."""
        current_delete = await self.latest(task_id)
        if action != "delete" and (str(task_id) in self.deleting or
                current_delete and current_delete.get("action") == "delete"):
            raise HTTPException(409, "Task is being deleted")
        previous = await self.read(request_id)
        target = {"task_id": str(task_id)}
        if previous:
            if (previous["action"] != action or previous["target"] != target
                    or previous.get("intent_seq") != intent_seq):
                raise HTTPException(409, "操作请求身份已用于其他操作，需核对")
            return previous, False
        current = await self.latest(task_id)
        if current and current["request_id"] == str(request_id):
            if (current["intent_seq"] != intent_seq or current.get("action", action) != action
                    or current.get("target", target) != target):
                raise HTTPException(409, "操作请求身份已用于其他操作，需核对")
            # The intent was saved but its operation record was not. Its effect
            # is unknown; the same identity must never be executed again.
            return {"request_id": str(request_id), "action": action, "target": target,
                    "intent_seq": intent_seq, "status": "unknown"}, False
        status = "accepted"
        if current and intent_seq <= current["intent_seq"]:
            status = "superseded" if intent_seq < current["intent_seq"] else "rejected"
        operation = {"request_id": str(request_id), "action": action, "target": target,
                     "intent_seq": intent_seq, "status": status}
        if status == "accepted":
            await Variable.aset("paperflow_v3_intent_" + str(task_id).replace("-", "_"),
                                {"request_id": str(request_id), "intent_seq": intent_seq,
                                 "action": action, "target": target}, overwrite=True)
        await self.save(operation)
        return operation, status == "accepted"

    async def view(self, operation):
        status = operation["status"]
        if "intent_seq" in operation:
            current = await self.latest(operation["target"]["task_id"])
            if current and current["intent_seq"] > operation["intent_seq"]:
                status = "superseded"
        return {**operation, "status": status, "found": True,
                "resolved": status in {"confirmed", "rejected", "superseded"},
                "accepted": status != "rejected"}
