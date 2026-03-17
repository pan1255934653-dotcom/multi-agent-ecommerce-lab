from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class AuditLogger:
    def __init__(self, log_dir: str | Path):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.decision_log = self.log_dir / "decisions.jsonl"
        self.audit_log = self.log_dir / "audit.jsonl"
        self.tasks_log = self.log_dir / "task_status.jsonl"
        self.approval_log = self.log_dir / "approvals.jsonl"

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        payload = {"timestamp": datetime.now().isoformat(timespec="seconds"), **payload}
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def log_decision(self, payload: dict[str, Any]) -> None:
        self._write(self.decision_log, payload)

    def log_audit(self, payload: dict[str, Any]) -> None:
        self._write(self.audit_log, payload)

    def log_task(self, payload: dict[str, Any]) -> None:
        self._write(self.tasks_log, payload)

    def log_approval(self, payload: dict[str, Any]) -> None:
        self._write(self.approval_log, payload)
