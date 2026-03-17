from __future__ import annotations

import json
from pathlib import Path

from .adapters import DataRegistry
from .engine import DecisionEngine
from .fetchers import ExternalSignalFetcher
from .logging_utils import AuditLogger


class OpsMVPService:
    def __init__(self, workspace_root: str | Path):
        self.workspace_root = Path(workspace_root)
        self.mock_dir = self.workspace_root / "mock"
        self.runtime_dir = self.workspace_root / "runtime" / "ops_mvp"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.logger = AuditLogger(self.runtime_dir / "logs")

    def run_cycle(self) -> dict:
        dataset = DataRegistry(self.mock_dir).load_all()
        fetch_status = {
            "weather": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "rule": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "news": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_search": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
        }
        try:
            fetched = ExternalSignalFetcher().fetch()
            if isinstance(fetched, tuple):
                live_events, live_fetch_status = fetched
            else:
                live_events, live_fetch_status = fetched, {}
            dataset["market_events"].extend(live_events)
            fetch_status.update(live_fetch_status)
        except Exception as exc:
            error_text = str(exc)
            fetch_status = {
                "weather": {"status": "fetch_failed", "source_mode": "mock", "error": error_text, "fallback_used": True},
                "rule": {"status": "fetch_failed", "source_mode": "mock", "error": error_text, "fallback_used": True},
                "news": {"status": "fetch_failed", "source_mode": "mock", "error": error_text, "fallback_used": True},
                "competitor_search": {"status": "fetch_failed", "source_mode": "mock", "error": error_text, "fallback_used": True},
            }
        result = DecisionEngine(dataset, fetch_status=fetch_status).run()
        payload = {
            "dataset": dataset,
            "result": result.to_dict(),
        }
        output_path = self.runtime_dir / "latest_run.json"
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        for item in result.signals:
            self.logger.log_decision({"kind": "signal", **item.to_dict()})
        for item in result.tasks:
            self.logger.log_task({"kind": "task", **item.to_dict()})
        for item in result.action_proposals:
            self.logger.log_audit({"kind": "action", **item.to_dict()})
            if item.status == "pending_approval":
                self.logger.log_approval({"kind": "approval_queue", **item.to_dict()})
            else:
                self.logger.log_approval({"kind": "auto_execution", **item.to_dict()})
        return payload
