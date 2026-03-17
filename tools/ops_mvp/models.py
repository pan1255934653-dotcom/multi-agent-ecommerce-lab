from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DecisionRecord:
    decision_id: str
    decision_type: str
    title: str
    priority: str
    risk_level: str
    status: str
    reason: str
    source_type: str = "trend_signal"
    source_tier: str = "medium"
    result_layer: str = "hint"
    source_mode: str = "mock"
    captured_at: str = ""
    published_at: str = ""
    freshness_window: str = "72h"
    stale_after: str = ""
    freshness_rule: str = "trend"
    freshness_score: float = 1.0
    is_stale: bool = False
    market: str = "PH"
    category: str = "printed_tshirt"
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunResult:
    summary: str
    signals: list[DecisionRecord]
    tasks: list[DecisionRecord]
    action_proposals: list[DecisionRecord]
    auto_actions: list[DecisionRecord]
    approval_queue: list[DecisionRecord]
    formal_conclusions: list[DecisionRecord]
    hint_layer: list[DecisionRecord]
    observation_layer: list[DecisionRecord]
    stale_background: list[DecisionRecord]
    fresh_active_signals: list[DecisionRecord]
    layer_summary: dict[str, Any]
    freshness_summary: dict[str, Any]
    source_summary: dict[str, Any]
    scan_agents: dict[str, Any]
    scan_agent_summary: dict[str, Any]
    fetch_status: dict[str, Any]
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "signals": [item.to_dict() for item in self.signals],
            "tasks": [item.to_dict() for item in self.tasks],
            "action_proposals": [item.to_dict() for item in self.action_proposals],
            "auto_actions": [item.to_dict() for item in self.auto_actions],
            "approval_queue": [item.to_dict() for item in self.approval_queue],
            "formal_conclusions": [item.to_dict() for item in self.formal_conclusions],
            "hint_layer": [item.to_dict() for item in self.hint_layer],
            "observation_layer": [item.to_dict() for item in self.observation_layer],
            "stale_background": [item.to_dict() for item in self.stale_background],
            "fresh_active_signals": [item.to_dict() for item in self.fresh_active_signals],
            "layer_summary": self.layer_summary,
            "freshness_summary": self.freshness_summary,
            "source_summary": self.source_summary,
            "scan_agents": self.scan_agents,
            "scan_agent_summary": self.scan_agent_summary,
            "fetch_status": self.fetch_status,
            "generated_at": self.generated_at,
        }
