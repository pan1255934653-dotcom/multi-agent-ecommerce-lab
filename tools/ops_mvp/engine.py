from __future__ import annotations

from datetime import datetime, timedelta

from .models import DecisionRecord, RunResult


class DecisionEngine:
    MARKET = "PH"
    CATEGORY = "printed_tshirt"
    SCAN_AGENT_SPECS = {
        "market_radar": {
            "label": "市场雷达 agent",
            "responsibilities": ["weather", "news", "inventory_watch", "ad_health"],
        },
        "competitor_watch": {
            "label": "竞品巡检 agent",
            "responsibilities": ["competitor", "creator_watch", "price_pressure"],
        },
        "trend_scout": {
            "label": "爆款线索 agent",
            "responsibilities": ["trend_signal", "motif_watch", "demand_clues"],
        },
        "external_environment": {
            "label": "外部环境 agent",
            "responsibilities": ["policy", "platform_rule", "compliance_risk"],
        },
        "daily_brief_synth": {
            "label": "日报汇总 agent",
            "responsibilities": ["cross_agent_rollup", "prioritization", "briefing"],
        },
    }
    DEFAULT_FRESHNESS_WINDOWS = {
        "weather": "12h",
        "news": "24h",
        "competitor": "24h",
        "trend_signal": "7d",
        "policy": "21d",
        "platform_rule": "14d",
    }
    FRESHNESS_RULES = {
        "weather": "intraday",
        "news": "intraday",
        "competitor": "intraday",
        "trend_signal": "trend",
        "policy": "slow_burn",
        "platform_rule": "slow_burn",
    }

    def __init__(self, dataset: dict[str, list[dict[str, object]]], now: datetime | None = None, fetch_status: dict[str, object] | None = None):
        self.dataset = dataset
        self.products = {item["product_id"]: item for item in dataset["products"]}
        self.now = now.astimezone() if now else datetime.now().astimezone()
        self.fetch_status = fetch_status or {}

    def run(self) -> RunResult:
        signals = self._build_signals()
        fresh_active_signals, stale_background = self._partition_by_freshness(signals)
        tasks = self._build_tasks(signals)
        actions = self._build_actions(signals)
        auto_actions = [a for a in actions if a.risk_level == "low"]
        approval_queue = [a for a in actions if a.risk_level in {"medium", "high"}]
        formal_conclusions, hint_layer, observation_layer = self._partition_result_layers(signals)
        source_summary = self._build_source_summary(signals)
        layer_summary = self._build_layer_summary(
            formal_conclusions,
            hint_layer,
            observation_layer,
            stale_background,
            source_summary,
        )
        freshness_summary = self._build_freshness_summary(signals, fresh_active_signals, stale_background, source_summary)
        scan_agents = self._build_scan_agents(signals, tasks, actions)
        scan_agent_summary = self._build_scan_agent_summary(scan_agents)
        summary = self._build_summary(
            signals=signals,
            auto_actions=auto_actions,
            approval_queue=approval_queue,
            layer_summary=layer_summary,
            freshness_summary=freshness_summary,
            source_summary=source_summary,
        )
        return RunResult(
            summary=summary,
            signals=signals,
            tasks=tasks,
            action_proposals=actions,
            auto_actions=auto_actions,
            approval_queue=approval_queue,
            formal_conclusions=formal_conclusions,
            hint_layer=hint_layer,
            observation_layer=observation_layer,
            stale_background=stale_background,
            fresh_active_signals=fresh_active_signals,
            layer_summary=layer_summary,
            freshness_summary=freshness_summary,
            source_summary=source_summary,
            scan_agents=scan_agents,
            scan_agent_summary=scan_agent_summary,
            fetch_status=self.fetch_status,
            generated_at=self.now.isoformat(timespec="seconds"),
        )

    def _build_signals(self) -> list[DecisionRecord]:
        signals: list[DecisionRecord] = []
        for product in self.dataset["products"]:
            if product["inventory"] == 0 and product["metrics"]["orders_7d"] > 100:
                signals.append(DecisionRecord(
                    decision_id=f"SIG-{product['product_id']}",
                    decision_type="signal",
                    title=f"{product['name']} 缺货但仍有 TikTok PH 需求",
                    priority="P0",
                    risk_level="low",
                    status="new",
                    reason="Philippines TikTok Shop 的 printed tee SKU 近7天仍有稳定订单，但库存为 0，说明断货正在损失销量。",
                    evidence=self._with_scan_agent_evidence(
                        {
                            "product_id": product["product_id"],
                            "orders_7d": product["metrics"]["orders_7d"],
                            "inventory": product["inventory"],
                            "market_context": product["market_context"],
                            "style_keywords": product["style_keywords"],
                            "semantic_hint": "Philippines TikTok printed tee",
                            "update_cadence": "intraday",
                        },
                        agent_key="market_radar",
                    ),
                    **self._source_metadata(source_type="trend_signal", source_tier="medium", freshness_window="72h"),
                ))
            if product["metrics"]["conversion_rate"] < 0.01 and product["status"] == "active":
                signals.append(DecisionRecord(
                    decision_id=f"SIG-CVR-{product['product_id']}",
                    decision_type="signal",
                    title=f"{product['name']} 在 TikTok PH 转化偏低",
                    priority="P1",
                    risk_level="low",
                    status="new",
                    reason="在售 printed tee 转化率低于 1%，说明商品页、尺码信息或印花表达和菲律宾流量不够匹配。",
                    evidence=self._with_scan_agent_evidence(
                        {
                            "product_id": product["product_id"],
                            "conversion_rate": product["metrics"]["conversion_rate"],
                            "market_context": product["market_context"],
                            "semantic_hint": "Philippines TikTok printed tee",
                            "update_cadence": "periodic",
                        },
                        agent_key="market_radar",
                    ),
                    **self._source_metadata(source_type="trend_signal", source_tier="medium", freshness_window="72h"),
                ))
        for ad in self.dataset["ads"]:
            if ad["roas"] < 1.2:
                signals.append(DecisionRecord(
                    decision_id=f"SIG-{ad['campaign_id']}",
                    decision_type="signal",
                    title=f"{ad['name']} ROAS 偏低",
                    priority="P0",
                    risk_level="low",
                    status="new",
                    reason="TikTok Shop PH 的 printed tee 广告 ROAS 低于 1.2，已接近亏损，需要马上止损复核。",
                    evidence=self._with_scan_agent_evidence(
                        {
                            "campaign_id": ad["campaign_id"],
                            "roas": ad["roas"],
                            "spend_24h": ad["spend_24h"],
                            "market": ad["market"],
                            "category": ad["category"],
                            "semantic_hint": "Philippines TikTok printed tee",
                            "update_cadence": "realtime",
                        },
                        agent_key="market_radar",
                    ),
                    **self._source_metadata(source_type="trend_signal", source_tier="medium", freshness_window="24h"),
                ))
        for event in self.dataset["market_events"]:
            signals.append(self._build_event_signal(event))
        for creator in self.dataset["creators"]:
            if creator["status"] == "overdue":
                signals.append(DecisionRecord(
                    decision_id=f"SIG-{creator['creator_id']}",
                    decision_type="signal",
                    title=f"达人 {creator['name']} 已逾期未交付",
                    priority="P1",
                    risk_level="low",
                    status="new",
                    reason="菲律宾 TikTok 印花短袖合作达人逾期，可能错过校园季和天气窗口。",
                    evidence=self._with_scan_agent_evidence(
                        {
                            "creator_id": creator["creator_id"],
                            "risk_flags": creator["risk_flags"],
                            "content_focus": creator["content_focus"],
                            "semantic_hint": "Philippines TikTok printed tee",
                            "update_cadence": "intraday",
                        },
                        agent_key="competitor_watch",
                    ),
                    **self._source_metadata(source_type="competitor", source_tier="low", freshness_window="72h"),
                ))
        return signals

    def _build_event_signal(self, event: dict[str, object]) -> DecisionRecord:
        impact_direction = str(event["impact_direction"])
        source_type = str(event["source_type"])
        severity = str(event["severity"])
        source_tier = str(event["source_tier"])
        posture = self._event_signal_posture(
            source_type=source_type,
            source_tier=source_tier,
            severity=severity,
            impact_direction=impact_direction,
        )
        if source_type == "weather":
            reason = "菲律宾天气样本显示高温体感正在抬升 breathable printed tee 的即时需求，应加强轻薄面料与透气卖点。"
        elif source_type == "news":
            reason = "菲律宾本地新闻/商业动态对校园与线下活动热度形成支撑，利好平价 graphic tee 内容切角。"
        elif source_type == "policy":
            reason = "政策样本来自高可信外部政策观察，若偏负向会直接影响补货时效与申报风险，需要提前收口。"
        elif source_type == "platform_rule":
            reason = "平台规则样本来自高可信官方口径，适合直接进入 listing 合规检查和主结论。"
        elif source_type == "competitor":
            reason = "竞品样本显示 Manila 卖家在打 bundle 价格战，需要复核我们 printed tee 套装与券策略。"
        else:
            reason = "爆款线索样本仍属观察层，但已经指向菲律宾本地化印花元素可能成为下一波题材。"

        metadata = self._source_metadata(
            source_type=source_type,
            source_tier=source_tier,
            source_mode=str(event.get("source_mode") or "mock"),
            freshness_window=str(event.get("freshness_window", self.DEFAULT_FRESHNESS_WINDOWS.get(source_type, "72h"))),
            published_at=event.get("published_at"),
            captured_at=event.get("captured_at"),
        )
        agent_key = str(event.get("evidence", {}).get("scan_agent") or self._agent_for_source_type(source_type))
        if agent_key not in self.SCAN_AGENT_SPECS:
            agent_key = self._agent_for_source_type(source_type)
        return DecisionRecord(
            decision_id=f"SIG-{event['event_id']}",
            decision_type="signal",
            title=str(event["title"]),
            priority=posture["priority"],
            risk_level=posture["risk_level"],
            status=posture["status"],
            reason=reason,
            evidence=self._with_scan_agent_evidence(
                {
                    "event_id": event["event_id"],
                    "severity": severity,
                    "impact_direction": impact_direction,
                    "summary": event["summary"],
                    "market": event["market"],
                    "category": event["category"],
                    "update_cadence": event["update_cadence"],
                    "semantic_hint": "Philippines TikTok printed tee",
                    **dict(event.get("evidence", {})),
                },
                agent_key=agent_key,
            ),
            **metadata,
        )

    def _build_tasks(self, signals: list[DecisionRecord]) -> list[DecisionRecord]:
        tasks: list[DecisionRecord] = []
        for signal in signals:
            mapping = {
                "缺货": ("补货核查", "P0"),
                "ROAS": ("广告止损复核", "P0"),
                "转化偏低": ("商品页诊断", "P1"),
                "heat index": ("天气窗口备货复核", "P1"),
                "campus fair": ("校园季内容排期", "P1"),
                "label print placement": ("规则合规检查", "P0"),
                "bundle price": ("竞品应对复盘", "P1"),
                "breakout clue": ("本地化印花观察", "P1"),
                "逾期": ("达人履约跟进", "P1"),
            }
            task_title = "常规分析"
            priority = signal.priority
            signal_title = signal.title.lower()
            for key, value in mapping.items():
                if key.lower() in signal_title:
                    task_title, priority = value
                    break
            tasks.append(DecisionRecord(
                decision_id=signal.decision_id.replace("SIG", "TASK", 1),
                decision_type="task",
                title=task_title,
                priority=priority,
                risk_level="low",
                status="queued",
                reason=f"由信号『{signal.title}』触发，需要形成可追踪任务闭环。",
                source_type=signal.source_type,
                source_tier=signal.source_tier,
                result_layer=signal.result_layer,
                source_mode=signal.source_mode,
                captured_at=signal.captured_at,
                published_at=signal.published_at,
                freshness_window=signal.freshness_window,
                stale_after=signal.stale_after,
                freshness_rule=signal.freshness_rule,
                freshness_score=signal.freshness_score,
                is_stale=signal.is_stale,
                market=signal.market,
                category=signal.category,
                evidence=self._with_scan_agent_evidence(
                    {
                        "source_signal": signal.decision_id,
                        "update_cadence": signal.evidence.get("update_cadence"),
                    },
                    agent_key=self._agent_from_record(signal),
                ),
            ))
        return tasks

    def _build_actions(self, signals: list[DecisionRecord]) -> list[DecisionRecord]:
        actions: list[DecisionRecord] = []
        for signal in signals:
            title = signal.title
            if "ROAS 偏低" in title:
                actions.append(self._build_action_from_signal(
                    signal,
                    title="将低 ROAS 广告加入人工止损审批",
                    priority="P0",
                    risk_level="high",
                    status="pending_approval",
                    reason="暂停或降预算会直接影响投放规模，属于高风险资金动作，必须人工确认。",
                ))
            elif "缺货" in title:
                actions.append(self._build_action_from_signal(
                    signal,
                    title="自动创建缺货预警并冻结关联补量建议",
                    priority="P0",
                    risk_level="low",
                    status="auto_executed",
                    reason="该动作只写本地预警与建议冻结，不触达真实供应链，风险低。",
                ))
            elif "heat index" in title.lower() or "campus fair" in title.lower() or "breakout clue" in title.lower():
                action_posture = self._event_action_posture(signal)
                action_title = "提交高潜 printed tee 选款建议"
                action_reason = "会影响预算和备货节奏，属于中风险，需要人工确认。"
                if action_posture["status"] == "draft":
                    action_title = "保留高潜 printed tee 观察建议"
                    action_reason = "当前更适合作为观察输入，不直接推动预算或备货动作。"
                actions.append(self._build_action_from_signal(
                    signal,
                    title=action_title,
                    priority=action_posture["priority"],
                    risk_level=action_posture["risk_level"],
                    status=action_posture["status"],
                    reason=action_reason,
                ))
            elif "转化偏低" in title or "label" in title.lower():
                actions.append(self._build_action_from_signal(
                    signal,
                    title="自动创建商品页诊断任务",
                    priority="P1",
                    risk_level="low",
                    status="auto_executed",
                    reason="只新增内部诊断任务，不修改商品或广告状态，风险低。",
                ))
            elif "policy" in title.lower() or signal.source_type == "policy":
                action_posture = self._event_action_posture(signal)
                actions.append(self._build_action_from_signal(
                    signal,
                    title="提交政策风险人工复核",
                    priority=action_posture["priority"],
                    risk_level=action_posture["risk_level"],
                    status=action_posture["status"],
                    reason="政策变化会影响补货与申报节奏，进入人工确认更稳。",
                ))
            elif "bundle price" in title.lower():
                action_posture = self._event_action_posture(signal)
                actions.append(self._build_action_from_signal(
                    signal,
                    title="提交价格策略人工复核",
                    priority=action_posture["priority"],
                    risk_level=action_posture["risk_level"],
                    status=action_posture["status"],
                    reason="价格调整会影响利润与定位，必须先进入人工确认。",
                ))
            elif "逾期" in title:
                actions.append(self._build_action_from_signal(
                    signal,
                    title="提交达人履约人工介入",
                    priority="P1",
                    risk_level="medium",
                    status="pending_approval",
                    reason="达人协作涉及外部沟通，不能自动触发真实消息，需人工确认。",
                ))
        return [self._apply_stale_action_guard(action) for action in actions]

    def _build_action_from_signal(
        self,
        signal: DecisionRecord,
        *,
        title: str,
        priority: str,
        risk_level: str,
        status: str,
        reason: str,
    ) -> DecisionRecord:
        return DecisionRecord(
            decision_id=signal.decision_id.replace("SIG", "ACT", 1),
            decision_type="action",
            title=title,
            priority=priority,
            risk_level=risk_level,
            status=status,
            reason=reason,
            source_type=signal.source_type,
            source_tier=signal.source_tier,
            result_layer=signal.result_layer,
            source_mode=signal.source_mode,
            captured_at=signal.captured_at,
            published_at=signal.published_at,
            freshness_window=signal.freshness_window,
            stale_after=signal.stale_after,
            freshness_rule=signal.freshness_rule,
            freshness_score=signal.freshness_score,
            is_stale=signal.is_stale,
            market=signal.market,
            category=signal.category,
            evidence=self._with_scan_agent_evidence(dict(signal.evidence), agent_key=self._agent_from_record(signal)),
        )

    def _apply_stale_action_guard(self, action: DecisionRecord) -> DecisionRecord:
        if not action.is_stale:
            return action
        action.result_layer = "background"
        action.priority = "P2"
        action.risk_level = "low"
        action.status = "draft"
        action.reason = f"原始信号已过 freshness window，当前仅保留为背景参考。{action.reason}"
        return action

    def _event_signal_posture(
        self,
        *,
        source_type: str,
        source_tier: str,
        severity: str,
        impact_direction: str,
    ) -> dict[str, str]:
        score = 0
        score += {"high": 3, "medium": 2, "low": 1}.get(severity, 1)
        score += {"high": 2, "medium": 1, "low": 0}.get(source_tier, 0)
        if impact_direction == "negative":
            score += 2
        elif impact_direction == "positive":
            score -= 1

        if source_type in {"policy", "platform_rule"}:
            score += 1
        elif source_type == "trend_signal":
            score -= 1

        if score >= 6:
            return {"priority": "P0", "risk_level": "high", "status": "escalated"}
        if score >= 4:
            return {"priority": "P1", "risk_level": "medium", "status": "new"}
        return {"priority": "P2", "risk_level": "low", "status": "observing"}

    def _event_action_posture(self, signal: DecisionRecord) -> dict[str, str]:
        if signal.is_stale:
            return {"priority": "P2", "risk_level": "low", "status": "draft"}
        if signal.risk_level == "high":
            return {"priority": signal.priority, "risk_level": "high", "status": "pending_approval"}
        if signal.risk_level == "medium":
            return {"priority": signal.priority, "risk_level": "medium", "status": "pending_approval"}
        if signal.status == "observing" or signal.source_tier == "low":
            return {"priority": signal.priority, "risk_level": "low", "status": "draft"}
        return {"priority": signal.priority, "risk_level": "low", "status": "auto_executed"}

    def _source_metadata(
        self,
        *,
        source_type: str,
        source_tier: str,
        freshness_window: str | None = None,
        published_at: object | None = None,
        captured_at: object | None = None,
        source_mode: str = "mock",
    ) -> dict[str, object]:
        freshness_rule = self.FRESHNESS_RULES.get(source_type, "trend")
        resolved_window = freshness_window or self.DEFAULT_FRESHNESS_WINDOWS.get(source_type, "72h")
        window_delta = self._parse_freshness_window(resolved_window)
        captured_dt = self._coerce_datetime(captured_at) or self.now
        published_dt = self._coerce_datetime(published_at) or (captured_dt - min(window_delta, timedelta(hours=6)))
        stale_after = published_dt + window_delta
        freshness_score = self._freshness_score(captured_dt, published_dt, stale_after)
        is_stale = captured_dt > stale_after
        result_layer = self._result_layer_for_source_tier(source_tier)
        if is_stale:
            result_layer = "background"
        return {
            "source_type": source_type,
            "source_tier": source_tier,
            "result_layer": result_layer,
            "source_mode": source_mode,
            "captured_at": captured_dt.isoformat(timespec="seconds"),
            "published_at": published_dt.isoformat(timespec="seconds"),
            "freshness_window": resolved_window,
            "stale_after": stale_after.isoformat(timespec="seconds"),
            "freshness_rule": freshness_rule,
            "freshness_score": freshness_score,
            "is_stale": is_stale,
            "market": self.MARKET,
            "category": self.CATEGORY,
        }

    def _coerce_datetime(self, value: object | None) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value.astimezone()
        return datetime.fromisoformat(str(value)).astimezone()

    def _freshness_score(self, captured_at: datetime, published_at: datetime, stale_after: datetime) -> float:
        total_window = max((stale_after - published_at).total_seconds(), 1.0)
        remaining = (stale_after - captured_at).total_seconds()
        return round(max(0.0, min(1.0, remaining / total_window)), 3)

    def _parse_freshness_window(self, value: str) -> timedelta:
        if value.endswith("h"):
            return timedelta(hours=int(value[:-1]))
        if value.endswith("d"):
            return timedelta(days=int(value[:-1]))
        return timedelta(hours=72)

    def _result_layer_for_source_tier(self, source_tier: str) -> str:
        return {
            "high": "formal_conclusion",
            "medium": "hint",
            "low": "observation",
        }.get(source_tier, "hint")

    def _partition_by_freshness(self, signals: list[DecisionRecord]) -> tuple[list[DecisionRecord], list[DecisionRecord]]:
        fresh_active = [item for item in signals if not item.is_stale]
        stale_background = [item for item in signals if item.is_stale]
        return fresh_active, stale_background

    def _partition_result_layers(
        self,
        signals: list[DecisionRecord],
    ) -> tuple[list[DecisionRecord], list[DecisionRecord], list[DecisionRecord]]:
        formal_conclusions = [item for item in signals if item.result_layer == "formal_conclusion"]
        hint_layer = [item for item in signals if item.result_layer == "hint"]
        observation_layer = [item for item in signals if item.result_layer == "observation"]
        return formal_conclusions, hint_layer, observation_layer

    def _build_layer_summary(
        self,
        formal_conclusions: list[DecisionRecord],
        hint_layer: list[DecisionRecord],
        observation_layer: list[DecisionRecord],
        stale_background: list[DecisionRecord],
        source_summary: dict[str, object],
    ) -> dict[str, dict[str, object]]:
        return {
            "formal_conclusion": self._summarize_layer(formal_conclusions),
            "hint": self._summarize_layer(hint_layer),
            "observation": self._summarize_layer(observation_layer),
            "background": self._summarize_layer(stale_background),
            "source_status": {
                "live": source_summary["totals"]["live"],
                "mock": source_summary["totals"]["mock"],
                "fallback": source_summary["totals"]["fallback"],
                "degraded": source_summary["totals"]["degraded"],
                "highlights": list(source_summary["highlights"]),
            },
        }

    def _summarize_layer(self, items: list[DecisionRecord]) -> dict[str, object]:
        return {
            "count": len(items),
            "source_tiers": sorted({item.source_tier for item in items}),
            "signal_ids": [item.decision_id for item in items],
        }

    def _build_freshness_summary(
        self,
        signals: list[DecisionRecord],
        fresh_active_signals: list[DecisionRecord],
        stale_background: list[DecisionRecord],
        source_summary: dict[str, object],
    ) -> dict[str, object]:
        return {
            "fresh_count": len(fresh_active_signals),
            "stale_count": len(stale_background),
            "fresh_signal_ids": [item.decision_id for item in fresh_active_signals],
            "background_signal_ids": [item.decision_id for item in stale_background],
            "freshness_rules": {item.decision_id: item.freshness_rule for item in signals},
            "source_status": {
                "live": source_summary["totals"]["live"],
                "mock": source_summary["totals"]["mock"],
                "fallback": source_summary["totals"]["fallback"],
                "degraded": source_summary["totals"]["degraded"],
            },
        }

    def _build_source_summary(self, signals: list[DecisionRecord]) -> dict[str, object]:
        totals = {"live": 0, "mock": 0, "fallback": 0, "degraded": 0}
        by_source: dict[str, dict[str, object]] = {}
        source_aliases = {
            "weather": "weather",
            "news": "news",
            "platform_rule": "rule",
            "policy": "policy",
            "competitor": "competitor_search",
            "trend_signal": "trend_signal",
        }

        for source_key, raw_status in self.fetch_status.items():
            status = raw_status if isinstance(raw_status, dict) else {}
            signal_key = str(source_key)
            source_mode = str(status.get("source_mode") or "mock")
            fallback_used = bool(status.get("fallback_used"))
            status_text = str(status.get("status") or "unknown")
            live_hits = sum(1 for item in signals if source_aliases.get(item.source_type, item.source_type) == signal_key and item.source_mode == "live")
            mock_hits = sum(1 for item in signals if source_aliases.get(item.source_type, item.source_type) == signal_key and item.source_mode == "mock")
            signal_status = "degraded" if "degraded" in status_text or status_text == "fetch_failed" or source_mode == "degraded" else ("live" if source_mode == "live" else "mock")
            by_source[signal_key] = {
                "source_mode": source_mode,
                "status": status_text,
                "signal_status": signal_status,
                "live_hits": live_hits,
                "mock_hits": mock_hits,
                "fallback_used": fallback_used,
                "error": status.get("error"),
            }
            if source_mode == "live":
                totals["live"] += 1
            else:
                totals["mock"] += 1
            if fallback_used:
                totals["fallback"] += 1
            if signal_status == "degraded":
                totals["degraded"] += 1

        highlights = [
            f"{source_key} {details['signal_status']}"
            for source_key, details in by_source.items()
            if source_key in {"weather", "news", "rule", "competitor_search"}
        ]
        return {"totals": totals, "by_source": by_source, "highlights": highlights}

    def _with_scan_agent_evidence(self, evidence: dict[str, object], *, agent_key: str) -> dict[str, object]:
        agent = self.SCAN_AGENT_SPECS[agent_key]
        payload = dict(evidence)
        payload["scan_agent"] = agent_key
        payload["scan_agent_label"] = agent["label"]
        return payload

    def _agent_for_source_type(self, source_type: str) -> str:
        if source_type in {"weather", "news"}:
            return "market_radar"
        if source_type == "competitor":
            return "competitor_watch"
        if source_type == "trend_signal":
            return "trend_scout"
        if source_type in {"policy", "platform_rule"}:
            return "external_environment"
        return "daily_brief_synth"

    def _agent_from_record(self, record: DecisionRecord) -> str:
        agent_key = str(record.evidence.get("scan_agent") or "").strip()
        if agent_key in self.SCAN_AGENT_SPECS:
            return agent_key
        return self._agent_for_source_type(record.source_type)

    def _build_scan_agents(
        self,
        signals: list[DecisionRecord],
        tasks: list[DecisionRecord],
        actions: list[DecisionRecord],
    ) -> dict[str, dict[str, object]]:
        agent_payload: dict[str, dict[str, object]] = {}
        for agent_key, spec in self.SCAN_AGENT_SPECS.items():
            agent_signals = [item.to_dict() for item in signals if self._agent_from_record(item) == agent_key]
            agent_tasks = [item.to_dict() for item in tasks if self._agent_from_record(item) == agent_key]
            agent_actions = [item.to_dict() for item in actions if self._agent_from_record(item) == agent_key]
            if agent_key == "daily_brief_synth":
                agent_signals = [item.to_dict() for item in signals]
                agent_tasks = [item.to_dict() for item in tasks]
                agent_actions = [item.to_dict() for item in actions]
            agent_payload[agent_key] = {
                "agent_key": agent_key,
                "label": spec["label"],
                "responsibilities": list(spec["responsibilities"]),
                "signals": agent_signals,
                "tasks": agent_tasks,
                "action_proposals": agent_actions,
            }
        return agent_payload

    def _build_scan_agent_summary(self, scan_agents: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
        summary: dict[str, dict[str, object]] = {}
        for agent_key, payload in scan_agents.items():
            summary[agent_key] = {
                "label": payload["label"],
                "signal_count": len(payload["signals"]),
                "task_count": len(payload["tasks"]),
                "action_count": len(payload["action_proposals"]),
                "responsibilities": payload["responsibilities"],
            }
        return summary

    def _build_summary(
        self,
        *,
        signals: list[DecisionRecord],
        auto_actions: list[DecisionRecord],
        approval_queue: list[DecisionRecord],
        layer_summary: dict[str, dict[str, object]],
        freshness_summary: dict[str, object],
        source_summary: dict[str, object],
    ) -> str:
        source_highlights = "，".join(source_summary["highlights"]) or "no tracked sources"
        return (
            f"菲律宾 TikTok 印花短袖场景共识别 {len(signals)} 个 signals；"
            f"正式结论 {layer_summary['formal_conclusion']['count']} 个（{','.join(layer_summary['formal_conclusion']['source_tiers']) or 'none'}）；"
            f"提示层 {layer_summary['hint']['count']} 个（{','.join(layer_summary['hint']['source_tiers']) or 'none'}）；"
            f"观察层 {layer_summary['observation']['count']} 个（{','.join(layer_summary['observation']['source_tiers']) or 'none'}）；"
            f"过时背景 {layer_summary['background']['count']} 个；"
            f"新鲜可用 {freshness_summary['fresh_count']} 个；"
            f"真实接入概况：{source_highlights}；"
            f"live {source_summary['totals']['live']} / mock {source_summary['totals']['mock']} / fallback {source_summary['totals']['fallback']}；"
            f"自动执行 {len(auto_actions)} 个低风险动作，{len(approval_queue)} 个中高风险动作进入人工确认队列。"
        )
