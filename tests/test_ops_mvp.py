import copy
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from tools.ops_mvp.adapters import DataRegistry
from tools.ops_mvp.engine import DecisionEngine
from tools.ops_mvp.models import DecisionRecord
from tools.ops_mvp.fetchers import ExternalSignalFetcher
from tools.ops_mvp.service import OpsMVPService


WORKSPACE = Path(__file__).resolve().parents[1]


class OpsMVPTests(unittest.TestCase):
    def test_decision_record_supports_domain_source_metadata(self):
        record = DecisionRecord(
            decision_id="SIG-PH-001",
            decision_type="signal",
            title="PH weather watch",
            priority="P1",
            risk_level="low",
            status="new",
            reason="Need weather-aware inventory planning.",
            source_type="weather",
            source_tier="high",
            captured_at="2026-03-17T08:30:00+08:00",
            published_at="2026-03-17T06:00:00+08:00",
            freshness_window="24h",
            stale_after="2026-03-18T06:00:00+08:00",
            market="PH",
            category="printed_tshirt",
            evidence={"region": "Metro Manila"},
        )

        payload = record.to_dict()

        self.assertEqual(payload["source_type"], "weather")
        self.assertEqual(payload["source_tier"], "high")
        self.assertEqual(payload["captured_at"], "2026-03-17T08:30:00+08:00")
        self.assertEqual(payload["published_at"], "2026-03-17T06:00:00+08:00")
        self.assertEqual(payload["freshness_window"], "24h")
        self.assertEqual(payload["stale_after"], "2026-03-18T06:00:00+08:00")
        self.assertEqual(payload["market"], "PH")
        self.assertEqual(payload["category"], "printed_tshirt")

    def test_ops_mvp_run_cycle_generates_outputs(self):
        service = OpsMVPService(WORKSPACE)
        payload = service.run_cycle()

        self.assertTrue(payload["dataset"]["products"])
        self.assertTrue(payload["result"]["signals"])
        self.assertTrue(payload["result"]["tasks"])
        self.assertTrue(payload["result"]["action_proposals"])
        self.assertTrue(payload["result"]["approval_queue"])
        self.assertTrue(payload["result"]["auto_actions"])

        latest_run = WORKSPACE / "runtime" / "ops_mvp" / "latest_run.json"
        self.assertTrue(latest_run.exists())

    def test_high_risk_actions_go_to_approval_queue(self):
        service = OpsMVPService(WORKSPACE)
        payload = service.run_cycle()

        pending = payload["result"]["approval_queue"]
        self.assertTrue(pending)
        self.assertTrue(all(item["risk_level"] in {"medium", "high"} for item in pending))

    def test_low_risk_actions_can_auto_execute(self):
        service = OpsMVPService(WORKSPACE)
        payload = service.run_cycle()

        auto_actions = payload["result"]["auto_actions"]
        self.assertTrue(auto_actions)
        self.assertTrue(all(item["risk_level"] == "low" for item in auto_actions))

    def test_mock_dataset_covers_ph_printed_tshirt_scan_sources(self):
        dataset = DataRegistry(WORKSPACE / "mock").load_all()

        for product in dataset["products"]:
            self.assertEqual(product["market"], "PH")
            self.assertEqual(product["category"], "printed_tshirt")
            self.assertIn("Philippines", product["market_context"])
            self.assertIn("print", product["style_keywords"])

        event_types = {item["source_type"] for item in dataset["market_events"]}
        self.assertEqual(
            event_types,
            {"weather", "news", "policy", "platform_rule", "competitor", "trend_signal"},
        )

        source_tiers = {item["source_tier"] for item in dataset["market_events"]}
        self.assertEqual(source_tiers, {"high", "medium", "low"})

        cadences = {item["update_cadence"] for item in dataset["market_events"]}
        self.assertEqual(cadences, {"realtime", "intraday", "periodic", "slow_variable"})

    def test_run_cycle_signals_include_ph_printed_tshirt_source_metadata(self):
        service = OpsMVPService(WORKSPACE)
        payload = service.run_cycle()

        signals = payload["result"]["signals"]
        self.assertTrue(signals)

        allowed_source_types = {"weather", "news", "policy", "platform_rule", "competitor", "trend_signal"}
        allowed_source_tiers = {"high", "medium", "low"}
        observed_types = {signal["source_type"] for signal in signals}
        observed_tiers = {signal["source_tier"] for signal in signals}
        cadence_counter = Counter(signal["evidence"].get("update_cadence") for signal in signals)

        self.assertTrue({"weather", "news", "policy", "platform_rule", "competitor", "trend_signal"}.issubset(observed_types))
        self.assertTrue({"high", "medium", "low"}.issubset(observed_tiers))
        self.assertGreaterEqual(cadence_counter["realtime"], 1)
        self.assertGreaterEqual(cadence_counter["intraday"], 1)
        self.assertGreaterEqual(cadence_counter["periodic"], 1)
        self.assertGreaterEqual(cadence_counter["slow_variable"], 1)

        semantic_blob = " ".join(
            [signal["title"] + " " + signal["reason"] + " " + str(signal["evidence"]) for signal in signals]
        ).lower()
        self.assertIn("philippines", semantic_blob)
        self.assertIn("printed tee", semantic_blob)
        self.assertIn("tiktok", semantic_blob)

        for signal in signals:
            self.assertIn(signal["source_type"], allowed_source_types)
            self.assertIn(signal["source_tier"], allowed_source_tiers)
            self.assertTrue(signal["captured_at"])
            self.assertTrue(signal["published_at"])
            self.assertTrue(signal["freshness_window"])
            self.assertTrue(signal["stale_after"])
            self.assertEqual(signal["market"], "PH")
            self.assertEqual(signal["category"], "printed_tshirt")

    def test_source_metadata_changes_decision_layer_not_just_evidence(self):
        dataset = copy.deepcopy(DataRegistry(WORKSPACE / "mock").load_all())
        for event in dataset["market_events"]:
            if event["event_id"] == "EV-PH-WEATHER-001":
                event["published_at"] = "2026-03-17T08:00:00+08:00"
            if event["event_id"] == "EV-PH-COMP-001":
                event["published_at"] = "2026-03-17T01:00:00+08:00"
        payload = DecisionEngine(dataset, now=datetime.fromisoformat("2026-03-17T09:00:00+08:00")).run().to_dict()

        signals = {item["decision_id"]: item for item in payload["signals"]}
        actions = {item["decision_id"]: item for item in payload["action_proposals"]}
        approval_ids = {item["decision_id"] for item in payload["approval_queue"]}
        auto_ids = {item["decision_id"] for item in payload["auto_actions"]}

        weather_signal = signals["SIG-EV-PH-WEATHER-001"]
        self.assertEqual(weather_signal["priority"], "P2")
        self.assertEqual(weather_signal["risk_level"], "low")
        self.assertEqual(weather_signal["status"], "observing")
        self.assertEqual(weather_signal["evidence"]["severity"], "medium")

        weather_action = actions["ACT-EV-PH-WEATHER-001"]
        self.assertEqual(weather_action["priority"], "P2")
        self.assertEqual(weather_action["risk_level"], "low")
        self.assertEqual(weather_action["status"], "draft")
        self.assertNotIn(weather_action["decision_id"], approval_ids)
        self.assertIn(weather_action["decision_id"], auto_ids)

        competitor_signal = signals["SIG-EV-PH-COMP-001"]
        self.assertEqual(competitor_signal["priority"], "P0")
        self.assertEqual(competitor_signal["risk_level"], "high")
        self.assertEqual(competitor_signal["status"], "escalated")

        competitor_action = actions["ACT-EV-PH-COMP-001"]
        self.assertEqual(competitor_action["priority"], "P0")
        self.assertEqual(competitor_action["risk_level"], "high")
        self.assertEqual(competitor_action["status"], "pending_approval")
        self.assertIn(competitor_action["decision_id"], approval_ids)

        policy_signal = signals["SIG-EV-PH-POLICY-001"]
        self.assertEqual(policy_signal["priority"], "P0")
        self.assertEqual(policy_signal["risk_level"], "high")
        self.assertEqual(policy_signal["status"], "escalated")

        policy_action = actions["ACT-EV-PH-POLICY-001"]
        self.assertEqual(policy_action["priority"], "P0")
        self.assertEqual(policy_action["risk_level"], "high")
        self.assertEqual(policy_action["status"], "pending_approval")
        self.assertIn(policy_action["decision_id"], approval_ids)

    def test_source_tier_drives_result_layers_and_api_payload(self):
        dataset = copy.deepcopy(DataRegistry(WORKSPACE / "mock").load_all())
        for event in dataset["market_events"]:
            if event["event_id"] == "EV-PH-WEATHER-001":
                event["published_at"] = "2026-03-17T08:00:00+08:00"
            if event["event_id"] == "EV-PH-NEWS-001":
                event["published_at"] = "2026-03-17T06:00:00+08:00"
        result = DecisionEngine(dataset, now=datetime.fromisoformat("2026-03-17T09:00:00+08:00")).run().to_dict()
        self.assertIn("formal_conclusions", result)
        self.assertIn("hint_layer", result)
        self.assertIn("observation_layer", result)
        self.assertIn("layer_summary", result)

        formal_ids = {item["decision_id"] for item in result["formal_conclusions"]}
        hint_ids = {item["decision_id"] for item in result["hint_layer"]}
        observation_ids = {item["decision_id"] for item in result["observation_layer"]}

        self.assertIn("SIG-EV-PH-WEATHER-001", formal_ids)
        self.assertIn("SIG-EV-PH-NEWS-001", hint_ids)
        self.assertIn("SIG-EV-PH-TREND-001", observation_ids)

        self.assertNotIn("SIG-EV-PH-TREND-001", formal_ids)
        self.assertNotIn("SIG-EV-PH-TREND-001", hint_ids)

        weather_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-EV-PH-WEATHER-001")
        news_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-EV-PH-NEWS-001")
        trend_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-EV-PH-TREND-001")

        self.assertEqual(weather_signal["result_layer"], "formal_conclusion")
        self.assertEqual(news_signal["result_layer"], "hint")
        self.assertEqual(trend_signal["result_layer"], "observation")

        self.assertEqual(result["layer_summary"]["formal_conclusion"]["source_tiers"], ["high"])
        self.assertEqual(result["layer_summary"]["hint"]["source_tiers"], ["medium"])
        self.assertEqual(result["layer_summary"]["observation"]["source_tiers"], ["low"])

        self.assertIn("正式结论", result["summary"])
        self.assertIn("提示层", result["summary"])
        self.assertIn("观察层", result["summary"])

    def test_low_trust_signal_cannot_directly_drive_high_risk_action(self):
        service = OpsMVPService(WORKSPACE)
        payload = service.run_cycle()

        trend_signal = next(item for item in payload["result"]["signals"] if item["decision_id"] == "SIG-EV-PH-TREND-001")
        trend_action = next(item for item in payload["result"]["action_proposals"] if item["decision_id"] == "ACT-EV-PH-TREND-001")

        self.assertEqual(trend_signal["source_tier"], "low")
        self.assertEqual(trend_signal["result_layer"], "observation")
        self.assertEqual(trend_action["source_tier"], "low")
        self.assertEqual(trend_action["risk_level"], "low")
        self.assertEqual(trend_action["status"], "draft")
        self.assertNotIn(trend_action, payload["result"]["approval_queue"])

    def test_freshness_rules_mark_stale_items_and_preserve_background_only(self):
        dataset = DataRegistry(WORKSPACE / "mock").load_all()
        now = datetime.fromisoformat("2026-03-17T15:00:00+08:00")
        engine = DecisionEngine(dataset, now=now)
        result = engine.run().to_dict()

        signals = {item["decision_id"]: item for item in result["signals"]}
        actions = {item["decision_id"]: item for item in result["action_proposals"]}
        stale_background = {item["decision_id"] for item in result["stale_background"]}
        fresh_active = {item["decision_id"] for item in result["fresh_active_signals"]}
        freshness_summary = result["freshness_summary"]

        weather = signals["SIG-EV-PH-WEATHER-001"]
        competitor = signals["SIG-EV-PH-COMP-001"]
        news = signals["SIG-EV-PH-NEWS-001"]
        trend = signals["SIG-EV-PH-TREND-001"]
        policy = signals["SIG-EV-PH-POLICY-001"]
        rule = signals["SIG-EV-PH-RULE-001"]

        self.assertTrue(weather["is_stale"])
        self.assertTrue(competitor["is_stale"])
        self.assertTrue(news["is_stale"])
        self.assertTrue(weather["freshness_score"] < 0.2)
        self.assertTrue(competitor["freshness_score"] < 0.2)
        self.assertTrue(news["freshness_score"] < 0.2)

        self.assertFalse(trend["is_stale"])
        self.assertFalse(policy["is_stale"])
        self.assertFalse(rule["is_stale"])
        self.assertGreater(trend["freshness_score"], 0.2)
        self.assertGreater(policy["freshness_score"], 0.2)
        self.assertGreater(rule["freshness_score"], 0.2)

        self.assertIn("SIG-EV-PH-WEATHER-001", stale_background)
        self.assertIn("SIG-EV-PH-COMP-001", stale_background)
        self.assertIn("SIG-EV-PH-NEWS-001", stale_background)
        self.assertNotIn("SIG-EV-PH-WEATHER-001", fresh_active)
        self.assertIn("SIG-EV-PH-TREND-001", fresh_active)
        self.assertIn("SIG-EV-PH-POLICY-001", fresh_active)

        self.assertEqual(weather["result_layer"], "background")
        self.assertEqual(competitor["result_layer"], "background")
        self.assertEqual(news["result_layer"], "background")
        self.assertEqual(actions["ACT-EV-PH-WEATHER-001"]["status"], "draft")
        self.assertEqual(actions["ACT-EV-PH-COMP-001"]["status"], "draft")
        self.assertEqual(actions["ACT-EV-PH-COMP-001"]["risk_level"], "low")
        self.assertNotIn("ACT-EV-PH-COMP-001", {item["decision_id"] for item in result["approval_queue"]})

        self.assertEqual(freshness_summary["stale_count"], 3)
        self.assertEqual(freshness_summary["fresh_count"], len(result["signals"]) - 3)
        self.assertIn("SIG-EV-PH-WEATHER-001", freshness_summary["background_signal_ids"])

    def test_freshness_rule_windows_follow_source_type(self):
        dataset = DataRegistry(WORKSPACE / "mock").load_all()
        now = datetime.fromisoformat("2026-03-17T15:00:00+08:00")
        engine = DecisionEngine(dataset, now=now)
        result = engine.run().to_dict()
        signals = {item["decision_id"]: item for item in result["signals"]}

        self.assertEqual(signals["SIG-EV-PH-WEATHER-001"]["freshness_rule"], "intraday")
        self.assertEqual(signals["SIG-EV-PH-NEWS-001"]["freshness_rule"], "intraday")
        self.assertEqual(signals["SIG-EV-PH-COMP-001"]["freshness_rule"], "intraday")
        self.assertEqual(signals["SIG-EV-PH-TREND-001"]["freshness_rule"], "trend")
        self.assertEqual(signals["SIG-EV-PH-RULE-001"]["freshness_rule"], "slow_burn")
        self.assertEqual(signals["SIG-EV-PH-POLICY-001"]["freshness_rule"], "slow_burn")

        self.assertEqual(signals["SIG-EV-PH-WEATHER-001"]["freshness_window"], "12h")
        self.assertEqual(signals["SIG-EV-PH-NEWS-001"]["freshness_window"], "24h")
        self.assertEqual(signals["SIG-EV-PH-COMP-001"]["freshness_window"], "24h")
        self.assertEqual(signals["SIG-EV-PH-TREND-001"]["freshness_window"], "7d")
        self.assertEqual(signals["SIG-EV-PH-RULE-001"]["freshness_window"], "14d")
        self.assertEqual(signals["SIG-EV-PH-POLICY-001"]["freshness_window"], "21d")

    def test_stale_low_tier_signal_stays_background_without_promoting_actions(self):
        dataset = DataRegistry(WORKSPACE / "mock").load_all()
        stale_dataset = copy.deepcopy(dataset)
        for event in stale_dataset["market_events"]:
            if event["event_id"] == "EV-PH-TREND-001":
                event["published_at"] = "2026-03-08T08:00:00+08:00"
                event["captured_at"] = "2026-03-17T15:00:00+08:00"
                break

        result = DecisionEngine(stale_dataset, now=datetime.fromisoformat("2026-03-17T15:00:00+08:00")).run().to_dict()
        trend_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-EV-PH-TREND-001")
        trend_action = next(item for item in result["action_proposals"] if item["decision_id"] == "ACT-EV-PH-TREND-001")

        self.assertTrue(trend_signal["is_stale"])
        self.assertEqual(trend_signal["result_layer"], "background")
        self.assertEqual(trend_action["status"], "draft")
        self.assertEqual(trend_action["risk_level"], "low")
        self.assertNotIn(trend_action, result["approval_queue"])

    def test_run_result_exposes_d_line_scan_agent_structure(self):
        payload = OpsMVPService(WORKSPACE).run_cycle()
        result = payload["result"]

        self.assertIn("scan_agents", result)
        self.assertIn("scan_agent_summary", result)

        agents = result["scan_agents"]
        summary = result["scan_agent_summary"]
        expected_agents = {
            "market_radar",
            "competitor_watch",
            "trend_scout",
            "external_environment",
            "daily_brief_synth",
        }

        self.assertEqual(set(agents.keys()), expected_agents)
        self.assertEqual(set(summary.keys()), expected_agents)

        for agent_name in expected_agents:
            agent_payload = agents[agent_name]
            self.assertEqual(agent_payload["agent_key"], agent_name)
            self.assertTrue(agent_payload["label"])
            self.assertIn("signals", agent_payload)
            self.assertIn("tasks", agent_payload)
            self.assertIn("action_proposals", agent_payload)
            self.assertIn("responsibilities", agent_payload)
            self.assertTrue(agent_payload["responsibilities"])
            self.assertEqual(summary[agent_name]["signal_count"], len(agent_payload["signals"]))
            self.assertEqual(summary[agent_name]["task_count"], len(agent_payload["tasks"]))
            self.assertEqual(summary[agent_name]["action_count"], len(agent_payload["action_proposals"]))

        self.assertTrue(agents["market_radar"]["signals"])
        self.assertTrue(agents["competitor_watch"]["signals"])
        self.assertTrue(agents["trend_scout"]["signals"])
        self.assertTrue(agents["external_environment"]["signals"])
        self.assertTrue(agents["daily_brief_synth"]["signals"])

    def test_agent_outputs_keep_role_attribution_and_still_roll_up(self):
        payload = OpsMVPService(WORKSPACE).run_cycle()
        result = payload["result"]

        signal_sources = {item["decision_id"]: item["evidence"].get("scan_agent") for item in result["signals"]}
        task_sources = {item["decision_id"]: item["evidence"].get("scan_agent") for item in result["tasks"]}
        action_sources = {item["decision_id"]: item["evidence"].get("scan_agent") for item in result["action_proposals"]}

        self.assertEqual(signal_sources["SIG-EV-PH-WEATHER-001"], "market_radar")
        self.assertEqual(signal_sources["SIG-EV-PH-COMP-001"], "competitor_watch")
        self.assertEqual(signal_sources["SIG-EV-PH-TREND-001"], "trend_scout")
        self.assertEqual(signal_sources["SIG-EV-PH-POLICY-001"], "external_environment")

        self.assertEqual(task_sources["TASK-EV-PH-WEATHER-001"], "market_radar")
        self.assertEqual(task_sources["TASK-EV-PH-COMP-001"], "competitor_watch")
        self.assertEqual(task_sources["TASK-EV-PH-TREND-001"], "trend_scout")
        self.assertEqual(task_sources["TASK-EV-PH-POLICY-001"], "external_environment")

        self.assertEqual(action_sources["ACT-EV-PH-WEATHER-001"], "market_radar")
        self.assertEqual(action_sources["ACT-EV-PH-COMP-001"], "competitor_watch")
        self.assertEqual(action_sources["ACT-EV-PH-TREND-001"], "trend_scout")
        self.assertEqual(action_sources["ACT-EV-PH-POLICY-001"], "external_environment")

        daily_brief_ids = {item["decision_id"] for item in result["scan_agents"]["daily_brief_synth"]["signals"]}
        self.assertTrue({"SIG-EV-PH-WEATHER-001", "SIG-EV-PH-COMP-001", "SIG-EV-PH-TREND-001", "SIG-EV-PH-POLICY-001"}.issubset(daily_brief_ids))

        self.assertIn("SIG-EV-PH-WEATHER-001", {item["decision_id"] for item in result["signals"]})
        self.assertIn("TASK-EV-PH-COMP-001", {item["decision_id"] for item in result["tasks"]})
        self.assertIn("ACT-EV-PH-POLICY-001", {item["decision_id"] for item in result["action_proposals"]})

    def test_fetcher_returns_live_weather_and_rule_events_for_external_environment(self):
        weather_payload = '{"current_condition":[{"temp_C":"34","FeelsLikeC":"41","humidity":"68","weatherDesc":[{"value":"Partly cloudy"}],"observation_time":"06:00 AM"}]}'
        rule_payload = """<html><body><main><h1>TikTok Shop Mall or cross-border sellers must provide accurate product details</h1><time datetime='2026-03-15T09:00:00+08:00'></time><p>Apparel listings should clearly disclose fabric composition, size, and print placement.</p></main></body></html>"""

        with patch.object(
            ExternalSignalFetcher,
            "_fetch_text",
            side_effect=[weather_payload, rule_payload],
        ):
            fetcher = ExternalSignalFetcher(now=datetime.fromisoformat("2026-03-17T15:00:00+08:00"))
            events, fetch_status = fetcher.fetch()

        weather_event = next(item for item in events if item["source_type"] == "weather")
        rule_event = next(item for item in events if item["source_type"] == "platform_rule")

        self.assertEqual(weather_event["evidence"]["scan_agent"], "external_environment")
        self.assertEqual(rule_event["evidence"]["scan_agent"], "external_environment")
        self.assertEqual(weather_event["market"], "PH")
        self.assertEqual(rule_event["category"], "printed_tshirt")
        self.assertEqual(weather_event["source_mode"], "live")
        self.assertEqual(rule_event["source_mode"], "live")
        self.assertEqual(fetch_status["weather"]["status"], "live_ok")
        self.assertEqual(fetch_status["rule"]["status"], "live_ok")
        self.assertIn("wttr.in", weather_event["evidence"]["source_url"])
        self.assertIn("tiktokshop", rule_event["evidence"]["source_url"])

    def test_run_cycle_exposes_live_mock_and_fetch_status_when_rule_fetch_degrades(self):
        live_events = [
            {
                "event_id": "LIVE-PH-WEATHER-001",
                "title": "Metro Manila heat index from live weather feed stays elevated",
                "source_type": "weather",
                "source_tier": "high",
                "source_mode": "live",
                "update_cadence": "realtime",
                "freshness_window": "12h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T14:00:00+08:00",
                "impact_direction": "positive",
                "severity": "medium",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Live weather keeps breathable printed tees relevant.",
                "evidence": {"scan_agent": "external_environment", "source_url": "https://wttr.in/Metro%20Manila?format=j1"},
            },
            {
                "event_id": "LIVE-PH-NEWS-001",
                "title": "Manila campus fair calendar adds fresh demand cues for graphic tees",
                "source_type": "news",
                "source_tier": "medium",
                "source_mode": "live",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T12:00:00+08:00",
                "impact_direction": "positive",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Public news feed highlights campus events that can lift printed tee demand.",
                "evidence": {"scan_agent": "external_environment", "source_url": "https://news.google.com/rss/search?q=site:ph+campus+fair+Manila"},
            },
        ]
        fetch_status = {
            "weather": {"status": "live_ok", "source_mode": "live", "error": None},
            "rule": {
                "status": "degraded_to_mock",
                "source_mode": "mock",
                "error": "HTTP 403",
                "fallback_used": True,
            },
            "news": {"status": "live_ok", "source_mode": "live", "error": None, "fallback_used": False},
        }

        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", return_value=(live_events, fetch_status)):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        self.assertIn("fetch_status", result)
        self.assertEqual(result["fetch_status"]["weather"]["status"], "live_ok")
        self.assertEqual(result["fetch_status"]["weather"]["source_mode"], "live")
        self.assertEqual(result["fetch_status"]["rule"]["status"], "degraded_to_mock")
        self.assertEqual(result["fetch_status"]["rule"]["source_mode"], "mock")
        self.assertIn("403", result["fetch_status"]["rule"]["error"])
        self.assertEqual(result["fetch_status"]["news"]["status"], "live_ok")
        self.assertEqual(result["fetch_status"]["news"]["source_mode"], "live")

        signal_sources = {item["decision_id"]: item for item in result["signals"]}
        self.assertEqual(signal_sources["SIG-LIVE-PH-WEATHER-001"]["source_mode"], "live")
        self.assertEqual(signal_sources["SIG-LIVE-PH-NEWS-001"]["source_mode"], "live")
        self.assertEqual(signal_sources["SIG-LIVE-PH-NEWS-001"]["evidence"]["scan_agent"], "external_environment")
        external_ids = {item["decision_id"] for item in result["scan_agents"]["external_environment"]["signals"]}
        self.assertIn("SIG-LIVE-PH-NEWS-001", external_ids)
        self.assertTrue(all(item["source_mode"] in {"live", "mock"} for item in result["signals"]))

    def test_fetcher_returns_live_news_event_and_degrades_independently(self):
        weather_payload = '{"current_condition":[{"temp_C":"34","FeelsLikeC":"41","humidity":"68","weatherDesc":[{"value":"Partly cloudy"}],"observation_time":"06:00 AM"}]}'
        rule_payload = """<html><body><main><h1>TikTok Shop Mall or cross-border sellers must provide accurate product details</h1><time datetime='2026-03-15T09:00:00+08:00'></time><p>Apparel listings should clearly disclose fabric composition, size, and print placement.</p></main></body></html>"""
        news_payload = """<?xml version='1.0' encoding='UTF-8'?><rss><channel><item><title>Manila schools line up campus fairs ahead of summer rush</title><link>https://example.com/manila-campus-fair</link><pubDate>Mon, 17 Mar 2026 06:30:00 +0800</pubDate><description>Campus activity in Metro Manila is picking up, with student footfall expected to rise.</description></item></channel></rss>"""

        with patch.object(ExternalSignalFetcher, "_fetch_text", side_effect=[weather_payload, rule_payload, news_payload]):
            fetcher = ExternalSignalFetcher(now=datetime.fromisoformat("2026-03-17T15:00:00+08:00"))
            events, fetch_status = fetcher.fetch()

        news_event = next(item for item in events if item["source_type"] == "news")
        self.assertEqual(news_event["source_mode"], "live")
        self.assertEqual(news_event["market"], "PH")
        self.assertEqual(news_event["category"], "printed_tshirt")
        self.assertEqual(news_event["evidence"]["scan_agent"], "external_environment")
        self.assertIn("campus fair", news_event["title"].lower())
        self.assertEqual(fetch_status["news"]["status"], "live_ok")
        self.assertEqual(fetch_status["news"]["source_mode"], "live")

        with patch.object(ExternalSignalFetcher, "_fetch_text", side_effect=[weather_payload, rule_payload, RuntimeError("news rss timeout")]):
            fetcher = ExternalSignalFetcher(now=datetime.fromisoformat("2026-03-17T15:00:00+08:00"))
            degraded_events, degraded_status = fetcher.fetch()

        self.assertTrue(any(item["source_type"] == "weather" for item in degraded_events))
        self.assertTrue(any(item["source_type"] == "platform_rule" for item in degraded_events))
        self.assertFalse(any(item["source_type"] == "news" for item in degraded_events))
        self.assertEqual(degraded_status["news"]["status"], "degraded_to_mock")
        self.assertEqual(degraded_status["news"]["source_mode"], "mock")
        self.assertIn("news rss timeout", degraded_status["news"]["error"])

    def test_run_cycle_keeps_main_flow_alive_and_records_fetch_failure_summary(self):
        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", side_effect=RuntimeError("rule parser timeout")):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        self.assertTrue(result["signals"])
        self.assertIn("fetch_status", result)
        self.assertIn("weather", result["fetch_status"])
        self.assertIn("rule", result["fetch_status"])
        self.assertEqual(result["fetch_status"]["weather"]["status"], "fetch_failed")
        self.assertEqual(result["fetch_status"]["rule"]["status"], "fetch_failed")
        self.assertIn("rule parser timeout", result["fetch_status"]["weather"]["error"])
        self.assertIn("rule parser timeout", result["fetch_status"]["rule"]["error"])
        self.assertTrue(all(item["source_mode"] in {"live", "mock"} for item in result["signals"]))

    def test_run_cycle_rolls_up_live_mock_and_fallback_source_summary(self):
        live_events = [
            {
                "event_id": "LIVE-PH-WEATHER-001",
                "title": "Metro Manila heat index from live weather feed stays elevated",
                "source_type": "weather",
                "source_tier": "high",
                "source_mode": "live",
                "update_cadence": "realtime",
                "freshness_window": "12h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T14:00:00+08:00",
                "impact_direction": "positive",
                "severity": "medium",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Live weather keeps breathable printed tees relevant.",
                "evidence": {"scan_agent": "external_environment", "source_url": "https://wttr.in/Metro%20Manila?format=j1"},
            },
            {
                "event_id": "LIVE-PH-NEWS-001",
                "title": "Manila campus fair calendar adds fresh demand cues for graphic tees",
                "source_type": "news",
                "source_tier": "medium",
                "source_mode": "live",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T12:00:00+08:00",
                "impact_direction": "positive",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Public news feed highlights campus events that can lift printed tee demand.",
                "evidence": {"scan_agent": "external_environment", "source_url": "https://news.google.com/rss/search?q=site:ph+campus+fair+Manila"},
            },
        ]
        fetch_status = {
            "weather": {"status": "live_ok", "source_mode": "live", "error": None, "fallback_used": False},
            "rule": {
                "status": "degraded_to_mock",
                "source_mode": "mock",
                "error": "HTTP 403",
                "fallback_used": True,
            },
            "news": {"status": "live_ok", "source_mode": "live", "error": None, "fallback_used": False},
        }

        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", return_value=(live_events, fetch_status)):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        self.assertIn("source_summary", result)
        self.assertEqual(result["source_summary"]["totals"], {"live": 2, "mock": 2, "fallback": 2, "degraded": 1})
        self.assertEqual(result["source_summary"]["by_source"]["weather"]["source_mode"], "live")
        self.assertEqual(result["source_summary"]["by_source"]["weather"]["live_hits"], 1)
        self.assertEqual(result["source_summary"]["by_source"]["news"]["source_mode"], "live")
        self.assertEqual(result["source_summary"]["by_source"]["news"]["live_hits"], 1)
        self.assertEqual(result["source_summary"]["by_source"]["rule"]["source_mode"], "mock")
        self.assertEqual(result["source_summary"]["by_source"]["rule"]["fallback_used"], True)
        self.assertEqual(result["source_summary"]["by_source"]["rule"]["signal_status"], "degraded")

        self.assertIn("weather live", result["summary"].lower())
        self.assertIn("news live", result["summary"].lower())
        self.assertIn("rule degraded", result["summary"].lower())
        self.assertIn("source_status", result["layer_summary"])
        self.assertIn("source_status", result["freshness_summary"])
        self.assertEqual(result["fetch_status"]["rule"]["source_mode"], "mock")

    def test_fetcher_extracts_tiktok_shop_search_competitor_events(self):
        weather_payload = '{"current_condition":[{"temp_C":"34","FeelsLikeC":"41","humidity":"68","weatherDesc":[{"value":"Partly cloudy"}],"observation_time":"06:00 AM"}]}'
        rule_payload = """<html><body><main><h1>TikTok Shop Mall or cross-border sellers must provide accurate product details</h1><time datetime='2026-03-15T09:00:00+08:00'></time><p>Apparel listings should clearly disclose fabric composition, size, and print placement.</p></main></body></html>"""
        news_payload = """<?xml version='1.0' encoding='UTF-8'?><rss><channel><item><title>Manila schools line up campus fairs ahead of summer rush</title><link>https://example.com/manila-campus-fair</link><pubDate>Mon, 17 Mar 2026 06:30:00 +0800</pubDate><description>Campus activity in Metro Manila is picking up, with student footfall expected to rise.</description></item></channel></rss>"""
        competitor_payload = '''<html><body><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{"webapp.shop.search":{"search_product":{"products":[{"product_id":"1729382255011993111","title":"Oversized Graphic Tee for Men","price":{"sale_price":"₱289"},"shop_name":"Manila Print Lab","product_url":"https://shop.tiktok.com/view/product/1729382255011993111"},{"product_id":"1729382255011993222","title":"Vintage Printed Tee Unisex","price":{"sale_price":"₱319"},"shop_name":"Streetwear PH Hub","product_url":"https://shop.tiktok.com/view/product/1729382255011993222"}]}}}</script></body></html>'''

        with patch.object(ExternalSignalFetcher, "_fetch_text", side_effect=[weather_payload, rule_payload, news_payload, competitor_payload]):
            fetcher = ExternalSignalFetcher(now=datetime.fromisoformat("2026-03-17T15:00:00+08:00"))
            events, fetch_status = fetcher.fetch()

        competitor_events = [item for item in events if item["source_type"] == "competitor"]
        self.assertEqual(fetch_status["competitor_search"]["status"], "live_ok")
        self.assertEqual(fetch_status["competitor_search"]["source_mode"], "live")
        self.assertGreaterEqual(len(competitor_events), 2)
        self.assertTrue(all(item["source_mode"] == "live" for item in competitor_events))
        self.assertTrue(all(item["evidence"]["scan_agent"] == "competitor_watch" for item in competitor_events))
        self.assertTrue(all(item["evidence"]["search_term"] in {"printed tee", "graphic tee", "oversized tee"} for item in competitor_events))
        self.assertTrue(any("oversized graphic tee" in item["title"].lower() for item in competitor_events))

    def test_run_cycle_keeps_competitor_watch_alive_when_tiktok_search_degrades(self):
        live_events = [
            {
                "event_id": "LIVE-PH-COMP-SEARCH-001",
                "title": "TikTok Shop search probe degraded for printed tee",
                "source_type": "competitor",
                "source_tier": "medium",
                "source_mode": "degraded",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T15:00:00+08:00",
                "impact_direction": "neutral",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Public TikTok Shop search did not yield stable parse output; keep competitor watch in degraded probe mode.",
                "evidence": {
                    "scan_agent": "competitor_watch",
                    "search_term": "printed tee",
                    "search_terms": ["printed tee", "graphic tee", "oversized tee"],
                    "fetch_status": "degraded_parse",
                    "validation_next_step": "Open TikTok Shop PH search in browser and verify __UNIVERSAL_DATA_FOR_REHYDRATION__ payload.",
                },
            },
        ]
        fetch_status = {
            "weather": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "rule": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "news": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_search": {
                "status": "degraded_probe",
                "source_mode": "degraded",
                "error": "search page shape changed",
                "fallback_used": True,
            },
        }

        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", return_value=(live_events, fetch_status)):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        competitor_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-LIVE-PH-COMP-SEARCH-001")
        competitor_agent_ids = {item["decision_id"] for item in result["scan_agents"]["competitor_watch"]["signals"]}

        self.assertEqual(result["fetch_status"]["competitor_search"]["status"], "degraded_probe")
        self.assertEqual(result["fetch_status"]["competitor_search"]["source_mode"], "degraded")
        self.assertEqual(competitor_signal["source_mode"], "degraded")
        self.assertEqual(competitor_signal["source_type"], "competitor")
        self.assertEqual(competitor_signal["evidence"]["scan_agent"], "competitor_watch")
        self.assertEqual(competitor_signal["evidence"]["fetch_status"], "degraded_parse")
        self.assertIn("validation_next_step", competitor_signal["evidence"])
        self.assertIn(competitor_signal["decision_id"], competitor_agent_ids)
        self.assertTrue(result["tasks"])

    def test_fetcher_extracts_tiktok_shop_product_detail_events(self):
        weather_payload = '{"current_condition":[{"temp_C":"34","FeelsLikeC":"41","humidity":"68","weatherDesc":[{"value":"Partly cloudy"}],"observation_time":"06:00 AM"}]}'
        rule_payload = """<html><body><main><h1>TikTok Shop Mall or cross-border sellers must provide accurate product details</h1><time datetime='2026-03-15T09:00:00+08:00'></time><p>Apparel listings should clearly disclose fabric composition, size, and print placement.</p></main></body></html>"""
        news_payload = """<?xml version='1.0' encoding='UTF-8'?><rss><channel><item><title>Manila schools line up campus fairs ahead of summer rush</title><link>https://example.com/manila-campus-fair</link><pubDate>Mon, 17 Mar 2026 06:30:00 +0800</pubDate><description>Campus activity in Metro Manila is picking up, with student footfall expected to rise.</description></item></channel></rss>"""
        competitor_payload = '''<html><body><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{"webapp.shop.search":{"search_product":{"products":[{"product_id":"1729382255011993111","title":"Oversized Graphic Tee for Men","price":{"sale_price":"₱289"},"shop_name":"Manila Print Lab","product_url":"https://shop.tiktok.com/view/product/1729382255011993111"}]},"product_detail":{"productBase":{"title":"Oversized Graphic Tee for Men","id":"1729382255011993111"},"seller":{"name":"Manila Print Lab"},"price":{"sale_price":{"formatted_amount":"₱289"}},"rating":{"average":"4.8","count":321},"sales":{"formatted":"1.2K sold"},"seo":{"canonical":"https://shop.tiktok.com/view/product/1729382255011993111"}}}}</script></body></html>'''

        with patch.object(ExternalSignalFetcher, "_fetch_text", side_effect=[weather_payload, rule_payload, news_payload, competitor_payload, competitor_payload]):
            fetcher = ExternalSignalFetcher(now=datetime.fromisoformat("2026-03-17T15:00:00+08:00"))
            events, fetch_status = fetcher.fetch()

        detail_events = [item for item in events if item["source_type"] == "competitor" and item["evidence"].get("fetch_status") == "live_detail_ok"]
        self.assertEqual(fetch_status["competitor_detail"]["status"], "live_ok")
        self.assertEqual(fetch_status["competitor_detail"]["source_mode"], "live")
        self.assertTrue(detail_events)
        detail_event = detail_events[0]
        self.assertEqual(detail_event["evidence"]["scan_agent"], "competitor_watch")
        self.assertEqual(detail_event["evidence"]["product_title"], "Oversized Graphic Tee for Men")
        self.assertEqual(detail_event["evidence"]["price"], "₱289")
        self.assertEqual(detail_event["evidence"]["shop_name"], "Manila Print Lab")
        self.assertEqual(detail_event["evidence"]["rating"], "4.8")
        self.assertEqual(detail_event["evidence"]["review_count"], 321)
        self.assertEqual(detail_event["evidence"]["sales_marker"], "1.2K sold")
        self.assertIn("product_url", detail_event["evidence"])

    def test_run_cycle_keeps_competitor_watch_alive_when_tiktok_product_detail_degrades(self):
        live_events = [
            {
                "event_id": "DEGRADED-PH-TTS-DETAIL-001",
                "title": "TikTok Shop product detail probe degraded",
                "source_type": "competitor",
                "source_tier": "medium",
                "source_mode": "degraded",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T15:00:00+08:00",
                "impact_direction": "neutral",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "Public TikTok Shop product detail page did not yield stable structured output; keep detail probe in degraded mode.",
                "evidence": {
                    "scan_agent": "competitor_watch",
                    "product_url": "https://shop.tiktok.com/view/product/1729382255011993111",
                    "fetch_status": "degraded_detail_probe",
                    "validation_next_step": "Open the product detail page in browser and inspect whether product-detail rehydration payload or selectors changed.",
                },
            },
        ]
        fetch_status = {
            "weather": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "rule": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "news": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_search": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_detail": {
                "status": "degraded_detail_probe",
                "source_mode": "degraded",
                "error": "detail payload missing",
                "fallback_used": True,
            },
        }

        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", return_value=(live_events, fetch_status)):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        detail_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-DEGRADED-PH-TTS-DETAIL-001")
        competitor_agent_ids = {item["decision_id"] for item in result["scan_agents"]["competitor_watch"]["signals"]}

        self.assertEqual(result["fetch_status"]["competitor_detail"]["status"], "degraded_detail_probe")
        self.assertEqual(result["fetch_status"]["competitor_detail"]["source_mode"], "degraded")
        self.assertEqual(detail_signal["source_mode"], "degraded")
        self.assertEqual(detail_signal["evidence"]["scan_agent"], "competitor_watch")
        self.assertEqual(detail_signal["evidence"]["fetch_status"], "degraded_detail_probe")
        self.assertIn("validation_next_step", detail_signal["evidence"])
        self.assertIn(detail_signal["decision_id"], competitor_agent_ids)
        self.assertTrue(result["tasks"])

    def test_run_cycle_rolls_product_detail_signal_into_competitor_watch(self):
        live_events = [
            {
                "event_id": "LIVE-PH-TTS-DETAIL-1729382255011993111",
                "title": "TikTok Shop detail: Oversized Graphic Tee for Men",
                "source_type": "competitor",
                "source_tier": "medium",
                "source_mode": "live",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": "2026-03-17T15:00:00+08:00",
                "published_at": "2026-03-17T15:00:00+08:00",
                "impact_direction": "neutral",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": "TikTok Shop PH product detail surfaced title, price, shop and rating for a printed tee competitor.",
                "evidence": {
                    "scan_agent": "competitor_watch",
                    "product_id": "1729382255011993111",
                    "product_title": "Oversized Graphic Tee for Men",
                    "price": "₱289",
                    "shop_name": "Manila Print Lab",
                    "rating": "4.8",
                    "review_count": 321,
                    "sales_marker": "1.2K sold",
                    "product_url": "https://shop.tiktok.com/view/product/1729382255011993111",
                    "fetch_status": "live_detail_ok",
                    "validation_next_step": "Re-open the same product detail URL and confirm title / price / shop / rating still match the visible page.",
                },
            }
        ]
        fetch_status = {
            "weather": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "rule": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "news": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_search": {"status": "mock_only", "source_mode": "mock", "error": None, "fallback_used": True},
            "competitor_detail": {"status": "live_ok", "source_mode": "live", "error": None, "fallback_used": False},
        }

        with patch("tools.ops_mvp.service.ExternalSignalFetcher.fetch", return_value=(live_events, fetch_status)):
            payload = OpsMVPService(WORKSPACE).run_cycle()

        result = payload["result"]
        detail_signal = next(item for item in result["signals"] if item["decision_id"] == "SIG-LIVE-PH-TTS-DETAIL-1729382255011993111")
        competitor_watch = result["scan_agents"]["competitor_watch"]

        self.assertEqual(detail_signal["source_mode"], "live")
        self.assertEqual(detail_signal["evidence"]["fetch_status"], "live_detail_ok")
        self.assertEqual(detail_signal["evidence"]["shop_name"], "Manila Print Lab")
        self.assertIn(detail_signal["decision_id"], {item["decision_id"] for item in competitor_watch["signals"]})
        self.assertEqual(result["fetch_status"]["competitor_detail"]["status"], "live_ok")


if __name__ == "__main__":
    unittest.main()
