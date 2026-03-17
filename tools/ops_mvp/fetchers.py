from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Callable
from urllib import parse, request


class ExternalSignalFetcher:
    WEATHER_URL = "https://wttr.in/Metro%20Manila?format=j1"
    RULE_URL = "https://seller-ph.tiktokshop.com/university/essay?identity=1&knowledge_id=1"
    NEWS_URL = "https://news.google.com/rss/search?q=Manila+campus+fair+apparel+when:7d&hl=en-PH&gl=PH&ceid=PH:en"
    COMPETITOR_SEARCH_URL = "https://shop.tiktok.com/search?q={query}&region=PH"
    COMPETITOR_SEARCH_TERMS = ["printed tee", "graphic tee", "oversized tee"]
    COMPETITOR_DETAIL_SAMPLE_URL = "https://shop.tiktok.com/view/product/{product_id}?region=PH"

    def __init__(self, now: datetime | None = None, timeout: int = 10):
        self.now = now.astimezone() if now else datetime.now().astimezone()
        self.timeout = timeout

    def fetch(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        fetch_status: dict[str, dict[str, Any]] = {}

        weather_event, weather_status = self._safe_fetch(self._fetch_weather_event, channel="weather")
        fetch_status["weather"] = weather_status
        if weather_event:
            events.append(weather_event)

        rule_event, rule_status = self._safe_fetch(self._fetch_rule_event, channel="rule")
        fetch_status["rule"] = rule_status
        if rule_event:
            events.append(rule_event)

        news_event, news_status = self._safe_fetch(self._fetch_news_event, channel="news")
        fetch_status["news"] = news_status
        if news_event:
            events.append(news_event)

        competitor_events, competitor_status = self._safe_fetch_many(
            self._fetch_competitor_search_events,
            channel="competitor_search",
        )
        fetch_status["competitor_search"] = competitor_status
        events.extend(competitor_events)

        detail_events, detail_status = self._safe_fetch_competitor_detail_events(competitor_events)
        fetch_status["competitor_detail"] = detail_status
        events.extend(detail_events)

        return events, fetch_status

    def _fetch_weather_event(self) -> dict[str, Any]:
        payload = json.loads(self._fetch_text(self.WEATHER_URL))
        current = payload.get("current_condition", [{}])[0]
        temp_c = self._coerce_number(current.get("temp_C"))
        feels_like_c = self._coerce_number(current.get("FeelsLikeC"))
        humidity = self._coerce_number(current.get("humidity"))
        weather_desc = ""
        descriptions = current.get("weatherDesc") or []
        if descriptions:
            weather_desc = str(descriptions[0].get("value") or "")
        impact_direction = "positive" if feels_like_c >= 36 else "neutral"
        severity = "medium" if feels_like_c >= 40 else "low"
        published_at = self.now - timedelta(hours=1)
        return {
            "event_id": "LIVE-PH-WEATHER-WTTR-001",
            "title": "Metro Manila live weather feed keeps breathable printed tee demand in focus",
            "source_type": "weather",
            "source_tier": "high",
            "source_mode": "live",
            "update_cadence": "realtime",
            "freshness_window": "12h",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": published_at.isoformat(timespec="seconds"),
            "impact_direction": impact_direction,
            "severity": severity,
            "market": "PH",
            "category": "printed_tshirt",
            "summary": f"Live wttr.in weather for Metro Manila shows {temp_c}C / feels like {feels_like_c}C with {humidity}% humidity, supporting light breathable graphic tees.",
            "evidence": {
                "scan_agent": "external_environment",
                "source_url": self.WEATHER_URL,
                "geo": "Metro Manila",
                "temperature_c": temp_c,
                "heat_index_c": feels_like_c,
                "humidity_pct": humidity,
                "condition": weather_desc,
                "provider": "wttr.in",
            },
        }

    def _fetch_rule_event(self) -> dict[str, Any]:
        html = self._fetch_text(self.RULE_URL)
        title = self._extract_first(html, [r"<h1[^>]*>(.*?)</h1>", r"<title[^>]*>(.*?)</title>"]) or "TikTok Shop PH seller academy policy page"
        summary = self._extract_first(html, [r"<p[^>]*>(.*?)</p>"]) or "Seller policy page highlights accurate apparel disclosures."
        published_raw = self._extract_first(html, [r"datetime=['\"]([^'\"]+)['\"]", r"content=['\"]([^'\"]+T[^'\"]+)['\"]"])
        published_at = self._normalize_datetime(published_raw) or (self.now - timedelta(days=1))
        return {
            "event_id": "LIVE-PH-RULE-TIKTOK-001",
            "title": self._clean_text(title)[:180],
            "source_type": "platform_rule",
            "source_tier": "high",
            "source_mode": "live",
            "update_cadence": "periodic",
            "freshness_window": "14d",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": published_at.isoformat(timespec="seconds"),
            "impact_direction": "negative",
            "severity": "high",
            "market": "PH",
            "category": "printed_tshirt",
            "summary": self._clean_text(summary)[:280],
            "evidence": {
                "scan_agent": "external_environment",
                "source_url": self.RULE_URL,
                "provider": "TikTok Shop Seller Academy PH",
                "compliance_focus": ["fabric claim", "print placement", "size chart"],
            },
        }

    def _fetch_news_event(self) -> dict[str, Any]:
        rss = self._fetch_text(self.NEWS_URL)
        title = self._extract_first(rss, [r"<item>.*?<title>(.*?)</title>"]) or "Metro Manila campus activity watch for apparel sellers"
        summary = self._extract_first(rss, [r"<item>.*?<description>(.*?)</description>"]) or "Public news feed points to campus and footfall activity in Metro Manila."
        link = self._extract_first(rss, [r"<item>.*?<link>(.*?)</link>"]) or self.NEWS_URL
        published_raw = self._extract_first(rss, [r"<item>.*?<pubDate>(.*?)</pubDate>"])
        published_at = self._normalize_datetime(published_raw) or (self.now - timedelta(hours=6))
        return {
            "event_id": "LIVE-PH-NEWS-GOOGLE-001",
            "title": self._clean_text(title)[:180],
            "source_type": "news",
            "source_tier": "medium",
            "source_mode": "live",
            "update_cadence": "intraday",
            "freshness_window": "24h",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": published_at.isoformat(timespec="seconds"),
            "impact_direction": "positive",
            "severity": "low",
            "market": "PH",
            "category": "printed_tshirt",
            "summary": self._clean_text(summary)[:280],
            "evidence": {
                "scan_agent": "external_environment",
                "source_url": self._clean_text(link),
                "feed_url": self.NEWS_URL,
                "provider": "Google News RSS",
                "coverage_focus": ["campus events", "student footfall", "Metro Manila"],
            },
        }

    def _fetch_competitor_search_events(self) -> list[dict[str, Any]]:
        keyword_pairs = [
            ("printed tee", self.COMPETITOR_SEARCH_TERMS[0]),
            ("graphic tee", self.COMPETITOR_SEARCH_TERMS[1]),
            ("oversized tee", self.COMPETITOR_SEARCH_TERMS[2]),
        ]
        primary_term, display_term = keyword_pairs[0]
        html = self._fetch_text(self.COMPETITOR_SEARCH_URL.format(query=parse.quote(primary_term)))
        products = self._extract_tiktok_search_products(html)
        if not products:
            raise ValueError("tiktok search payload missing products")

        events: list[dict[str, Any]] = []
        for index, product in enumerate(products[:6], start=1):
            title = self._clean_text(str(product.get("title") or "TikTok Shop tee listing"))
            shop_name = self._clean_text(str(product.get("shop_name") or product.get("seller_name") or "Unknown seller"))
            product_id = str(product.get("product_id") or product.get("id") or f"unknown-{index}")
            product_url = self._clean_text(str(product.get("product_url") or product.get("pdp_url") or self.COMPETITOR_SEARCH_URL.format(query=parse.quote(primary_term))))
            price_text = self._extract_price_text(product)
            events.append({
                "event_id": f"LIVE-PH-TTS-SEARCH-{product_id}",
                "title": f"TikTok Shop search hit: {title}",
                "source_type": "competitor",
                "source_tier": "medium",
                "source_mode": "live",
                "update_cadence": "intraday",
                "freshness_window": "24h",
                "captured_at": self.now.isoformat(timespec="seconds"),
                "published_at": self.now.isoformat(timespec="seconds"),
                "impact_direction": "neutral",
                "severity": "low",
                "market": "PH",
                "category": "printed_tshirt",
                "summary": f"TikTok Shop PH search for {display_term} surfaced {title} from {shop_name} at {price_text}.",
                "evidence": {
                    "scan_agent": "competitor_watch",
                    "source_url": self.COMPETITOR_SEARCH_URL.format(query=parse.quote(primary_term)),
                    "provider": "TikTok Shop search",
                    "search_term": display_term,
                    "search_terms": list(self.COMPETITOR_SEARCH_TERMS),
                    "keyword_scope": list(self.COMPETITOR_SEARCH_TERMS),
                    "product_id": product_id,
                    "product_title": title,
                    "shop_name": shop_name,
                    "price": price_text,
                    "product_url": product_url,
                    "fetch_status": "live_parse_ok",
                    "validation_next_step": "Open the same TikTok Shop PH search URL in browser and confirm top results still expose product cards.",
                },
            })
        return events

    def _build_competitor_probe_event(self, *, error: str) -> dict[str, Any]:
        return {
            "event_id": "DEGRADED-PH-TTS-SEARCH-PROBE-001",
            "title": "TikTok Shop competitor search probe degraded",
            "source_type": "competitor",
            "source_tier": "medium",
            "source_mode": "degraded",
            "update_cadence": "intraday",
            "freshness_window": "24h",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": self.now.isoformat(timespec="seconds"),
            "impact_direction": "neutral",
            "severity": "low",
            "market": "PH",
            "category": "printed_tshirt",
            "summary": "TikTok Shop PH search page is not yet parsing stably, so competitor_watch stays in degraded probe mode with explicit validation steps.",
            "evidence": {
                "scan_agent": "competitor_watch",
                "provider": "TikTok Shop search",
                "search_term": self.COMPETITOR_SEARCH_TERMS[0],
                "search_terms": list(self.COMPETITOR_SEARCH_TERMS),
                "keyword_scope": list(self.COMPETITOR_SEARCH_TERMS),
                "fetch_status": "degraded_parse",
                "validation_next_step": "Open TikTok Shop PH search for printed tee / graphic tee / oversized tee and verify whether the rehydration payload or product-card selectors changed.",
                "last_error": error,
            },
        }

    def _fetch_competitor_detail_events(self, search_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        candidate_url = None
        candidate_id = None
        for event in search_events:
            evidence = event.get("evidence", {}) if isinstance(event, dict) else {}
            candidate_url = evidence.get("product_url")
            candidate_id = evidence.get("product_id")
            if candidate_url:
                break
        if not candidate_url:
            raise ValueError("competitor detail probe missing candidate product_url")

        html = self._fetch_text(str(candidate_url))
        detail = self._extract_tiktok_product_detail(html)
        if not detail:
            raise ValueError("tiktok detail payload missing structured fields")

        product_id = str(detail.get("product_id") or candidate_id or "unknown-detail")
        product_title = self._clean_text(str(detail.get("product_title") or "TikTok Shop tee listing"))
        price_text = self._clean_text(str(detail.get("price") or "price_unavailable"))
        shop_name = self._clean_text(str(detail.get("shop_name") or "Unknown seller"))
        product_url = self._clean_text(str(detail.get("product_url") or candidate_url))
        rating = detail.get("rating")
        review_count = detail.get("review_count")
        sales_marker = self._clean_text(str(detail.get("sales_marker") or ""))

        return [{
            "event_id": f"LIVE-PH-TTS-DETAIL-{product_id}",
            "title": f"TikTok Shop detail: {product_title}",
            "source_type": "competitor",
            "source_tier": "medium",
            "source_mode": "live",
            "update_cadence": "intraday",
            "freshness_window": "24h",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": self.now.isoformat(timespec="seconds"),
            "impact_direction": "neutral",
            "severity": "low",
            "market": "PH",
            "category": "printed_tshirt",
            "summary": f"TikTok Shop PH product detail surfaced title, price, shop and rating for {product_title}.",
            "evidence": {
                "scan_agent": "competitor_watch",
                "provider": "TikTok Shop product detail",
                "product_id": product_id,
                "product_title": product_title,
                "price": price_text,
                "shop_name": shop_name,
                "rating": rating,
                "review_count": review_count,
                "sales_marker": sales_marker,
                "product_url": product_url,
                "fetch_status": "live_detail_ok",
                "validation_next_step": "Re-open the same product detail URL and confirm title / price / shop / rating still match the visible page.",
            },
        }]

    def _build_competitor_detail_probe_event(self, *, error: str, product_url: str | None = None) -> dict[str, Any]:
        return {
            "event_id": "DEGRADED-PH-TTS-DETAIL-001",
            "title": "TikTok Shop product detail probe degraded",
            "source_type": "competitor",
            "source_tier": "medium",
            "source_mode": "degraded",
            "update_cadence": "intraday",
            "freshness_window": "24h",
            "captured_at": self.now.isoformat(timespec="seconds"),
            "published_at": self.now.isoformat(timespec="seconds"),
            "impact_direction": "neutral",
            "severity": "low",
            "market": "PH",
            "category": "printed_tshirt",
            "summary": "TikTok Shop PH product detail page is not yet parsing stably, so competitor_watch keeps a degraded detail probe with explicit verification steps.",
            "evidence": {
                "scan_agent": "competitor_watch",
                "provider": "TikTok Shop product detail",
                "product_url": product_url or self.COMPETITOR_DETAIL_SAMPLE_URL.format(product_id="sample"),
                "fetch_status": "degraded_detail_probe",
                "validation_next_step": "Open the product detail page in browser and inspect whether the product-detail rehydration payload or visible selectors changed.",
                "last_error": error,
            },
        }

    def _extract_tiktok_search_products(self, html: str) -> list[dict[str, Any]]:
        script_payload = self._extract_first(
            html,
            [r'<script[^>]+id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>(.*?)</script>'],
        )
        if not script_payload:
            return []
        cleaned_payload = self._clean_script_json(script_payload)
        try:
            payload = json.loads(cleaned_payload)
            default_scope = payload.get("__DEFAULT_SCOPE__", {})
            candidate_paths = [
                ["webapp.shop.search", "search_product", "products"],
                ["webapp.shop.search", "searchResult", "products"],
                ["seo.abtest", "search_product", "products"],
            ]
            for path in candidate_paths:
                node: Any = default_scope
                for key in path:
                    if not isinstance(node, dict):
                        node = None
                        break
                    node = node.get(key)
                if isinstance(node, list):
                    return [item for item in node if isinstance(item, dict)]
        except Exception:
            pass

        product_matches = re.finditer(
            r'\{"product_id":"(?P<product_id>[^"]+)".*?"title":"(?P<title>[^"]+)".*?"sale_price":"(?P<sale_price>[^"]+)".*?"shop_name":"(?P<shop_name>[^"]+)".*?"product_url":"(?P<product_url>[^"]+)"',
            cleaned_payload,
            flags=re.DOTALL,
        )
        products: list[dict[str, Any]] = []
        for match in product_matches:
            products.append({
                "product_id": match.group("product_id"),
                "title": match.group("title"),
                "price": {"sale_price": match.group("sale_price")},
                "shop_name": match.group("shop_name"),
                "product_url": match.group("product_url"),
            })
        return products

    def _extract_tiktok_product_detail(self, html: str) -> dict[str, Any]:
        script_payload = self._extract_first(
            html,
            [r'<script[^>]+id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>(.*?)</script>'],
        )
        if not script_payload:
            return {}
        cleaned_payload = self._clean_script_json(script_payload)
        try:
            payload = json.loads(cleaned_payload)
            default_scope = payload.get("__DEFAULT_SCOPE__", {})
            candidate_nodes = [
                ["webapp.shop.search", "product_detail"],
                ["webapp.shop.pdp", "product_detail"],
                ["webapp.shop.detail", "product_detail"],
            ]
            for path in candidate_nodes:
                node: Any = default_scope
                for key in path:
                    if not isinstance(node, dict):
                        node = None
                        break
                    node = node.get(key)
                if isinstance(node, dict):
                    normalized = self._normalize_tiktok_product_detail(node)
                    if normalized.get("product_title") or normalized.get("product_id"):
                        return normalized
        except Exception:
            pass

        regex_fallback = {
            "product_id": self._extract_first(cleaned_payload, [r'"productBase":\{"title":"[^"]+","id":"([^"]+)"', r'"product_id":"([^"]+)"']),
            "product_title": self._extract_first(cleaned_payload, [r'"productBase":\{"title":"([^"]+)"', r'"title":"([^"]+)"']),
            "shop_name": self._extract_first(cleaned_payload, [r'"seller":\{"name":"([^"]+)"', r'"shop_name":"([^"]+)"']),
            "price": self._extract_first(cleaned_payload, [r'"formatted_amount":"([^"]+)"', r'"sale_price":"([^"]+)"']),
            "rating": self._extract_first(cleaned_payload, [r'"rating":\{"average":"([^"]+)"']),
            "review_count": self._coerce_optional_number(self._extract_first(cleaned_payload, [r'"rating":\{"average":"[^"]+","count":(\d+)'])),
            "sales_marker": self._extract_first(cleaned_payload, [r'"sales":\{"formatted":"([^"]+)"']),
            "product_url": self._extract_first(cleaned_payload, [r'"canonical":"([^"]+)"', r'"product_url":"([^"]+)"']),
        }
        if regex_fallback.get("product_title") or regex_fallback.get("product_id"):
            return regex_fallback
        return {}

    def _normalize_tiktok_product_detail(self, detail: dict[str, Any]) -> dict[str, Any]:
        product_base = detail.get("productBase") if isinstance(detail.get("productBase"), dict) else {}
        seller = detail.get("seller") if isinstance(detail.get("seller"), dict) else {}
        seo = detail.get("seo") if isinstance(detail.get("seo"), dict) else {}
        rating = detail.get("rating") if isinstance(detail.get("rating"), dict) else {}
        sales = detail.get("sales") if isinstance(detail.get("sales"), dict) else {}
        price = detail.get("price") if isinstance(detail.get("price"), dict) else {}
        sale_price = price.get("sale_price") if isinstance(price.get("sale_price"), dict) else {}

        return {
            "product_id": product_base.get("id") or detail.get("product_id") or detail.get("id"),
            "product_title": product_base.get("title") or detail.get("title"),
            "shop_name": seller.get("name") or detail.get("shop_name"),
            "price": sale_price.get("formatted_amount") or price.get("formatted_price") or detail.get("price_text"),
            "rating": str(rating.get("average") or "").strip() or None,
            "review_count": self._coerce_optional_number(rating.get("count")),
            "sales_marker": sales.get("formatted") or detail.get("sales_text"),
            "product_url": seo.get("canonical") or detail.get("product_url"),
        }

    def _clean_script_json(self, value: str) -> str:
        return value.strip().replace("&quot;", '"')

    def _extract_price_text(self, product: dict[str, Any]) -> str:
        price = product.get("price")
        if isinstance(price, dict):
            for key in ["sale_price", "price", "formatted_price", "min_price"]:
                if price.get(key):
                    return self._clean_text(str(price[key]))
        if price:
            return self._clean_text(str(price))
        for key in ["sale_price", "formatted_price", "price_text"]:
            if product.get(key):
                return self._clean_text(str(product[key]))
        return "price_unavailable"

    def _safe_fetch(self, fetcher: Callable[[], dict[str, Any]], *, channel: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        try:
            event = fetcher()
            return event, {
                "status": "live_ok",
                "source_mode": "live",
                "error": None,
                "fallback_used": False,
            }
        except Exception as exc:
            return None, {
                "status": "degraded_to_mock",
                "source_mode": "mock",
                "error": str(exc),
                "fallback_used": True,
                "channel": channel,
            }

    def _safe_fetch_many(self, fetcher: Callable[[], list[dict[str, Any]]], *, channel: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        try:
            events = fetcher()
            return events, {
                "status": "live_ok",
                "source_mode": "live",
                "error": None,
                "fallback_used": False,
                "event_count": len(events),
            }
        except Exception as exc:
            return [self._build_competitor_probe_event(error=str(exc))], {
                "status": "degraded_probe",
                "source_mode": "degraded",
                "error": str(exc),
                "fallback_used": True,
                "channel": channel,
                "event_count": 1,
            }

    def _safe_fetch_competitor_detail_events(self, search_events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        candidate_url = None
        for event in search_events:
            evidence = event.get("evidence", {}) if isinstance(event, dict) else {}
            candidate_url = evidence.get("product_url")
            if candidate_url:
                break
        try:
            events = self._fetch_competitor_detail_events(search_events)
            return events, {
                "status": "live_ok",
                "source_mode": "live",
                "error": None,
                "fallback_used": False,
                "event_count": len(events),
            }
        except StopIteration:
            return [], {
                "status": "not_attempted",
                "source_mode": "mock",
                "error": None,
                "fallback_used": True,
                "channel": "competitor_detail",
                "event_count": 0,
            }
        except Exception as exc:
            return [self._build_competitor_detail_probe_event(error=str(exc), product_url=str(candidate_url) if candidate_url else None)], {
                "status": "degraded_detail_probe",
                "source_mode": "degraded",
                "error": str(exc),
                "fallback_used": True,
                "channel": "competitor_detail",
                "event_count": 1,
            }

    def _fetch_text(self, url: str) -> str:
        req = request.Request(url, headers={"User-Agent": "Mozilla/5.0 OpenClaw ops_mvp/1.0"})
        with request.urlopen(req, timeout=self.timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace")

    def _extract_first(self, text: str, patterns: list[str]) -> str | None:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1)
        return None

    def _clean_text(self, value: str) -> str:
        value = re.sub(r"<[^>]+>", " ", value)
        value = unescape(value)
        return re.sub(r"\s+", " ", value).strip()

    def _normalize_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(self.now.tzinfo)
        except ValueError:
            try:
                dt = parsedate_to_datetime(value)
                return dt.astimezone(self.now.tzinfo)
            except Exception:
                return None

    def _coerce_number(self, value: Any) -> int:
        try:
            return int(float(str(value)))
        except Exception:
            return 0

    def _coerce_optional_number(self, value: Any) -> int | None:
        try:
            return int(float(str(value)))
        except Exception:
            return None
