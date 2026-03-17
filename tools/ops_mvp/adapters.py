from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class BaseAdapter:
    def load(self) -> list[dict[str, Any]]:
        raise NotImplementedError


class MockJsonAdapter(BaseAdapter):
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> list[dict[str, Any]]:
        return json.loads(self.path.read_text(encoding="utf-8"))


class DataRegistry:
    def __init__(self, root: str | Path):
        root = Path(root)
        self.products = MockJsonAdapter(root / "products.json")
        self.ads = MockJsonAdapter(root / "ads.json")
        self.market_events = MockJsonAdapter(root / "market_events.json")
        self.creators = MockJsonAdapter(root / "creators.json")

    def load_all(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "products": self.products.load(),
            "ads": self.ads.load(),
            "market_events": self.market_events.load(),
            "creators": self.creators.load(),
        }
