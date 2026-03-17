from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .service import OpsMVPService


HTML = """<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8'>
  <title>OpenClaw Ops Mock MVP</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #0b1020; color: #e8ecf3; }
    h1, h2 { margin-bottom: 8px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
    .card { background: #141b2d; padding: 16px; border-radius: 12px; border: 1px solid #24304f; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; margin-right: 6px; }
    .low { background: #123524; color: #9ef0b8; }
    .medium { background: #4e3a10; color: #ffd777; }
    .high { background: #4d1d22; color: #ff9aa5; }
    .priority { background: #1d2d4d; color: #a8c8ff; }
    button { background: #4c8dff; color: white; border: 0; padding: 10px 14px; border-radius: 8px; cursor: pointer; }
    pre { white-space: pre-wrap; word-break: break-word; }
    ul { padding-left: 18px; }
  </style>
</head>
<body>
  <h1>OpenClaw 运营决策 Mock MVP</h1>
  <p id='summary'>加载中...</p>
  <button onclick='rerun()'>重新跑一轮</button>
  <div class='grid' id='stats'></div>
  <h2>Signals</h2><div id='signals'></div>
  <h2>Tasks</h2><div id='tasks'></div>
  <h2>Action Proposals</h2><div id='actions'></div>
  <h2>审批队列</h2><div id='approvals'></div>
  <h2>自动执行</h2><div id='auto'></div>
  <script>
    async function load() {
      const resp = await fetch('/api/state');
      const data = await resp.json();
      render(data);
    }
    async function rerun() {
      await fetch('/api/run', { method: 'POST' });
      await load();
    }
    function card(item) {
      const risk = item.risk_level || 'low';
      return `<div class="card"><div><span class="pill priority">${item.priority}</span><span class="pill ${risk}">${risk}</span></div><h3>${item.title}</h3><p>${item.reason}</p><pre>${JSON.stringify(item.evidence, null, 2)}</pre></div>`;
    }
    function render(data) {
      const result = data.result;
      document.getElementById('summary').innerText = `${result.summary} 生成时间：${result.generated_at}`;
      document.getElementById('stats').innerHTML = `
        <div class='card'><h3>商品</h3><p>${data.dataset.products.length}</p></div>
        <div class='card'><h3>广告</h3><p>${data.dataset.ads.length}</p></div>
        <div class='card'><h3>市场事件</h3><p>${data.dataset.market_events.length}</p></div>
        <div class='card'><h3>达人</h3><p>${data.dataset.creators.length}</p></div>`;
      document.getElementById('signals').innerHTML = result.signals.map(card).join('');
      document.getElementById('tasks').innerHTML = result.tasks.map(card).join('');
      document.getElementById('actions').innerHTML = result.action_proposals.map(card).join('');
      document.getElementById('approvals').innerHTML = result.approval_queue.map(card).join('');
      document.getElementById('auto').innerHTML = result.auto_actions.map(card).join('');
    }
    load();
  </script>
</body>
</html>
"""


class OpsHandler(BaseHTTPRequestHandler):
    service: OpsMVPService
    latest_payload: dict

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in {"/", "/index.html"}:
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/api/state":
            self._json(self.latest_payload)
            return
        if self.path == "/api/logs":
            log_dir = self.service.runtime_dir / "logs"
            payload = {}
            for name in ["decisions.jsonl", "audit.jsonl", "task_status.jsonl", "approvals.jsonl"]:
                path = log_dir / name
                payload[name] = path.read_text(encoding="utf-8") if path.exists() else ""
            self._json(payload)
            return
        self._json({"error": "not found"}, status=404)

    def do_POST(self):
        if self.path == "/api/run":
            self.__class__.latest_payload = self.service.run_cycle()
            self._json(self.latest_payload)
            return
        if self.path == "/api/approve":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            payload = json.loads(raw.decode("utf-8"))
            record = {
                "action_id": payload.get("action_id"),
                "decision": payload.get("decision", "approved"),
                "reviewer": payload.get("reviewer", "human"),
                "reason": payload.get("reason", "manual review"),
            }
            self.service.logger.log_approval({"kind": "manual_review", **record})
            self._json({"ok": True, "record": record})
            return
        self._json({"error": "not found"}, status=404)


def run_server(workspace_root: str | Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    service = OpsMVPService(workspace_root)
    OpsHandler.service = service
    OpsHandler.latest_payload = service.run_cycle()
    server = ThreadingHTTPServer((host, port), OpsHandler)
    print(f"Ops Mock MVP running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
