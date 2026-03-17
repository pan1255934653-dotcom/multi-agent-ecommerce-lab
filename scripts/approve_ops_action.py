import json
import sys
import urllib.request


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/approve_ops_action.py <action_id> [approved|rejected] [reason]")
        return 1
    action_id = sys.argv[1]
    decision = sys.argv[2] if len(sys.argv) >= 3 else "approved"
    reason = sys.argv[3] if len(sys.argv) >= 4 else "manual review"
    payload = json.dumps({
        "action_id": action_id,
        "decision": decision,
        "reviewer": "human",
        "reason": reason,
    }).encode("utf-8")
    req = urllib.request.Request(
        "http://127.0.0.1:8765/api/approve",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print(resp.read().decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
