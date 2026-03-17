from pathlib import Path

from tools.ops_mvp.web import run_server


if __name__ == "__main__":
    run_server(Path(__file__).resolve().parents[1])
