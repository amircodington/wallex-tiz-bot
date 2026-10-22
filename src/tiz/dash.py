"""Local dashboard for backtests: uv run python -m tiz.dash → http://localhost:8787
Serves results/ (live runs write results/live/<tag>.json every 30 simulated minutes) and dash.html, which polls."""
import json
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tiz.backtest import OUT

PAGE = Path(__file__).with_name("dash.html")


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            return self.send(PAGE.read_bytes(), "text/html; charset=utf-8")
        if self.path == "/index.json":  # running/finished live runs + saved single-run results that have an equity curve
            live = sorted(p.name for p in (OUT / "live").glob("*.json"))
            done = []
            for p in sorted(OUT.glob("*.json")):
                try:
                    if '"equity"' in p.read_text()[:200_000] and isinstance(json.loads(p.read_text()), dict):
                        done.append(p.name)
                except (OSError, ValueError):
                    pass
            return self.send(json.dumps(dict(live=live, results=done)).encode(), "application/json")
        super().do_GET()

    def send(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    (OUT / "live").mkdir(parents=True, exist_ok=True)
    print("http://localhost:8787")
    ThreadingHTTPServer(("127.0.0.1", 8787), partial(Handler, directory=str(OUT))).serve_forever()
