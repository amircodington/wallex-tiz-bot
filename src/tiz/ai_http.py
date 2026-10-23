"""AI_CMD for machines without the claude CLI: prompt on stdin → reply on stdout, via an OpenAI-compatible HTTP gateway
(default GapGPT, reachable from Iran; BoxAPI via AI_BASE_URL). AI_CMD="uv run python -m tiz.ai_http"; key from AI_API_KEY, else BOX_API_SECONF
(allows Claude Haiku, the backtested judge), else BOX_API (hackathon key: gpt-5.4-mini only, set AI_MODEL)."""
import os
import sys

import httpx

from tiz import bot as B  # loads .env


def ask(prompt):
    r = httpx.post(B.env("AI_BASE_URL", "https://api.gapgpt.app/v1").rstrip("/") + "/chat/completions", timeout=110,
                   headers={"Authorization": f"Bearer {next(os.environ[k] for k in ('AI_API_KEY', 'BOX_API_SECONF', 'BOX_API') if os.environ.get(k)).strip()}"},
                   json=dict(model=B.env("AI_MODEL", "claude-haiku-5-5"), messages=[dict(role="user", content=prompt)]))
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


if __name__ == "__main__":
    print(ask(sys.stdin.read()))
