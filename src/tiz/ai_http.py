"""AI_CMD for machines without the claude CLI: prompt on stdin → reply on stdout, via an OpenAI-compatible HTTP gateway
(default BoxAPI, reachable from Iran; the hackathon key allows gpt-5.4-mini, not Claude). AI_CMD="uv run python -m tiz.ai_http" with AI_API_KEY (or BOX_API) in .env."""
import os
import sys

import httpx

from tiz import bot as B  # loads .env


def ask(prompt):
    r = httpx.post(B.env("AI_BASE_URL", "https://ai.boxapi.ir/ai/v1").rstrip("/") + "/chat/completions", timeout=110,
                   headers={"Authorization": f"Bearer {os.environ.get('AI_API_KEY') or os.environ['BOX_API']}"},
                   json=dict(model=B.env("AI_MODEL", "gpt-5.4-mini"), messages=[dict(role="user", content=prompt)]))
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


if __name__ == "__main__":
    print(ask(sys.stdin.read()))
