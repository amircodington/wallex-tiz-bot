# SignalOps panel + shadow (paper) bot in one container. Live trading is refused here (NO_LIVE=1).
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends procps tzdata && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV HOST=0.0.0.0 PORT=8787 NO_LIVE=1 AUTOSTART=shadow PYTHONUNBUFFERED=1 UV_NO_SYNC=1 TZ=Asia/Tehran \
    UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project
COPY . .
RUN uv sync --frozen --no-dev
EXPOSE 8787
CMD ["uv", "run", "python", "-m", "tiz.dash"]
