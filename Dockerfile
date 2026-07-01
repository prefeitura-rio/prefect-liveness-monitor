FROM python:3.14-alpine AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1  UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY src/ ./src/

RUN uv sync --frozen --no-dev

FROM python:3.14-alpine AS runtime

WORKDIR /app

COPY --from=builder /app/.venv ./.venv
COPY src/ ./src/
COPY main.py .

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "main.py"]
