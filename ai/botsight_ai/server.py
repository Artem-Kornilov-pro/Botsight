"""Internal HTTP API of ai-worker, called by the api service (Botsight.md, 13.3: /ask).
Not exposed to the internet: the api checks project membership and limits, then calls
here with a shared token."""

from __future__ import annotations

import hmac
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from .nlsql import Asker, NLSQLError

STATUS = {
    "invalid_question": 400,
    "invalid_project": 400,
    "invalid_sql": 422,
    "query_failed": 422,
    "llm_error": 502,
    "llm_disabled": 503,
}


class AskRequest(BaseModel):
    project_id: int = Field(gt=0)
    question: str = Field(min_length=1, max_length=500)


def error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


def create_app(asker: Asker, token: str, ready: Callable[[], None]) -> FastAPI:
    app = FastAPI(title="botsight ai-worker", docs_url=None, redoc_url=None)

    @app.post("/internal/ask", response_model=None)
    def ask(body: AskRequest, x_internal_token: str = Header(default="")) -> Any:
        if not token or not hmac.compare_digest(x_internal_token, token):
            return error(401, "unauthorized", "invalid internal token")
        try:
            return asker.ask(body.project_id, body.question)
        except NLSQLError as exc:
            return error(STATUS.get(exc.code, 500), exc.code, exc.message)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    def readyz() -> Any:
        try:
            ready()
        except Exception as exc:
            return error(503, "unavailable", str(exc)[:200])
        return {"status": "ready"}

    @app.get("/metrics")
    def prom(_: Request) -> PlainTextResponse:
        return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)

    return app
