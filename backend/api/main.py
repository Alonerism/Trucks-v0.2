from __future__ import annotations

from fastapi import FastAPI
from .routes import router


def create_app() -> FastAPI:
    app = FastAPI(title="Fleet Optimizer")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    app.include_router(router)
    return app
