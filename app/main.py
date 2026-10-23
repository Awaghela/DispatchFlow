"""FastAPI entrypoint:  uvicorn app.main:app"""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .api.middleware import MetricsMiddleware
from .api.routes import router
from .config import get_settings
from .db import engine, init_db
from .errors import DomainError
from .hub import hub
from .redis_store import store

cfg = get_settings()
logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await store.start()
    await hub.start()
    yield
    await hub.stop()
    await store.stop()
    await engine.dispose()


app = FastAPI(
    title="DispatchFlow",
    description="Real-time delivery dispatch: order <-> courier matching, live status streaming.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)
app.include_router(router)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.exception_handler(DomainError)
async def domain_error_handler(_: Request, exc: DomainError):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.code, "detail": exc.detail})
