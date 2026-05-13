from __future__ import annotations

"""FastAPI entrypoint with modular router composition."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from web.routers.analysis_router import router as analysis_router
from web.routers.compare_router import router as compare_router
from web.routers.explore_router import router as explore_router
from web.routers.overview_router import router as overview_router
from web.routers.queries_router import router as queries_router
from web.routers.terrain_router import router as terrain_router

APP_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="TFM GeoContext API",
    version="1.0.0",
    description="API and web layer for mineral and socioeconomic insights.",
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(overview_router)
app.include_router(explore_router)
app.include_router(analysis_router)
app.include_router(compare_router)
app.include_router(terrain_router)
app.include_router(queries_router)

