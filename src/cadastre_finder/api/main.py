"""Application FastAPI — point d'entrée de l'API REST cadastre-finder."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from cadastre_finder.api.routes.communes import router as communes_router
from cadastre_finder.api.routes.parse import router as parse_router
from cadastre_finder.api.routes.search import router as search_router

app = FastAPI(
    title="Cadastre Finder API",
    version="1.0.0",
    description="API REST pour la recherche de parcelles cadastrales.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200", "http://127.0.0.1:4200"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(communes_router, prefix="/api")
app.include_router(search_router, prefix="/api")
app.include_router(parse_router, prefix="/api")

# En production : servir le build Angular depuis frontend/dist/
_STATIC_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist" / "cadastre-finder" / "browser"
if _STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="angular")
