"""Application FastAPI — point d'entrée de l'API REST cadastre-finder."""
from __future__ import annotations

import sys
from pathlib import Path

import webbrowser

from fastapi import FastAPI, HTTPException, Query
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


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/open-url")
def open_external_url(url: str = Query(...)) -> dict:
    """Ouvre une URL dans le navigateur système (utilisé par l'app Tauri)."""
    if not (url.startswith("https://") or url.startswith("http://")):
        raise HTTPException(status_code=400, detail="URL invalide")
    webbrowser.open(url)
    return {"ok": True}


# En mode PyInstaller, les fichiers Angular sont embarqués dans le bundle
if getattr(sys, "frozen", False):
    _STATIC_DIR = Path(sys._MEIPASS) / "frontend_static"  # type: ignore[attr-defined]
else:
    _STATIC_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist" / "frontend" / "browser"

if _STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="angular")
