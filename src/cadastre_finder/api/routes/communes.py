"""Route GET /api/communes — liste des communes pour l'autocomplétion."""
from __future__ import annotations

from fastapi import APIRouter
from loguru import logger

from cadastre_finder.api.schemas import CommuneItem
from cadastre_finder.config import DB_PATH

router = APIRouter()

_COMMUNES_CACHE: list[CommuneItem] | None = None


def _load_communes() -> list[CommuneItem]:
    global _COMMUNES_CACHE
    if _COMMUNES_CACHE is not None:
        return _COMMUNES_CACHE

    import duckdb
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        rows = con.execute("SELECT nom, code_dept FROM communes ORDER BY nom").fetchall()
        con.close()
        _COMMUNES_CACHE = [
            CommuneItem(label=f"{nom} ({dept})", nom=nom, code_dept=dept)
            for nom, dept in rows
        ]
        logger.info(f"[communes] {len(_COMMUNES_CACHE)} communes chargées.")
    except Exception as e:
        logger.warning(f"[communes] Impossible de charger les communes : {e}")
        _COMMUNES_CACHE = []

    return _COMMUNES_CACHE


@router.get("/communes", response_model=list[CommuneItem])
async def get_communes() -> list[CommuneItem]:
    return _load_communes()
