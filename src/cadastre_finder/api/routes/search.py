"""Routes de recherche — POST /api/search/parcelles et /api/search/dpe-positions."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from cadastre_finder.api.schemas import (
    DPEPositionMatchSchema,
    SearchDPERequest,
    SearchParcelRequest,
    SearchResult,
    dpe_position_to_schema,
    result_to_schema,
)
from cadastre_finder.config import DB_PATH
from cadastre_finder.search.models import NeighborMode

router = APIRouter()


def _run_parcel_search(req: SearchParcelRequest) -> list:
    from cadastre_finder.search.orchestrator import search_orchestrated

    try:
        neighbor_mode = NeighborMode(req.neighbor_mode)
    except ValueError:
        neighbor_mode = NeighborMode.NONE

    return search_orchestrated(
        commune=req.commune,
        surface_m2=req.surface_m2,
        living_surface=req.living_surface,
        dpe_label=req.dpe_label,
        ges_label=req.ges_label,
        postal_code=req.postal_code,
        tolerance_pct=req.tolerance_pct,
        neighbor_mode=neighbor_mode,
        db_path=DB_PATH,
    )


def _run_dpe_search(req: SearchDPERequest) -> list:
    from cadastre_finder.processing.adjacency import resolve_insee_scope
    from cadastre_finder.search.dpe_match import search_dpe_positions
    from cadastre_finder.utils.geocoding import resolve_commune

    try:
        neighbor_mode = NeighborMode(req.neighbor_mode)
    except ValueError:
        neighbor_mode = NeighborMode.NONE

    res = resolve_commune(req.commune, postal_code=req.postal_code, db_path=DB_PATH)
    if not res or not res.best:
        return []

    scope_rang = resolve_insee_scope(res.best.code_insee, neighbor_mode, DB_PATH)

    return search_dpe_positions(
        scope_rang=scope_rang,
        living_surface=req.living_surface,
        dpe_label=req.dpe_label,
        ges_label=req.ges_label,
        dpe_date=req.dpe_date,
        conso_ep=req.conso_ep if req.conso_ep and req.conso_ep > 0 else None,
        ges_ep=req.ges_ep if req.ges_ep and req.ges_ep > 0 else None,
        tolerance_pct=req.tolerance_pct,
        db_path=DB_PATH,
    )


@router.post("/search/parcelles")
async def search_parcelles(req: SearchParcelRequest) -> list[SearchResult]:
    if not req.commune:
        raise HTTPException(status_code=422, detail="La commune est requise.")
    if req.surface_m2 <= 0:
        raise HTTPException(status_code=422, detail="La surface doit être positive.")

    results = await run_in_threadpool(_run_parcel_search, req)
    return [result_to_schema(r) for r in results]


@router.post("/search/dpe-positions")
async def search_dpe_positions_route(req: SearchDPERequest) -> list[DPEPositionMatchSchema]:
    if not req.commune:
        raise HTTPException(status_code=422, detail="La commune est requise.")
    if req.living_surface <= 0:
        raise HTTPException(status_code=422, detail="La surface habitable doit être positive.")

    results = await run_in_threadpool(_run_dpe_search, req)
    return [dpe_position_to_schema(r) for r in results]
