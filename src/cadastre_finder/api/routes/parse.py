"""Route POST /api/parse-ad — extraction des critères depuis un texte d'annonce."""
from __future__ import annotations

from fastapi import APIRouter

from cadastre_finder.api.schemas import ParseAdRequest, ParseAdResponse
from cadastre_finder.search.ad_parser import parse_ad_text

router = APIRouter()


@router.post("/parse-ad", response_model=ParseAdResponse)
async def parse_ad(req: ParseAdRequest) -> ParseAdResponse:
    criteria = parse_ad_text(req.text)
    return ParseAdResponse(
        terrain_surface=criteria.terrain_surface,
        living_surface=criteria.living_surface,
        dpe_label=criteria.dpe_label,
        ges_label=criteria.ges_label,
        dpe_date=criteria.dpe_date,
        commune=criteria.commune,
        postal_code=criteria.postal_code,
    )
