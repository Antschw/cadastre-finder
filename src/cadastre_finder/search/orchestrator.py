"""Orchestrateur de recherche.

Deux modes :
- **DPE-led** (un DPE et/ou GES est fourni) : on consulte la base ADEME pour
  identifier jusqu'à 20 adresses, on les géo-code via la Géoplateforme et on
  retourne les parcelles (ou agrégats) correspondantes. La phase combo n'est
  pas exécutée.
- **Combo-led** (aucun label DPE/GES) : on cherche des parcelles individuelles
  ou des agrégations adjacentes dont la surface correspond à la cible.

Dans les deux cas, le périmètre géographique est contrôlé par `neighbor_mode`
(`NONE`, `RANK1`, `RANK2`). Les filtres durs et le scoring sont communs.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional, Union

import duckdb
from loguru import logger

from cadastre_finder.config import (
    DB_PATH, DEFAULT_TOLERANCE_PCT, DEFAULT_TOP_N,
    SCORE_BONUS_DPE_LABEL, SCORE_BONUS_DPE_PARCEL,
    SCORE_W_COMPACT, SCORE_W_DISTANCE, SCORE_W_OCCUPATION, SCORE_W_SURFACE,
)
from cadastre_finder.processing.adjacency import resolve_insee_scope
from cadastre_finder.search.ad_parser import parse_ad_text
from cadastre_finder.search.building_filter import _polsby_popper_geojson, apply_hard_filters
from cadastre_finder.search.combo_match import search_combos
from cadastre_finder.search.dpe_match import dpe_led_search, enrich_combos_dpe
from cadastre_finder.search.models import ComboMatch, NeighborMode, ParcelMatch
from cadastre_finder.search.strict_match import search_strict
from cadastre_finder.utils.geocoding import resolve_commune


# ---------------------------------------------------------------------------
# Recherche orchestrée
# ---------------------------------------------------------------------------

def search_orchestrated(
    commune: str,
    surface_m2: float,
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    postal_code: Optional[str] = None,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    neighbor_mode: NeighborMode = NeighborMode.NONE,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Coordonne la recherche selon la présence (ou non) d'un DPE/GES."""
    res = resolve_commune(commune, postal_code, db_path)
    if not res.best:
        logger.warning(f"[orchestrator] Commune introuvable : '{commune}'")
        return []

    code_insee_main = res.best.code_insee
    nom_main = res.best.nom
    scope_rang = resolve_insee_scope(code_insee_main, neighbor_mode, db_path)

    logger.info(
        f"[orchestrator] '{nom_main}' ({code_insee_main}), "
        f"mode={neighbor_mode.value}, périmètre={len(scope_rang)} commune(s)"
    )

    if dpe_label or ges_label:
        results: list[Union[ParcelMatch, ComboMatch]] = list(dpe_led_search(
            insee_codes=list(scope_rang.keys()),
            target_terrain=surface_m2,
            living_surface=living_surface,
            dpe_label=dpe_label,
            ges_label=ges_label,
            tolerance_pct=tolerance_pct,
            limit=DEFAULT_TOP_N,
            db_path=db_path,
        ))
        # Propage le rang depuis le scope (pour le rendu UI)
        for m in results:
            if isinstance(m, ParcelMatch):
                m.rank = scope_rang.get(m.code_insee, 0)
            else:
                m.rank = min(scope_rang.get(p.code_insee, 0) for p in m.parts)
    else:
        results = _combo_led_search(
            commune=commune,
            surface_m2=surface_m2,
            postal_code=postal_code,
            tolerance_pct=tolerance_pct,
            neighbor_mode=neighbor_mode,
            db_path=db_path,
        )

    results = apply_hard_filters(
        results,
        target_terrain=surface_m2,
        tolerance_pct=tolerance_pct,
        db_path=db_path,
    )

    commune_centroid = _commune_centroid(code_insee_main, db_path)
    return _score_and_limit(
        results,
        target_terrain=surface_m2,
        target_living=living_surface,
        query_dpe_label=dpe_label,
        commune_centroid=commune_centroid,
    )


# ---------------------------------------------------------------------------
# Branche combo (sans DPE/GES)
# ---------------------------------------------------------------------------

def _combo_led_search(
    commune: str,
    surface_m2: float,
    postal_code: Optional[str],
    tolerance_pct: float,
    neighbor_mode: NeighborMode,
    db_path: Path,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Recherche combo classique : strict + agrégations adjacentes."""
    out: list[Union[ParcelMatch, ComboMatch]] = []
    seen: set[frozenset[str]] = set()

    strict = search_strict(commune, surface_m2, postal_code, tolerance_pct, db_path=db_path)
    for m in strict:
        ids = frozenset([m.id_parcelle])
        if ids in seen:
            continue
        seen.add(ids)
        out.append(m)

    combos = search_combos(
        commune, surface_m2, postal_code, tolerance_pct,
        neighbor_mode=neighbor_mode, anchors_only=True, db_path=db_path,
    )
    enrich_combos_dpe(combos, db_path=db_path)
    for m in combos:
        ids = frozenset(m.ids)
        if ids in seen:
            continue
        seen.add(ids)
        out.append(m)

    return out


# ---------------------------------------------------------------------------
# Scoring & tri
# ---------------------------------------------------------------------------

def _commune_centroid(code_insee: str, db_path: Path) -> Optional[tuple[float, float]]:
    """Récupère le centroïde (lat, lon) de la commune principale."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return None
    try:
        con.execute("LOAD spatial;")
        row = con.execute(
            "SELECT ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)) "
            "FROM communes WHERE code_insee = ?",
            [code_insee],
        ).fetchone()
        if row and row[0] is not None and row[1] is not None:
            return float(row[0]), float(row[1])
        return None
    except Exception:
        return None
    finally:
        con.close()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _score_one(
    result: Union[ParcelMatch, ComboMatch],
    target_terrain: float,
    target_living: Optional[float],
    query_dpe_label: Optional[str],
    commune_centroid: Optional[tuple[float, float]],
) -> float:
    surf = result.contenance if isinstance(result, ParcelMatch) else result.total_contenance
    if isinstance(result, ParcelMatch):
        barea = result.built_area or 0.0
    else:
        barea = sum(p.built_area or 0.0 for p in result.parts)

    # Distance commune (décroissance exponentielle, échelle 5 km)
    if commune_centroid:
        d_km = _haversine_km(
            result.centroid_lat, result.centroid_lon,
            commune_centroid[0], commune_centroid[1],
        )
        s_distance = math.exp(-d_km / 5.0) * SCORE_W_DISTANCE
    else:
        s_distance = 0.0

    # Surface
    ecart_rel = abs(surf - target_terrain) / target_terrain if target_terrain else 1.0
    s_surface = max(0.0, 1.0 - ecart_rel) * SCORE_W_SURFACE

    # Occupation : surface habitable / emprise bâtie idéale ∈ [0.5, 3]
    if target_living and barea > 0:
        ratio = target_living / barea
        if 0.5 <= ratio <= 3.0:
            s_occupation = SCORE_W_OCCUPATION
        else:
            distance_from_band = (
                0.5 - ratio if ratio < 0.5 else ratio - 3.0
            )
            s_occupation = max(0.0, SCORE_W_OCCUPATION * (1.0 - min(distance_from_band / 3.0, 1.0)))
    else:
        s_occupation = 0.0

    # Compacité
    if isinstance(result, ComboMatch):
        pp = result.compactness
    else:
        pp = _polsby_popper_geojson(result.geometry_geojson)
    s_compact = pp * SCORE_W_COMPACT

    # Bonus DPE
    bonus = 0.0
    if result.dpe_label:
        bonus += SCORE_BONUS_DPE_PARCEL
        if query_dpe_label and result.dpe_label == query_dpe_label:
            bonus += SCORE_BONUS_DPE_LABEL

    return s_distance + s_surface + s_occupation + s_compact + bonus


def _score_and_limit(
    results: list[Union[ParcelMatch, ComboMatch]],
    *,
    target_terrain: float,
    target_living: Optional[float],
    query_dpe_label: Optional[str],
    commune_centroid: Optional[tuple[float, float]],
) -> list[Union[ParcelMatch, ComboMatch]]:
    for r in results:
        r.score = _score_one(
            r, target_terrain, target_living, query_dpe_label, commune_centroid
        )

    results.sort(
        key=lambda r: (
            -r.score,
            abs((r.contenance if isinstance(r, ParcelMatch) else r.total_contenance) - target_terrain),
        )
    )
    return results[:DEFAULT_TOP_N]


# ---------------------------------------------------------------------------
# Recherche depuis le texte d'une annonce
# ---------------------------------------------------------------------------

def search_from_text(
    text: str,
    commune_hint: Optional[str] = None,
    neighbor_mode: NeighborMode = NeighborMode.NONE,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Point d'entrée principal : extrait les critères du texte et orchestre la recherche."""
    criteria = parse_ad_text(text)

    commune = commune_hint or criteria.commune
    if not commune:
        logger.warning("Aucune commune identifiée dans l'annonce ou fournie.")
        return []

    if not criteria.terrain_surface:
        logger.warning("Aucune surface de terrain identifiée dans l'annonce.")
        return []

    return search_orchestrated(
        commune=commune,
        surface_m2=criteria.terrain_surface,
        living_surface=criteria.living_surface,
        dpe_label=criteria.dpe_label,
        ges_label=criteria.ges_label,
        neighbor_mode=neighbor_mode,
        db_path=db_path,
    )
