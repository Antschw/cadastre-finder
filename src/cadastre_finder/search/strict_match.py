"""Moteur de recherche — Étape 1 : match strict (ville + surface exacte).

Usage :
    matches = search_strict("Mortagne-au-Perche", surface_m2=4200)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from cadastre_finder.config import DB_PATH, MIN_TERRAIN_M2
from cadastre_finder.search.building_filter import filter_built_parcels
from cadastre_finder.search.models import ParcelMatch
from cadastre_finder.utils.geocoding import resolve_commune


def search_strict(
    commune: str,
    surface_m2: float,
    postal_code: Optional[str] = None,
    tolerance_pct: float = 0.0,
    min_surface: float = MIN_TERRAIN_M2,
    built_only: bool = True,
    db_path: Path = DB_PATH,
) -> list[ParcelMatch]:
    """Recherche des parcelles correspondant exactement à une commune et une surface.

    Args:
        commune:       Nom de la commune annoncée
        surface_m2:    Surface en m² annoncée
        postal_code:   Code postal optionnel pour lever les ambiguïtés
        tolerance_pct: Tolérance sur la surface en % (0 = exact)
        min_surface:   Surface minimale du terrain (filtre pré-discriminant)
        db_path:       Chemin DuckDB

    Returns:
        Liste de ParcelMatch triée par écart de surface croissant.
    """
    result = resolve_commune(commune, postal_code, db_path)
    if not result.candidates:
        logger.warning(f"[strict_match] Commune introuvable : '{commune}'")
        return []

    if len(result.candidates) > 1 and result.unique is None:
        logger.info(
            f"[strict_match] Ambiguïté pour '{commune}' : "
            f"{[c.code_insee for c in result.candidates]}. "
            f"Utilisation du meilleur candidat."
        )

    best = result.best
    code_insee = best.code_insee
    nom_commune = best.nom

    delta = surface_m2 * tolerance_pct / 100.0
    surface_min = surface_m2 - delta
    surface_max = surface_m2 + delta

    logger.info(
        f"[strict_match] Recherche '{nom_commune}' ({code_insee}), "
        f"surface [{surface_min:.0f}, {surface_max:.0f}] m², "
        f"terrain >= {min_surface} m²"
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        rows = con.execute("""
            SELECT
                id,
                code_insee,
                contenance,
                ST_X(ST_Centroid(geometry)) AS lon,
                ST_Y(ST_Centroid(geometry)) AS lat,
                ST_AsGeoJSON(geometry)      AS geojson
            FROM parcelles
            WHERE code_insee = ?
              AND contenance >= ?
              AND contenance BETWEEN ? AND ?
            ORDER BY ABS(contenance - ?) ASC
        """, [
            code_insee,
            min_surface,
            surface_min, surface_max,
            surface_m2,
        ]).fetchall()

        matches = []
        for id_parc, insee, contenance, lon, lat, geojson in rows:
            ecart = abs(contenance - surface_m2)
            score = max(0.0, 100.0 - (ecart / surface_m2 * 100.0))
            matches.append(ParcelMatch(
                id_parcelle=id_parc,
                code_insee=insee,
                nom_commune=nom_commune,
                contenance=contenance,
                centroid_lat=lat,
                centroid_lon=lon,
                geometry_geojson=geojson or "{}",
                score=score,
                rank=0,
            ))

        if built_only:
            matches = filter_built_parcels(matches, con)
    finally:
        con.close()

    logger.info(f"[strict_match] {len(matches)} parcelle(s) trouvée(s).")
    return matches
