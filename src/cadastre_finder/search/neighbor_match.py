"""Moteur de recherche — Étape 2 : élargissement aux communes voisines + tolérance.

Usage :
    matches = search_with_neighbors("Mortagne-au-Perche", surface_m2=4200)
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from cadastre_finder.config import DB_PATH, DEFAULT_TOLERANCE_PCT, DEFAULT_TOP_N, MIN_TERRAIN_M2
from cadastre_finder.processing.adjacency import get_neighbors
from cadastre_finder.search.building_filter import filter_built_parcels
from cadastre_finder.search.models import ParcelMatch
from cadastre_finder.utils.geocoding import resolve_commune

# Bonus de score selon le rang de la commune
SCORE_BONUS = {0: 10, 1: 3, 2: 1}


def search_with_neighbors(
    commune: str,
    surface_m2: float,
    postal_code: Optional[str] = None,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    include_rank2: bool = False,
    min_surface: float = MIN_TERRAIN_M2,
    top_n: int = DEFAULT_TOP_N,
    built_only: bool = True,
    db_path: Path = DB_PATH,
) -> list[ParcelMatch]:
    """Recherche élargie aux communes adjacentes avec tolérance de surface.

    Score :
    - +10 si parcelle dans la commune annoncée
    - +3  si voisine rang 1
    - +1  si voisine rang 2
    - Score de surface : 100 - écart_relatif * 100

    Args:
        commune:       Nom de la commune annoncée
        surface_m2:    Surface en m² annoncée
        postal_code:   Code postal optionnel
        tolerance_pct: Tolérance surface en % (défaut 5%)
        include_rank2: Inclure les voisines de 2e rang
        min_surface:   Surface minimale du terrain
        top_n:         Nombre maximum de résultats
        db_path:       Chemin DuckDB

    Returns:
        Liste de ParcelMatch triée par score décroissant puis écart surface croissant.
    """
    result = resolve_commune(commune, postal_code, db_path)
    if not result.candidates:
        logger.warning(f"[neighbor_match] Commune introuvable : '{commune}'")
        return []

    best = result.best
    code_insee_main = best.code_insee
    nom_commune_main = best.nom

    max_rang = 2 if include_rank2 else 1
    neighbors = get_neighbors(code_insee_main, rang=max_rang, db_path=db_path)

    # Mapping code_insee -> (rang, nom)
    commune_ranks: dict[str, int] = {code_insee_main: 0}
    for neighbor_insee in neighbors:
        # On ne sait pas le rang exact depuis get_neighbors, on le calcule
        commune_ranks[neighbor_insee] = commune_ranks.get(neighbor_insee, max_rang)

    # Récupérer les rangs précis
    con_ranks = duckdb.connect(str(db_path), read_only=True)
    try:
        rows_ranks = con_ranks.execute(
            "SELECT code_insee_b, MIN(rang) FROM communes_adjacency "
            "WHERE code_insee_a = ? AND rang <= ? "
            "GROUP BY code_insee_b",
            [code_insee_main, max_rang],
        ).fetchall()
        for insee_b, rang in rows_ranks:
            commune_ranks[insee_b] = rang

        # Récupérer les noms des communes
        all_codes = list(commune_ranks.keys())
        placeholders = ", ".join("?" * len(all_codes))
        nom_rows = con_ranks.execute(
            f"SELECT code_insee, nom FROM communes WHERE code_insee IN ({placeholders})",
            all_codes,
        ).fetchall()
        commune_noms = {r[0]: r[1] for r in nom_rows}
        commune_noms[code_insee_main] = nom_commune_main
    finally:
        con_ranks.close()

    all_codes = list(commune_ranks.keys())
    delta = surface_m2 * tolerance_pct / 100.0
    surface_min = surface_m2 - delta
    surface_max = surface_m2 + delta

    logger.info(
        f"[neighbor_match] Recherche sur {len(all_codes)} commune(s) "
        f"(rang max={max_rang}), surface [{surface_min:.0f}, {surface_max:.0f}] m²"
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        placeholders = ", ".join("?" * len(all_codes))
        rows = con.execute(f"""
            SELECT
                id,
                code_insee,
                contenance,
                ST_X(ST_Centroid(geometry)) AS lon,
                ST_Y(ST_Centroid(geometry)) AS lat,
                ST_AsGeoJSON(geometry)      AS geojson
            FROM parcelles
            WHERE code_insee IN ({placeholders})
              AND contenance >= ?
              AND contenance BETWEEN ? AND ?
            ORDER BY ABS(contenance - ?) ASC
        """, [*all_codes, min_surface, surface_min, surface_max, surface_m2]).fetchall()

        matches = []
        for id_parc, insee, contenance, lon, lat, geojson in rows:
            rang = commune_ranks.get(insee, max_rang)
            bonus = SCORE_BONUS.get(rang, 0)
            ecart_rel = abs(contenance - surface_m2) / surface_m2
            surface_score = max(0.0, 100.0 - ecart_rel * 100.0)
            score = surface_score + bonus
            nom = commune_noms.get(insee, insee)

            matches.append(ParcelMatch(
                id_parcelle=id_parc,
                code_insee=insee,
                nom_commune=nom,
                contenance=contenance,
                centroid_lat=lat,
                centroid_lon=lon,
                geometry_geojson=geojson or "{}",
                score=score,
                rank=rang,
            ))

        if built_only:
            matches = filter_built_parcels(matches, con)
    finally:
        con.close()

    # Tri : score décroissant, puis écart surface croissant
    matches.sort(key=lambda m: (-m.score, abs(m.contenance - surface_m2)))
    result_list = matches[:top_n]

    logger.info(f"[neighbor_match] {len(result_list)} parcelle(s) retournée(s) (top {top_n}).")
    return result_list
