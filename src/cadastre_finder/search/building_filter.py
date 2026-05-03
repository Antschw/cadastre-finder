"""Filtre les parcelles non bâties en vérifiant l'intersection avec la table buildings OSM."""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Union

import duckdb
from loguru import logger
from shapely.geometry import shape

from cadastre_finder.config import (
    DB_PATH, MAX_BUILT_RATIO, MIN_ANCHOR_BUILT_M2, MIN_COMPACTNESS,
)
from cadastre_finder.search.models import ComboMatch, ParcelMatch


def filter_built_parcels(
    matches: list[ParcelMatch],
    con: duckdb.DuckDBPyConnection,
    drop_unbuilt: bool = True,
) -> list[ParcelMatch]:
    """Calcule la surface bâtie (OSM) pour chaque parcelle.
    Si drop_unbuilt=True, retire les parcelles sans aucun bâti.
    """
    if not matches:
        return matches

    ids = [m.id_parcelle for m in matches]
    placeholders = ", ".join("?" * len(ids))

    try:
        # On calcule la surface bâtie totale pour chaque parcelle (intersection)
        # On se restreint aux osm_type = 'area' (polygones) pour avoir une surface réelle
        rows = con.execute(f"""
            SELECT p.id, SUM(ST_Area(ST_Transform(ST_Intersection(b.geometry, p.geometry), 'EPSG:4326', 'EPSG:2154'))) as built_area
            FROM parcelles p, buildings b
            WHERE p.id IN ({placeholders})
              AND b.osm_type = 'area'
              AND ST_Intersects(b.geometry, p.geometry)
            GROUP BY p.id
        """, ids).fetchall()
    except Exception as e:
        logger.warning(f"[building_filter] Impossible de calculer les surfaces bâties : {e}")
        return matches

    built_areas = {r[0]: r[1] for r in rows}
    
    for m in matches:
        m.built_area = built_areas.get(m.id_parcelle, 0.0)

    if not drop_unbuilt:
        return matches

    filtered = [m for m in matches if m.id_parcelle in built_areas]

    if not filtered and matches:
        logger.warning(
            f"[building_filter] Aucun bâtiment trouvé sur les {len(matches)} parcelles candidates. "
            "Filtre désactivé par sécurité."
        )
        # On marque built_area comme -1 pour signaler l'absence de résultats bâtis
        for m in matches:
            m.built_area = -1.0
        return matches

    removed = len(matches) - len(filtered)
    if removed:
        logger.info(f"[building_filter] {removed} parcelle(s) non bâtie(s) exclue(s).")

    return filtered


def get_built_area(parcel_id: str, con: duckdb.DuckDBPyConnection) -> float:
    """Calcule la surface bâtie (OSM) pour une parcelle donnée."""
    try:
        row = con.execute("""
            SELECT SUM(ST_Area(ST_Transform(ST_Intersection(b.geometry, p.geometry), 'EPSG:4326', 'EPSG:2154')))
            FROM parcelles p, buildings b
            WHERE p.id = ?
              AND b.osm_type = 'area'
              AND ST_Intersects(b.geometry, p.geometry)
        """, [parcel_id]).fetchone()
        return row[0] if row and row[0] is not None else 0.0
    except Exception as e:
        logger.debug(f"[building_filter] Erreur calcul built_area pour {parcel_id}: {e}")
        return 0.0


def filter_anchors(
    matches: list[ParcelMatch],
    con: duckdb.DuckDBPyConnection,
    min_area: float,
) -> list[ParcelMatch]:
    """Ne garde que les parcelles ayant une surface bâtie >= min_area."""
    # S'assure que built_area est peuplé
    built_matches = filter_built_parcels(matches, con)
    
    # Fallback si données bâtiments manquantes (signalé par built_area = -1)
    if built_matches and all(m.built_area == -1.0 for m in built_matches):
        logger.info("[building_filter] Données bâtiments manquantes : toutes les parcelles sont considérées comme ancres.")
        return built_matches

    anchors = [m for m in built_matches if m.built_area and m.built_area >= min_area]
    
    removed = len(built_matches) - len(anchors)
    if removed:
        logger.info(f"[building_filter] {removed} parcelle(s) bâties rejetées (sous le seuil ancre de {min_area}m²).")
    
    return anchors


def filter_built_combos(
    combos: list[ComboMatch],
    con: duckdb.DuckDBPyConnection,
) -> list[ComboMatch]:
    """Retourne uniquement les combos dont au moins une parcelle est bâtie."""
    if not combos:
        return combos

    all_ids = list({p.id_parcelle for combo in combos for p in combo.parts})
    placeholders = ", ".join("?" * len(all_ids))

    try:
        rows = con.execute(f"""
            SELECT DISTINCT p.id
            FROM parcelles p
            WHERE p.id IN ({placeholders})
              AND EXISTS (
                  SELECT 1 FROM buildings b
                  WHERE b.osm_type = 'area'
                    AND ST_Intersects(b.geometry, p.geometry)
              )
        """, all_ids).fetchall()
    except Exception as e:
        logger.warning(f"[building_filter] Impossible de filtrer les bâtiments (combos) : {e}")
        return combos

    built_ids = {r[0] for r in rows}
    filtered = [c for c in combos if any(p.id_parcelle in built_ids for p in c.parts)]

    if not filtered and combos:
        logger.warning(
            "[building_filter] Aucun combo bâti trouvé — données bâtiments manquantes "
            "pour cette zone. Filtre désactivé."
        )
        return combos

    removed = len(combos) - len(filtered)
    if removed:
        logger.info(f"[building_filter] {removed} combo(s) non bâti(s) exclu(s).")

    return filtered


# ---------------------------------------------------------------------------
# Filtres durs unifiés (orchestrateur)
# ---------------------------------------------------------------------------

def _polsby_popper_geojson(geojson_str: str) -> float:
    """Calcule l'indice Polsby-Popper depuis une chaîne GeoJSON."""
    try:
        if not geojson_str or geojson_str == "{}":
            return 0.0
        geom = shape(json.loads(geojson_str))
        if geom.is_empty:
            return 0.0
        peri = geom.length
        if peri == 0:
            return 0.0
        return min(1.0, 4 * math.pi * geom.area / (peri ** 2))
    except Exception:
        return 0.0


def _ensure_built_areas(
    results: list[Union[ParcelMatch, ComboMatch]],
    db_path: Path,
) -> None:
    """Calcule built_area pour toutes les parcelles qui n'en ont pas encore."""
    parcels_to_compute: list[ParcelMatch] = []
    for r in results:
        if isinstance(r, ParcelMatch):
            if r.built_area is None:
                parcels_to_compute.append(r)
        else:
            for p in r.parts:
                if p.built_area is None:
                    parcels_to_compute.append(p)

    if not parcels_to_compute:
        return

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        filter_built_parcels(parcels_to_compute, con, drop_unbuilt=False)
    finally:
        con.close()


def apply_hard_filters(
    results: list[Union[ParcelMatch, ComboMatch]],
    *,
    target_terrain: float,
    tolerance_pct: float,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Applique les filtres durs métier :
    - surface hors fenêtre cible ±tolerance
    - emprise bâtie cumulée < MIN_ANCHOR_BUILT_M2
    - ratio bâti > MAX_BUILT_RATIO
    - compacité Polsby-Popper < MIN_COMPACTNESS
    - combos sans aucune part bâtie

    `built_area = -1.0` est traité comme une donnée manquante (ne déclenche pas
    le rejet) — typique des zones où les bâtiments OSM sont absents.

    REMARQUE : Si le nombre de résultats est faible (< 10), on désactive les filtres
    de surface et de bâti pour éviter de masquer des correspondances potentielles
    (notamment en recherche pilotée par le DPE où la surface peut diverger).
    """
    if not results:
        return results

    _ensure_built_areas(results, db_path)

    # Si peu de résultats, on est très indulgent
    is_lenient = len(results) < 10
    if is_lenient:
        logger.info(f"[filter] Peu de résultats ({len(results)}), assouplissement des filtres.")

    delta = target_terrain * tolerance_pct / 100.0
    surf_lo, surf_hi = target_terrain - delta, target_terrain + delta

    kept: list[Union[ParcelMatch, ComboMatch]] = []
    for r in results:
        if isinstance(r, ParcelMatch):
            surf = r.contenance
            barea = r.built_area or 0.0
            barea_known = (r.built_area is not None and r.built_area >= 0.0)
            geojson = r.geometry_geojson
            any_built = barea > 0.0
        else:
            surf = r.total_contenance
            parts_known = [p for p in r.parts if p.built_area is not None and p.built_area >= 0.0]
            barea = sum(p.built_area or 0.0 for p in parts_known)
            barea_known = bool(parts_known)
            geojson = r.combined_geojson
            any_built = any((p.built_area or 0.0) > 0.0 for p in r.parts)

        if surf <= 0:
            continue

        # Filtre de surface (ignoré si indulgent, sauf si vraiment aberrant < 10m2)
        if not is_lenient:
            if not (surf_lo <= surf <= surf_hi):
                logger.debug(
                    f"[filter] rejet surface : {surf:.0f} m² hors [{surf_lo:.0f}, {surf_hi:.0f}]"
                )
                continue
        elif surf < 10:
            continue

        if barea_known and not is_lenient:
            if barea < MIN_ANCHOR_BUILT_M2:
                logger.debug(
                    f"[filter] rejet bâti insuffisant : {barea:.0f} m² < {MIN_ANCHOR_BUILT_M2}"
                )
                continue
            if barea / surf > MAX_BUILT_RATIO:
                logger.debug(
                    f"[filter] rejet ratio bâti : {barea/surf:.0%} > {MAX_BUILT_RATIO:.0%}"
                )
                continue

        if isinstance(r, ComboMatch) and not any_built and not is_lenient:
            logger.debug("[filter] rejet combo sans part bâtie.")
            continue

        if isinstance(r, ComboMatch):
            pp = r.compactness
        else:
            pp = _polsby_popper_geojson(geojson)
        
        # Le filtre de compacité reste actif car il élimine les artefacts géométriques, 
        # mais on est plus souple si indulgent
        threshold_pp = MIN_COMPACTNESS if not is_lenient else MIN_COMPACTNESS / 2
        if pp > 0.0 and pp < threshold_pp:
            logger.debug(f"[filter] rejet compacité : PP {pp:.2f} < {threshold_pp}")
            continue

        kept.append(r)

    removed = len(results) - len(kept)
    if removed:
        logger.info(f"[filter] {removed} résultat(s) écarté(s) par les filtres durs.")
    return kept
