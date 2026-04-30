"""Filtre les parcelles non bâties en vérifiant l'intersection avec la table buildings OSM."""
from __future__ import annotations

import duckdb
from loguru import logger

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
        rows = con.execute(f"""
            SELECT p.id, SUM(ST_Area(ST_Transform(ST_Intersection(b.geometry, p.geometry), 'EPSG:4326', 'EPSG:2154'))) as built_area
            FROM parcelles p, buildings b
            WHERE p.id IN ({placeholders})
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
            "[building_filter] Aucune parcelle bâtie trouvée — données bâtiments manquantes "
            "pour cette zone. Filtre désactivé."
        )
        # On marque built_area comme -1 pour signaler l'absence de données
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
                  WHERE ST_Intersects(b.geometry, p.geometry)
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
