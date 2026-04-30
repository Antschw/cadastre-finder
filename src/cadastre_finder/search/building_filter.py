"""Filtre les parcelles non bâties en vérifiant l'intersection avec la table buildings OSM."""
from __future__ import annotations

import duckdb
from loguru import logger

from cadastre_finder.search.models import ComboMatch, ParcelMatch


def filter_built_parcels(
    matches: list[ParcelMatch],
    con: duckdb.DuckDBPyConnection,
) -> list[ParcelMatch]:
    """Retourne uniquement les parcelles ayant au moins un bâtiment OSM sur leur emprise."""
    if not matches:
        return matches

    ids = [m.id_parcelle for m in matches]
    placeholders = ", ".join("?" * len(ids))

    try:
        rows = con.execute(f"""
            SELECT DISTINCT p.id
            FROM parcelles p
            WHERE p.id IN ({placeholders})
              AND EXISTS (
                  SELECT 1 FROM buildings b
                  WHERE ST_Intersects(b.geometry, p.geometry)
              )
        """, ids).fetchall()
    except Exception as e:
        logger.warning(f"[building_filter] Impossible de filtrer les bâtiments : {e}")
        return matches

    built_ids = {r[0] for r in rows}
    filtered = [m for m in matches if m.id_parcelle in built_ids]

    if not filtered and matches:
        logger.warning(
            "[building_filter] Aucune parcelle bâtie trouvée — données bâtiments manquantes "
            "pour cette zone. Filtre désactivé."
        )
        return matches

    removed = len(matches) - len(filtered)
    if removed:
        logger.info(f"[building_filter] {removed} parcelle(s) non bâtie(s) exclue(s).")

    return filtered


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
