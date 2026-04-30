"""Recherche de combinaisons de parcelles adjacentes dont la somme correspond à la cible.

Usage :
    combos = search_combos("Neuvy-le-Roi", surface_m2=5415)
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger
from shapely.geometry import shape
from shapely.ops import unary_union

from cadastre_finder.config import DB_PATH, DEFAULT_TOLERANCE_PCT, DEFAULT_TOP_N, MIN_TERRAIN_M2
from cadastre_finder.processing.adjacency import get_neighbors
from cadastre_finder.search.building_filter import filter_built_combos
from cadastre_finder.search.models import ComboMatch, ParcelMatch
from cadastre_finder.utils.geocoding import resolve_commune

MIN_PART_M2 = 10        # Seuil absolu minimum (parcelles accessoires très petites)
MAX_PARTS = 6           # Couvre les cas de maisons découpées en 6 parcelles
MAX_DFS_NODES = 500_000 # Plafond de nœuds explorés pour éviter les explosions combinatoires


# ---------------------------------------------------------------------------
# Récupération des candidats
# ---------------------------------------------------------------------------

def _fetch_candidates(
    con: duckdb.DuckDBPyConnection,
    codes_insee: list[str],
    min_contenance: float,
    max_contenance: float,
    commune_noms: dict[str, str],
) -> list[ParcelMatch]:
    placeholders = ", ".join("?" * len(codes_insee))
    rows = con.execute(f"""
        SELECT
            id, code_insee, contenance,
            ST_X(ST_Centroid(geometry)) AS lon,
            ST_Y(ST_Centroid(geometry)) AS lat,
            ST_AsGeoJSON(geometry)      AS geojson
        FROM parcelles
        WHERE code_insee IN ({placeholders})
          AND contenance >= ?
          AND contenance <= ?
        ORDER BY contenance DESC
    """, [*codes_insee, min_contenance, max_contenance]).fetchall()

    return [
        ParcelMatch(
            id_parcelle=row[0],
            code_insee=row[1],
            nom_commune=commune_noms.get(row[1], row[1]),
            contenance=row[2],
            centroid_lon=row[3],
            centroid_lat=row[4],
            geometry_geojson=row[5] or "{}",
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Graphe d'adjacence : pré-calculé (rapide) ou spatial à la volée (fallback)
# ---------------------------------------------------------------------------

def _load_adjacency_precomputed(
    con: duckdb.DuckDBPyConnection,
    candidate_ids: list[str],
) -> dict[str, set[str]] | None:
    """Tente de charger l'adjacence depuis la table pré-calculée. Retourne None si absente."""
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'parcelles_adjacency'"
        ).fetchone()[0]
        if not n:
            return None

        id_set = set(candidate_ids)
        placeholders = ", ".join("?" * len(candidate_ids))
        rows = con.execute(f"""
            SELECT id_a, id_b FROM parcelles_adjacency
            WHERE id_a IN ({placeholders}) OR id_b IN ({placeholders})
        """, candidate_ids + candidate_ids).fetchall()

        graph: dict[str, set[str]] = {}
        for id_a, id_b in rows:
            if id_a in id_set and id_b in id_set:
                graph.setdefault(id_a, set()).add(id_b)
                graph.setdefault(id_b, set()).add(id_a)
        return graph
    except Exception:
        return None


def _load_adjacency_spatial(
    con: duckdb.DuckDBPyConnection,
    codes_insee: list[str],
    max_contenance: float,
) -> dict[str, set[str]]:
    """Calcule l'adjacence à la volée par jointure spatiale (plus lent)."""
    placeholders = ", ".join("?" * len(codes_insee))
    rows = con.execute(f"""
        SELECT DISTINCT a.id, b.id
        FROM parcelles a
        JOIN parcelles b ON (
            a.id < b.id
            AND a.code_insee = b.code_insee
            AND ST_Intersects(a.geometry, b.geometry)
            AND NOT ST_Equals(a.geometry, b.geometry)
        )
        WHERE a.code_insee IN ({placeholders})
          AND a.contenance >= ? AND a.contenance <= ?
          AND b.contenance >= ? AND b.contenance <= ?
    """, [*codes_insee, MIN_PART_M2, max_contenance, MIN_PART_M2, max_contenance]).fetchall()

    graph: dict[str, set[str]] = {}
    for id_a, id_b in rows:
        graph.setdefault(id_a, set()).add(id_b)
        graph.setdefault(id_b, set()).add(id_a)
    return graph


def _get_adjacency(
    con: duckdb.DuckDBPyConnection,
    candidates: list[ParcelMatch],
    codes_insee: list[str],
    max_contenance: float,
) -> dict[str, set[str]]:
    candidate_ids = [p.id_parcelle for p in candidates]
    graph = _load_adjacency_precomputed(con, candidate_ids)
    if graph is None:
        logger.info("[combo_match] Table pré-calculée absente → jointure spatiale (plus lent).")
        return _load_adjacency_spatial(con, codes_insee, max_contenance)
    if not graph:
        # Table présente mais aucune paire pour ces candidats → données non encore calculées
        logger.info("[combo_match] Table pré-calculée vide pour ces communes → jointure spatiale.")
        return _load_adjacency_spatial(con, codes_insee, max_contenance)
    logger.debug("[combo_match] Adjacence chargée depuis la table pré-calculée.")
    return graph


# ---------------------------------------------------------------------------
# Construction du ComboMatch
# ---------------------------------------------------------------------------

_RANK_BONUS = {0: 10, 1: 3, 2: 1}


def _polsby_popper(geom) -> float:
    """Indice de compacité Polsby-Popper [0, 1].

    1 = cercle parfait, ~0.78 = carré.
    Pénalise les bandes étroites (~0.05) et les formes fragmentées/irrégulières.
    Fonctionne en coordonnées WGS84 car le ratio surface/périmètre² est sans dimension.
    """
    try:
        if geom is None or geom.is_empty:
            return 0.0
        perimeter = geom.length
        if perimeter == 0:
            return 0.0
        return min(1.0, 4 * math.pi * geom.area / (perimeter ** 2))
    except Exception:
        return 0.0


def _build_combo(parts: list[ParcelMatch], target_m2: float) -> ComboMatch:
    total = sum(p.contenance for p in parts)
    rank = min(p.rank for p in parts)

    geoms = []
    for p in parts:
        try:
            geoms.append(shape(json.loads(p.geometry_geojson)))
        except Exception:
            pass

    combined_geom = None
    if geoms:
        combined_geom = unary_union(geoms)
        combined_geojson = json.dumps(combined_geom.__geo_interface__)
        centroid = combined_geom.centroid
        clat, clon = centroid.y, centroid.x
    else:
        clat, clon = parts[0].centroid_lat, parts[0].centroid_lon
        combined_geojson = "{}"

    # --- Score ---
    # Surface : pénalité proportionnelle à l'écart relatif
    ecart_rel = abs(total - target_m2) / target_m2
    surface_score = max(0.0, 100.0 - ecart_rel * 100.0)

    # Rang : bonus identique aux parcelles uniques (commune annoncée = +10)
    rank_bonus = _RANK_BONUS.get(rank, 0)

    # Nombre de parcelles : légère pénalité (regrouper 6 parcelles est moins fiable)
    part_penalty = (len(parts) - 1) * 0.5

    # Forme : compacité Polsby-Popper (0→1), contribue jusqu'à +8 points
    # Un agrégat compact (PP≈0.6) vaut mieux qu'une bande le long d'une route (PP≈0.05)
    pp = _polsby_popper(combined_geom)
    shape_bonus = pp * 8

    score = surface_score + rank_bonus - part_penalty + shape_bonus

    return ComboMatch(
        parts=parts,
        total_contenance=total,
        centroid_lat=clat,
        centroid_lon=clon,
        combined_geojson=combined_geojson,
        score=score,
        rank=rank,
        compactness=pp,
    )


# ---------------------------------------------------------------------------
# DFS générique — trouve toutes les combinaisons connexes de 2 à N parcelles
# ---------------------------------------------------------------------------

def _find_combos_dfs(
    candidates: list[ParcelMatch],
    graph: dict[str, set[str]],
    target_m2: float,
    tolerance_pct: float,
    max_parts: int,
    top_n: int,
) -> list[ComboMatch]:
    """DFS sur le graphe d'adjacence pour trouver les sous-ensembles connexes dans la plage."""
    if not candidates:
        return []

    idx_by_id = {p.id_parcelle: i for i, p in enumerate(candidates)}
    delta = target_m2 * tolerance_pct / 100.0
    lo, hi = target_m2 - delta, target_m2 + delta

    # Voisins pré-indexés par indice candidat pour éviter les lookups répétés
    neighbors_by_idx: list[list[int]] = []
    for p in candidates:
        nbrs = sorted(
            idx_by_id[n] for n in graph.get(p.id_parcelle, set()) if n in idx_by_id
        )
        neighbors_by_idx.append(nbrs)

    results: list[ComboMatch] = []
    nodes_visited = [0]  # liste pour modification dans closure

    def dfs(combo: list[int], total: int, reachable: list[int]) -> None:
        nodes_visited[0] += 1
        if nodes_visited[0] > MAX_DFS_NODES:
            return

        if lo <= total <= hi and len(combo) >= 2:
            parts = [candidates[i] for i in combo]
            results.append(_build_combo(parts, target_m2))

        if total >= hi or len(combo) >= max_parts:
            return

        max_idx = combo[-1]

        for idx in reachable:
            if idx <= max_idx:
                continue
            p = candidates[idx]
            new_total = total + p.contenance
            if new_total > hi:
                continue

            # Étendre la frontière : voisins de idx pas encore dans combo
            combo_set = set(combo)
            new_reachable = sorted(
                set(reachable) | set(neighbors_by_idx[idx]) - combo_set - {idx}
            )
            dfs(combo + [idx], new_total, new_reachable)

    for i, p in enumerate(candidates):
        if nodes_visited[0] > MAX_DFS_NODES:
            logger.warning(
                f"[combo_match] Plafond DFS atteint ({MAX_DFS_NODES:,} nœuds). "
                "Résultats partiels. Réduisez la tolérance ou installez la table d'adjacence pré-calculée."
            )
            break
        dfs([i], p.contenance, neighbors_by_idx[i])

    results.sort(key=lambda c: (-c.score, abs(c.total_contenance - target_m2)))
    return results[:top_n]


# ---------------------------------------------------------------------------
# Déduplication : parmi les combos qui partagent des parcelles, garder le plus proche
# ---------------------------------------------------------------------------

def _deduplicate_combos(combos: list[ComboMatch], target_m2: float) -> list[ComboMatch]:
    """Élimine les combos redondants : si un combo A est un sous-ensemble ou sur-ensemble
    d'un combo B déjà retenu et plus proche de la cible, A est éliminé.

    Deux combos qui partagent des parcelles sans relation d'inclusion sont conservés tous
    les deux — ils peuvent représenter des propriétés différentes sur un même terrain.
    """
    sorted_combos = sorted(combos, key=lambda c: abs(c.total_contenance - target_m2))
    kept: list[ComboMatch] = []
    kept_sets: list[frozenset[str]] = []
    for combo in sorted_combos:
        ids = frozenset(p.id_parcelle for p in combo.parts)
        if not any(ids.issubset(ks) or ids.issuperset(ks) for ks in kept_sets):
            kept.append(combo)
            kept_sets.append(ids)
    return kept


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def search_combos(
    commune: str,
    surface_m2: float,
    postal_code: Optional[str] = None,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    include_rank2: bool = False,
    max_parts: int = MAX_PARTS,
    top_n: int = DEFAULT_TOP_N,
    built_only: bool = True,
    db_path: Path = DB_PATH,
) -> list[ComboMatch]:
    """Recherche des combinaisons connexes de 2 à max_parts parcelles adjacentes.

    Utilise la table `parcelles_adjacency` si disponible (rapide),
    sinon calcule l'adjacence à la volée par jointure spatiale.
    """
    result = resolve_commune(commune, postal_code, db_path)
    if not result.candidates:
        logger.warning(f"[combo_match] Commune introuvable : '{commune}'")
        return []

    best = result.best
    code_insee_main = best.code_insee
    nom_main = best.nom

    max_rang = 2 if include_rank2 else 1

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")

        neighbors_rows = con.execute(
            "SELECT code_insee_b, MIN(rang) FROM communes_adjacency "
            "WHERE code_insee_a = ? AND rang <= ? GROUP BY code_insee_b",
            [code_insee_main, max_rang],
        ).fetchall()

        all_codes = [code_insee_main] + [r[0] for r in neighbors_rows]

        nom_rows = con.execute(
            f"SELECT code_insee, nom FROM communes WHERE code_insee IN "
            f"({', '.join('?' * len(all_codes))})",
            all_codes,
        ).fetchall()
        commune_noms = {r[0]: r[1] for r in nom_rows}
        commune_noms[code_insee_main] = nom_main

        # Borne haute : chaque parcelle doit laisser de la place aux autres
        max_single = surface_m2 * (1 + tolerance_pct / 100.0) - (max_parts - 1) * MIN_PART_M2

        # Borne basse : seuil pour éliminer les parcelles insignifiantes (voirie, etc.)
        # On utilise un facteur large pour ne pas exclure de vraies parcelles de jardin/garage.
        min_single = max(MIN_PART_M2, int(surface_m2 / 500))

        logger.info(
            f"[combo_match] Recherche combos sur {len(all_codes)} commune(s), "
            f"cible {surface_m2:.0f} m² ±{tolerance_pct}%, max {max_parts} parcelles, "
            f"candidats [{min_single}–{max_single:.0f}] m²"
        )

        candidates = _fetch_candidates(con, all_codes, min_single, max_single, commune_noms)
        logger.info(f"[combo_match] {len(candidates)} parcelles candidates")

        if not candidates:
            return []

        graph = _get_adjacency(con, candidates, all_codes, max_single)
        logger.info(
            f"[combo_match] {sum(len(v) for v in graph.values()) // 2} paires adjacentes"
        )

        combos = _find_combos_dfs(candidates, graph, surface_m2, tolerance_pct, max_parts, top_n * 3)
        combos = _deduplicate_combos(combos, surface_m2)

        if built_only:
            combos = filter_built_combos(combos, con)

    finally:
        con.close()

    combos = combos[:top_n]
    logger.info(f"[combo_match] {len(combos)} combo(s) trouvé(s).")
    return combos
