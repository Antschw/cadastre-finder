"""Moteur de recherche — Croisement avec les données DPE de l'ADEME.

Quand l'utilisateur fournit un DPE (et/ou GES), c'est l'ADEME qui sert de point
d'entrée : on récupère jusqu'à 20 enregistrements correspondants dans le périmètre
INSEE, on les géo-code via la Géoplateforme et on tente de localiser la parcelle
(ou un agrégat de parcelles voisines) qui correspond à la surface cible.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Optional, Union

import duckdb
from loguru import logger
from shapely.geometry import shape
from shapely.ops import unary_union

from pyproj import Transformer

from cadastre_finder.config import DB_PATH, DPE_TABLE
from cadastre_finder.search.models import ComboMatch, DPEPositionMatch, ParcelMatch
from cadastre_finder.utils.geocoding import geocode_address, reverse_geocode_parcel

_lambert93_to_wgs84 = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


_MAX_AGGREGATE_PARTS = 6
_MAX_BFS_NODES = 5000

_HAS_ADJ_TABLE_CACHE: Optional[bool] = None


# ---------------------------------------------------------------------------
# Recherche d'enregistrements DPE
# ---------------------------------------------------------------------------

def search_dpe(
    insee_codes: list[str],
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    tolerance_pct: float = 5.0,
    limit: int = 20,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """Recherche les enregistrements DPE de l'ADEME dans le périmètre INSEE.

    Si `living_surface` est fourni, filtre par surface habitable ±tolerance_pct.
    Sinon, retourne les enregistrements correspondant à la lettre DPE/GES dans
    le périmètre, sans contrainte de surface.
    """
    if not insee_codes:
        return []

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        table_exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{DPE_TABLE}'"
        ).fetchone()[0]
        if not table_exists:
            logger.warning(f"Table {DPE_TABLE} absente. Lancez l'ingestion DPE.")
            return []

        all_rows = []
        # On tente plusieurs niveaux de relâchement
        for attempt in range(4):
            placeholders = ", ".join("?" * len(insee_codes))
            query = f"""
                SELECT
                    adresse_brut, code_postal_brut, nom_commune_brut, code_insee_ban,
                    surface_habitable_logement, etiquette_dpe, etiquette_ges,
                    date_etablissement_dpe
                FROM {DPE_TABLE}
                WHERE code_insee_ban IN ({placeholders})
            """
            params: list = [*insee_codes]

            # Garantir une tolérance minimale positive pour les fallbacks
            base_tol = max(tolerance_pct, 1.0)
            current_tolerance = tolerance_pct
            if attempt == 1:
                current_tolerance = max(tolerance_pct, base_tol * 3)  # min 3%
            elif attempt >= 2:
                current_tolerance = max(tolerance_pct, base_tol * 5)  # min 5%

            if living_surface:
                delta = living_surface * current_tolerance / 100.0
                query += " AND surface_habitable_logement BETWEEN ? AND ?"
                params.extend([living_surface - delta, living_surface + delta])

            # Au 4ème essai, on ignore aussi le DPE si rien n'est trouvé
            if dpe_label and attempt < 3:
                query += " AND etiquette_dpe = ?"
                params.append(dpe_label)

            # Au 3ème essai, on ignore le GES s'il bloque tout
            if ges_label and attempt < 2:
                query += " AND etiquette_ges = ?"
                params.append(ges_label)

            query += " ORDER BY date_etablissement_dpe DESC LIMIT ?"
            params.append(limit)

            rows = con.execute(query, params).fetchall()
            if rows:
                if attempt > 0:
                    logger.debug(f"[dpe_match] Résultats trouvés après relâchement (essai {attempt+1}).")
                
                # Ajouter les nouveaux uniquement pour éviter les doublons
                new_added = 0
                for r in rows:
                    if r not in all_rows:
                        all_rows.append(r)
                        new_added += 1
                
                # Si on a trouvé des nouveaux résultats, on vérifie si on en a assez
                # On veut au moins 5 records pour avoir un choix si possible,
                # sauf si on est déjà au stade le plus relâché.
                if len(all_rows) >= 5 or (attempt >= 1 and new_added == 0):
                    break
        
        if not all_rows:
            return []

        return [
            {
                "address": r[0],
                "postcode": r[1],
                "city": r[2],
                "code_insee": r[3],
                "surface": r[4],
                "dpe": r[5],
                "ges": r[6],
                "date": r[7],
            }
            for r in all_rows
        ]
    except Exception as e:
        logger.error(f"[dpe_match] Erreur lors de la recherche DPE : {e}")
        return []
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Localisation parcelle pour un enregistrement DPE
# ---------------------------------------------------------------------------

def _fetch_best_anchor(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    dpe_record: dict,
) -> Optional[ParcelMatch]:
    """Cherche la meilleure parcelle ancre aux coordonnées données.

    1. Cherche les parcelles dans un rayon de 25m.
    2. Si une parcelle intersecte le point et fait > 50m2, on la prend.
    3. Sinon, on prend la parcelle de > 50m2 la plus proche (< 15m).
    4. En dernier recours, utilise l'API reverse geocoding de la Géoplateforme.
    """
    insee = dpe_record.get("code_insee")
    # Buffer de ~40m pour le filtrage spatial initial (0.0004 deg)
    rows = con.execute("""
        SELECT id, code_insee, contenance,
               ST_X(ST_Centroid(geometry)) AS lon,
               ST_Y(ST_Centroid(geometry)) AS lat,
               ST_AsGeoJSON(geometry)      AS geojson,
               ST_Distance(
                   ST_Transform(ST_FlipCoordinates(ST_Point(?, ?)), 'EPSG:4326', 'EPSG:2154'),
                   ST_Transform(ST_FlipCoordinates(geometry), 'EPSG:4326', 'EPSG:2154')
               ) as dist
        FROM parcelles
        WHERE ST_Intersects(geometry, ST_Buffer(ST_Point(?, ?), 0.0004))
          AND (code_insee = ? OR ? IS NULL)
        ORDER BY dist
        LIMIT 10
    """, [lon, lat, lon, lat, insee, insee]).fetchall()

    exact_match = None
    if rows:
        for r in rows:
            if r[6] <= 0.00001:  # Quasi-intersection
                exact_match = r
                break
        
        # Si intersection sur une parcelle non-minuscule
        if exact_match and exact_match[2] >= 50:
            return _row_to_parcel_match(con, exact_match, dpe_record)

        # Chercher une parcelle raisonnable (> 50m2) très proche (< 15m)
        for r in rows:
            if r[2] >= 50 and r[6] < 15:
                return _row_to_parcel_match(con, r, dpe_record)

    # Fallback API
    logger.debug(f"[dpe_match] Pas d'ancre locale idéale pour ({lat}, {lon}), appel API...")
    parcel_ids = reverse_geocode_parcel(lat, lon)
    for parcel_id in parcel_ids:
        anchor = _fetch_parcel_by_id(con, parcel_id, dpe_record)
        if anchor:
            return anchor

    # Dernier recours : l'exact match même si petit, ou la plus proche
    if exact_match:
        return _row_to_parcel_match(con, exact_match, dpe_record)
    if rows:
        return _row_to_parcel_match(con, rows[0], dpe_record)

    return None


def _row_to_parcel_match(con: duckdb.DuckDBPyConnection, row: tuple, dpe_record: dict) -> ParcelMatch:
    nom_row = con.execute(
        "SELECT nom FROM communes WHERE code_insee = ?", [row[1]]
    ).fetchone()
    nom_commune = nom_row[0] if nom_row else row[1]

    return ParcelMatch(
        id_parcelle=row[0],
        code_insee=row[1],
        nom_commune=nom_commune,
        contenance=row[2],
        centroid_lon=row[3],
        centroid_lat=row[4],
        geometry_geojson=row[5] or "{}",
        score=0.0,
        dpe_label=dpe_record.get("dpe"),
        ges_label=dpe_record.get("ges"),
    )


def _fetch_parcel_by_id(
    con: duckdb.DuckDBPyConnection,
    parcel_id: str,
    dpe_record: dict,
) -> Optional[ParcelMatch]:
    row = con.execute(
        """
        SELECT id, code_insee, contenance,
               ST_X(ST_Centroid(geometry)) AS lon,
               ST_Y(ST_Centroid(geometry)) AS lat,
               ST_AsGeoJSON(geometry)      AS geojson
        FROM parcelles
        WHERE id = ?
        """,
        [parcel_id],
    ).fetchone()
    if not row:
        return None

    nom_row = con.execute(
        "SELECT nom FROM communes WHERE code_insee = ?", [row[1]]
    ).fetchone()
    nom_commune = nom_row[0] if nom_row else row[1]

    return ParcelMatch(
        id_parcelle=row[0],
        code_insee=row[1],
        nom_commune=nom_commune,
        contenance=row[2],
        centroid_lon=row[3],
        centroid_lat=row[4],
        geometry_geojson=row[5] or "{}",
        score=0.0,
        dpe_label=dpe_record.get("dpe"),
        ges_label=dpe_record.get("ges"),
    )


def _fetch_parcel_at_point(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    dpe_record: dict,
) -> Optional[ParcelMatch]:
    row = con.execute(
        """
        SELECT id, code_insee, contenance,
               ST_X(ST_Centroid(geometry)) AS lon,
               ST_Y(ST_Centroid(geometry)) AS lat,
               ST_AsGeoJSON(geometry)      AS geojson
        FROM parcelles
        WHERE ST_Intersects(geometry, ST_Point(?, ?))
        LIMIT 1
        """,
        [lon, lat],
    ).fetchone()
    if not row:
        return None

    nom_row = con.execute(
        "SELECT nom FROM communes WHERE code_insee = ?", [row[1]]
    ).fetchone()
    nom_commune = nom_row[0] if nom_row else row[1]

    return ParcelMatch(
        id_parcelle=row[0],
        code_insee=row[1],
        nom_commune=nom_commune,
        contenance=row[2],
        centroid_lon=row[3],
        centroid_lat=row[4],
        geometry_geojson=row[5] or "{}",
        score=0.0,
        dpe_label=dpe_record.get("dpe"),
        ges_label=dpe_record.get("ges"),
    )


def _fetch_local_pool(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    insee: Optional[str],
    radius_m: float = 100.0,
) -> list[tuple]:
    """Récupère toutes les parcelles dans un rayon donné autour d'un point (en mètres).

    Utilise ST_Buffer en degrés (~0.001° ≈ 100m en France) pour le filtre spatial,
    puis calcule la distance réelle en Lambert93 pour le tri et le seuillage.
    """
    approx_deg = radius_m / 111_000.0
    rows = con.execute("""
        SELECT id, code_insee, contenance,
               ST_X(ST_Centroid(geometry)) AS clon,
               ST_Y(ST_Centroid(geometry)) AS clat,
               ST_AsGeoJSON(geometry) AS geojson,
               ST_Distance(
                   ST_Transform(ST_FlipCoordinates(ST_Point(?, ?)), 'EPSG:4326', 'EPSG:2154'),
                   ST_Transform(ST_FlipCoordinates(geometry), 'EPSG:4326', 'EPSG:2154')
               ) AS dist_m
        FROM parcelles
        WHERE ST_Intersects(geometry, ST_Buffer(ST_Point(?, ?), ?))
          AND (code_insee = ? OR ? IS NULL)
        ORDER BY dist_m
    """, [lon, lat, lon, lat, approx_deg, insee, insee]).fetchall()
    return [r for r in rows if r[6] <= radius_m]


def _is_connected(geoms: list) -> bool:
    """Vérifie si une liste de géométries Shapely forme un seul bloc contigu (BFS)."""
    if not geoms:
        return False
    if len(geoms) == 1:
        return True

    n = len(geoms)
    adj: dict[int, set[int]] = {i: set() for i in range(n)}
    for i in range(n):
        for j in range(i + 1, n):
            if geoms[i].intersects(geoms[j]):
                adj[i].add(j)
                adj[j].add(i)

    visited = {0}
    queue = list(adj[0])
    while queue:
        curr = queue.pop(0)
        if curr not in visited:
            visited.add(curr)
            queue.extend(adj[curr] - visited)
    return len(visited) == n


def _find_micro_combos_in_pool(
    con: duckdb.DuckDBPyConnection,
    pool: list[tuple],
    target_terrain: float,
    tolerance_pct: float,
    dpe_record: dict,
    max_parts: int = 4,
) -> list[ComboMatch]:
    """Cherche des combinaisons contiguës de parcelles dans le pool local.

    Pré-parse les géométries une seule fois, filtre par surface puis par
    contiguïté spatiale (Shapely) avant de construire le ComboMatch.
    Parcourt tous les k sans s'arrêter au premier k fructueux : une combinaison
    de plus petites parcelles (k=3+) peut être plus précise qu'une paire à k=2.
    Priorité aux combos contenant la parcelle la plus proche du point géocodé (pool[0]).
    """
    from itertools import combinations as _combinations

    delta = target_terrain * tolerance_pct / 100.0
    lo, hi = target_terrain - delta, target_terrain + delta
    found: list[ComboMatch] = []
    # ID de la parcelle la plus proche du point DPE géocodé (pool trié par dist_m)
    nearest_id = pool[0][0] if pool else None

    # Pré-parser les géométries Shapely une seule fois (r[5] = GeoJSON)
    parsed_geoms: list[Optional[object]] = []
    for r in pool:
        try:
            parsed_geoms.append(shape(json.loads(r[5])))
        except Exception:
            parsed_geoms.append(None)

    for k in range(2, min(max_parts + 1, len(pool) + 1)):
        for idx_combo in _combinations(range(len(pool)), k):
            total = sum(pool[i][2] for i in idx_combo)
            if not (lo <= total <= hi):
                continue
            geoms = [parsed_geoms[i] for i in idx_combo if parsed_geoms[i] is not None]
            if len(geoms) != k or not _is_connected(geoms):
                continue
            ids = [pool[i][0] for i in idx_combo]
            full = _fetch_parcels_bulk(con, ids)
            parts = [full[pid] for pid in ids if pid in full]
            if parts:
                found.append(_build_combo_from_parts(parts, dpe_record))

    if not found:
        return []

    # Priorité aux combos contenant la parcelle la plus proche du point DPE géocodé,
    # puis par précision de surface (delta absolu)
    found.sort(key=lambda c: (
        0 if nearest_id and any(p.id_parcelle == nearest_id for p in c.parts) else 1,
        abs(c.total_contenance - target_terrain),
    ))
    return found[:5]


def _fetch_neighbor_ids(con: duckdb.DuckDBPyConnection, parcel_id: str) -> list[str]:
    """Récupère les voisins immédiats d'une parcelle via parcelles_adjacency.

    Fallback : jointure spatiale ST_Intersects si la table pré-calculée est absente.
    """
    global _HAS_ADJ_TABLE_CACHE
    if _HAS_ADJ_TABLE_CACHE is None:
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'parcelles_adjacency'"
            ).fetchone()[0]
            _HAS_ADJ_TABLE_CACHE = bool(n)
        except Exception:
            _HAS_ADJ_TABLE_CACHE = False

    if _HAS_ADJ_TABLE_CACHE:
        rows = con.execute(
            "SELECT id_b FROM parcelles_adjacency WHERE id_a = ? "
            "UNION SELECT id_a FROM parcelles_adjacency WHERE id_b = ?",
            [parcel_id, parcel_id],
        ).fetchall()
        return [r[0] for r in rows]

    # Fallback spatial à la volée
    rows = con.execute(
        """
        SELECT b.id
        FROM parcelles a
        JOIN parcelles b ON
            a.id <> b.id
            AND a.code_insee = b.code_insee
            AND ST_Intersects(a.geometry, b.geometry)
            AND NOT ST_Equals(a.geometry, b.geometry)
        WHERE a.id = ?
        """,
        [parcel_id],
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_parcels_minimal(
    con: duckdb.DuckDBPyConnection,
    ids: list[str],
) -> dict[str, ParcelMatch]:
    if not ids:
        return {}
    placeholders = ", ".join("?" * len(ids))
    rows = con.execute(
        f"SELECT id, code_insee, contenance FROM parcelles WHERE id IN ({placeholders})",
        ids,
    ).fetchall()
    return {
        r[0]: ParcelMatch(
            id_parcelle=r[0],
            code_insee=r[1],
            nom_commune=r[1],
            contenance=r[2],
            centroid_lon=0.0,
            centroid_lat=0.0,
            geometry_geojson="{}",
            score=0.0,
        )
        for r in rows
    }


def _fetch_parcels_bulk(
    con: duckdb.DuckDBPyConnection,
    ids: list[str],
) -> dict[str, ParcelMatch]:
    if not ids:
        return {}
    placeholders = ", ".join("?" * len(ids))
    rows = con.execute(
        f"""
        SELECT p.id, p.code_insee, p.contenance,
               ST_X(ST_Centroid(p.geometry)) AS lon,
               ST_Y(ST_Centroid(p.geometry)) AS lat,
               ST_AsGeoJSON(p.geometry)      AS geojson,
               c.nom
        FROM parcelles p
        LEFT JOIN communes c ON c.code_insee = p.code_insee
        WHERE p.id IN ({placeholders})
        """,
        ids,
    ).fetchall()
    return {
        r[0]: ParcelMatch(
            id_parcelle=r[0],
            code_insee=r[1],
            nom_commune=r[6] or r[1],
            contenance=r[2],
            centroid_lon=r[3],
            centroid_lat=r[4],
            geometry_geojson=r[5] or "{}",
            score=0.0,
        )
        for r in rows
    }


def _build_combo_from_parts(parts: list[ParcelMatch], dpe_record: dict) -> ComboMatch:
    geoms = []
    for p in parts:
        try:
            geoms.append(shape(json.loads(p.geometry_geojson)))
        except Exception:
            pass

    combined = unary_union(geoms) if geoms else None
    if combined is not None and not combined.is_empty:
        combined_geojson = json.dumps(combined.__geo_interface__)
        centroid = combined.centroid
        clat, clon = centroid.y, centroid.x
        peri = combined.length
        pp = min(1.0, 4 * math.pi * combined.area / (peri ** 2)) if peri else 0.0
    else:
        clat, clon = parts[0].centroid_lat, parts[0].centroid_lon
        combined_geojson = "{}"
        pp = 0.0

    return ComboMatch(
        parts=parts,
        total_contenance=sum(p.contenance for p in parts),
        centroid_lat=clat,
        centroid_lon=clon,
        combined_geojson=combined_geojson,
        score=0.0,
        rank=0,
        compactness=pp,
        dpe_label=dpe_record.get("dpe"),
        ges_label=dpe_record.get("ges"),
    )


def _aggregate_around(
    con: duckdb.DuckDBPyConnection,
    anchor: ParcelMatch,
    target_terrain: float,
    tolerance_pct: float,
    dpe_record: dict,
) -> list[ComboMatch]:
    """BFS local autour d'une parcelle ancre, cherche les combos proches de la cible.

    Retourne une liste de ComboMatch triée par proximité à la cible.
    """
    delta = target_terrain * tolerance_pct / 100.0
    lo, hi = target_terrain - delta, target_terrain + delta

    # Frontière initiale : voisins directs de l'ancre
    initial_neighbors = _fetch_neighbor_ids(con, anchor.id_parcelle)
    if not initial_neighbors:
        return []

    # On hydrate l'ancre + voisins de niveau 1 dans un seul aller-retour
    visited_ids: set[str] = {anchor.id_parcelle, *initial_neighbors}
    parcels_cache = _fetch_parcels_minimal(con, list(visited_ids))
    if anchor.id_parcelle not in parcels_cache:
        parcels_cache[anchor.id_parcelle] = anchor

    neighbors_cache: dict[str, list[str]] = {anchor.id_parcelle: initial_neighbors}
    
    # On garde les N meilleurs combos uniques
    found_combos: dict[frozenset[str], tuple[float, list[ParcelMatch]]] = {}
    nodes = 0

    def dfs(current: list[ParcelMatch], total: float, frontier: list[str]) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_BFS_NODES:
            return

        if len(current) >= 2 and lo <= total <= hi:
            ids = frozenset(p.id_parcelle for p in current)
            d = abs(total - target_terrain)
            if ids not in found_combos or d < found_combos[ids][0]:
                found_combos[ids] = (d, list(current))
                logger.debug(f"[dpe_match] Combo trouvé : {total}m2 (delta {d})")

        if total >= hi or len(current) >= _MAX_AGGREGATE_PARTS:
            return

        # S'assurer que tous les IDs de la frontière sont dans le cache
        missing = [pid for pid in frontier if pid not in parcels_cache]
        if missing:
            parcels_cache.update(_fetch_parcels_minimal(con, missing))

        candidates = []
        for pid in frontier:
            p = parcels_cache.get(pid)
            if p:
                candidates.append(p)
        
        # Heuristique : on trie par proximité à la surface restante
        remaining = target_terrain - total
        candidates.sort(key=lambda x: abs(x.contenance - remaining))

        for p in candidates:
            if p.id_parcelle in {x.id_parcelle for x in current}:
                continue
            new_total = total + p.contenance
            if new_total > hi:
                continue

            if p.id_parcelle not in neighbors_cache:
                neighbors_cache[p.id_parcelle] = _fetch_neighbor_ids(con, p.id_parcelle)
            
            nbr_ids = neighbors_cache[p.id_parcelle]
            new_ids_to_fetch = [n for n in nbr_ids if n not in parcels_cache]
            if new_ids_to_fetch:
                parcels_cache.update(_fetch_parcels_minimal(con, new_ids_to_fetch))
            
            current_ids = {x.id_parcelle for x in current}
            new_frontier = sorted(
                {*frontier, *nbr_ids} - current_ids - {p.id_parcelle}
            )
            dfs(current + [p], new_total, new_frontier)

    dfs([anchor], anchor.contenance, initial_neighbors)

    if not found_combos:
        return []
    
    # Trier par delta et prendre les 10 meilleurs (pour laisser du choix à l'orchestration finale)
    sorted_candidates = sorted(found_combos.values(), key=lambda x: x[0])[:10]
    
    results = []
    for d, parts in sorted_candidates:
        # Hydrater complètement les parcelles
        full_parcels = _fetch_parcels_bulk(con, [p.id_parcelle for p in parts])
        hydrated_parts = [full_parcels[p.id_parcelle] for p in parts if p.id_parcelle in full_parcels]
        if hydrated_parts:
            results.append(_build_combo_from_parts(hydrated_parts, dpe_record))
    
    return results


def find_parcel_for_dpe_record(
    dpe_record: dict,
    target_terrain: Optional[float],
    tolerance_pct: float = 5.0,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Localise une parcelle (ou agrégat) pour un enregistrement DPE.

    Retourne une liste de candidats.
    """
    full_address = f"{dpe_record['address']}, {dpe_record['postcode']} {dpe_record['city']}"
    coords = geocode_address(
        full_address,
        city=dpe_record.get("city"),
        postcode=dpe_record.get("postcode"),
        citycode=dpe_record.get("code_insee"),
    )
    if not coords:
        return []
    lat, lon = coords

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")

        # --- Phase 1 : Filet de pêche local ---
        pool = _fetch_local_pool(con, lat, lon, dpe_record.get("code_insee"))

        if pool and target_terrain is not None:
            delta = target_terrain * tolerance_pct / 100.0
            lo, hi = target_terrain - delta, target_terrain + delta

            # Cherche d'abord une parcelle unique dans le pool
            for r in pool:
                if lo <= r[2] <= hi:
                    logger.debug(f"[dpe_match] Match direct dans le pool : {r[2]}m2")
                    return [_row_to_parcel_match(con, r, dpe_record)]

            # Micro-combos dans le pool
            combos = _find_micro_combos_in_pool(con, pool, target_terrain, tolerance_pct, dpe_record)
            if combos:
                logger.debug(f"[dpe_match] {len(combos)} micro-combo(s) trouvé(s) dans le pool.")
                return combos

        # --- Phase 2 : Fallback ancre + BFS ---
        anchor = _fetch_best_anchor(con, lat, lon, dpe_record)
        if anchor is None:
            return []

        if target_terrain is None:
            return [anchor]

        delta = target_terrain * tolerance_pct / 100.0
        lo, hi = target_terrain - delta, target_terrain + delta

        if lo <= anchor.contenance <= hi:
            return [anchor]

        if anchor.contenance < lo:
            for attempt in range(3):
                current_tol = tolerance_pct
                if attempt == 1:
                    current_tol = max(tolerance_pct, 5.0)
                elif attempt == 2:
                    current_tol = max(tolerance_pct, 15.0)
                combos = _aggregate_around(con, anchor, target_terrain, current_tol, dpe_record)
                if combos:
                    logger.debug(
                        f"[dpe_match] {len(combos)} combo(s) BFS trouvé(s) pour {target_terrain}m2 "
                        f"(essai: {attempt+1})"
                    )
                    return combos
                if attempt == 2:
                    break

        return [anchor]
    except Exception as e:
        logger.error(f"[dpe_match] Erreur localisation DPE : {e}")
        return []
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Recherche pilotée par le DPE (haut niveau)
# ---------------------------------------------------------------------------

def dpe_led_search(
    insee_codes: list[str],
    target_terrain: float,
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    tolerance_pct: float = 5.0,
    limit: int = 20,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Recherche complète DPE-led : ADEME → géocodage → parcelle/agrégat."""
    records = search_dpe(
        insee_codes=insee_codes,
        living_surface=living_surface,
        dpe_label=dpe_label,
        ges_label=ges_label,
        tolerance_pct=tolerance_pct,
        limit=limit,
        db_path=db_path,
    )
    if not records:
        logger.info("[dpe_match] Aucun enregistrement DPE pour ce périmètre.")
        return []

    logger.info(f"[dpe_match] {len(records)} enregistrement(s) DPE → géocodage et localisation parcelle.")
    matches: list[Union[ParcelMatch, ComboMatch]] = []
    seen_ids: set[frozenset[str]] = set()
    for rec in records:
        rec_matches = find_parcel_for_dpe_record(rec, target_terrain, tolerance_pct, db_path=db_path)
        for m in rec_matches:
            ids = (
                frozenset([m.id_parcelle]) if isinstance(m, ParcelMatch)
                else frozenset(m.ids)
            )
            if ids in seen_ids:
                continue
            seen_ids.add(ids)
            matches.append(m)

    logger.info(f"[dpe_match] {len(matches)} parcelle(s)/combo(s) localisé(s) via DPE.")
    return matches


# ---------------------------------------------------------------------------
# Recherche directe de positions DPE (sans matching parcellaire)
# ---------------------------------------------------------------------------

def _query_api_positions(
    scope_rang: dict[str, int],
    living_surface: float,
    dpe_label: Optional[str],
    ges_label: Optional[str],
    dpe_date: Optional[str],
    tolerance_pct: float,
    limit: int,
) -> list[DPEPositionMatch]:
    """Interroge l'API ADEME Open Data et convertit les résultats en DPEPositionMatch.

    Itère sur chaque commune du scope. Applique le filtre dpe_date côté Python
    (l'API Elasticsearch ne supporte pas ce filtre directement).
    Retourne une liste vide si l'API est indisponible ou retourne 0 résultat.
    """
    from cadastre_finder.search.external_search import _query_ademe_api

    # Tentatives progressives : plein → sans GES → sans DPE ni GES
    label_attempts = [(dpe_label, ges_label)]
    if dpe_label or ges_label:
        label_attempts.append((dpe_label, None))
        label_attempts.append((None, None))

    all_records: list[DPEPositionMatch] = []
    seen_addresses: set[str] = set()

    for attempt_idx, (try_dpe, try_ges) in enumerate(label_attempts):
        if attempt_idx > 0 and all_records:
            break
        if attempt_idx > 0:
            logger.debug(
                f"[dpe_positions/api] Relâchement essai {attempt_idx + 1} : "
                f"dpe={try_dpe}, ges={try_ges}"
            )

        for insee_code, rank in scope_rang.items():
            raw = _query_ademe_api(
                insee_code=insee_code,
                living_surface=living_surface,
                dpe_label=try_dpe,
                ges_label=try_ges,
                tolerance_pct=tolerance_pct,
                limit=limit * 3,
            )
            for rec in raw:
                # Filtre date côté Python (l'API ne le supporte pas)
                if dpe_date and rec.get("date") and str(rec["date"])[:10] != dpe_date:
                    continue

                lat, lon = None, None

                # Priorité : _geopoint WGS84 natif (lat,lon)
                if rec.get("geopoint"):
                    try:
                        parts = str(rec["geopoint"]).split(",")
                        if len(parts) == 2:
                            lat_t, lon_t = float(parts[0]), float(parts[1])
                            if 41.0 <= lat_t <= 51.5 and -5.5 <= lon_t <= 10.0:
                                lat, lon = lat_t, lon_t
                    except Exception:
                        pass

                # Fallback : coordonnées Lambert93 BAN
                if lat is None and rec.get("coord_x") and rec.get("coord_y"):
                    try:
                        lon_t, lat_t = _lambert93_to_wgs84.transform(
                            float(rec["coord_x"]), float(rec["coord_y"])
                        )
                        if 41.0 <= lat_t <= 51.5 and -5.5 <= lon_t <= 10.0:
                            lat, lon = lat_t, lon_t
                    except Exception:
                        pass

                if lat is None:
                    continue

                address = rec.get("address") or ""
                key = f"{address}|{rec.get('code_insee', '')}"
                if key in seen_addresses:
                    continue
                seen_addresses.add(key)

                surface = float(rec["surface"]) if rec.get("surface") is not None else 0.0
                score = max(0.0, 100.0 - abs(surface - living_surface) / living_surface * 100.0) - rank * 3

                all_records.append(DPEPositionMatch(
                    address=address,
                    postcode=rec.get("postcode") or "",
                    city=rec.get("city") or "",
                    code_insee=rec.get("code_insee") or "",
                    surface_habitable=surface,
                    dpe_label=rec.get("dpe"),
                    ges_label=rec.get("ges"),
                    date=str(rec["date"])[:10] if rec.get("date") else None,
                    centroid_lat=lat,
                    centroid_lon=lon,
                    score=round(score, 1),
                    rank=rank,
                ))

        if all_records:
            break

    all_records.sort(key=lambda m: m.score, reverse=True)
    logger.info(f"[dpe_positions/api] {len(all_records)} position(s) DPE trouvée(s) via API ADEME.")
    return all_records[:limit]


def search_dpe_positions(
    scope_rang: dict[str, int],
    living_surface: float,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    dpe_date: Optional[str] = None,
    conso_ep: Optional[float] = None,
    ges_ep: Optional[float] = None,
    tolerance_pct: float = 5.0,
    limit: int = 20,
    db_path: Path = DB_PATH,
) -> list[DPEPositionMatch]:
    """Retourne directement les positions GPS des enregistrements DPE correspondants.

    Interroge d'abord l'API ADEME Open Data (toujours à jour) puis bascule sur la
    base locale si l'API retourne 0 résultat. Les filtres kWh/CO₂ (conso_ep, ges_ep)
    s'appliquent uniquement via la base locale (l'API ne les expose pas).
    """
    if not scope_rang or living_surface <= 0:
        return []

    # --- Phase 1 : API ADEME (prioritaire) ---
    api_hits = _query_api_positions(
        scope_rang=scope_rang,
        living_surface=living_surface,
        dpe_label=dpe_label,
        ges_label=ges_label,
        dpe_date=dpe_date,
        tolerance_pct=tolerance_pct,
        limit=limit,
    )
    if api_hits:
        return api_hits

    # --- Phase 2 : fallback base locale ---
    logger.info("[dpe_positions] API ADEME sans résultat — basculement base locale.")
    insee_codes = list(scope_rang.keys())
    delta = living_surface * tolerance_pct / 100.0
    lo, hi = living_surface - delta, living_surface + delta

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table_exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{DPE_TABLE}'"
        ).fetchone()[0]
        if not table_exists:
            logger.warning(f"Table {DPE_TABLE} absente. Lancez l'ingestion DPE.")
            return []

        cols = {r[0].lower() for r in con.execute(f"DESCRIBE {DPE_TABLE}").fetchall()}
        has_coords = (
            "coordonnee_cartographique_x_ban" in cols
            and "coordonnee_cartographique_y_ban" in cols
        )
        if not has_coords:
            logger.warning("[dpe_positions] Colonnes de coordonnées BAN absentes de la table DPE.")
            return []

        has_conso = "conso_5_usages_par_m2_ep" in cols
        has_ges_ep = "emission_ges_5_usages par_m2" in cols

        placeholders = ", ".join("?" * len(insee_codes))
        base_query = f"""
            SELECT
                adresse_brut, code_postal_brut, nom_commune_brut, code_insee_ban,
                surface_habitable_logement, etiquette_dpe, etiquette_ges,
                date_etablissement_dpe,
                coordonnee_cartographique_x_ban, coordonnee_cartographique_y_ban
            FROM {DPE_TABLE}
            WHERE code_insee_ban IN ({placeholders})
              AND surface_habitable_logement BETWEEN ? AND ?
              AND coordonnee_cartographique_x_ban IS NOT NULL
              AND coordonnee_cartographique_y_ban IS NOT NULL
        """

        # Relâchement progressif : essai 0=tout, essai 1=sans conso/ges_ep, essai 2=sans GES
        rows: list = []
        for attempt in range(3):
            if attempt > 0 and rows:
                break
            query = base_query
            params: list = [*insee_codes, lo, hi]

            use_energy = (attempt == 0)
            use_ges = (attempt < 2)

            if dpe_label:
                query += " AND etiquette_dpe = ?"
                params.append(dpe_label)
            if ges_label and use_ges:
                query += " AND etiquette_ges = ?"
                params.append(ges_label)
            if dpe_date:
                query += " AND date_etablissement_dpe = ?"
                params.append(dpe_date)
            if use_energy and conso_ep is not None and has_conso:
                delta_c = conso_ep * tolerance_pct / 100.0
                query += " AND conso_5_usages_par_m2_ep BETWEEN ? AND ?"
                params.extend([conso_ep - delta_c, conso_ep + delta_c])
            if use_energy and ges_ep is not None and has_ges_ep:
                delta_g = ges_ep * tolerance_pct / 100.0
                query += ' AND "emission_ges_5_usages par_m2" BETWEEN ? AND ?'
                params.extend([ges_ep - delta_g, ges_ep + delta_g])

            query += " ORDER BY date_etablissement_dpe DESC LIMIT ?"
            params.append(limit * 3)  # Marge car on filtre les coords invalides

            rows = con.execute(query, params).fetchall()
            if rows:
                if attempt > 0:
                    logger.debug(f"[dpe_positions] Résultats trouvés après relâchement (essai {attempt + 1}).")
                break

    except Exception as e:
        logger.error(f"[dpe_positions] Erreur requête DPE : {e}")
        return []
    finally:
        con.close()

    results: list[DPEPositionMatch] = []
    for r in rows:
        x_ban, y_ban = r[8], r[9]
        try:
            lon, lat = _lambert93_to_wgs84.transform(float(x_ban), float(y_ban))
        except Exception:
            continue
        # Coordonnées hors France métropolitaine → ignorer
        if not (41.0 <= lat <= 51.5 and -5.5 <= lon <= 10.0):
            continue

        surface = r[4] or 0.0
        rank = scope_rang.get(r[3], 0)
        score = max(0.0, 100.0 - abs(surface - living_surface) / living_surface * 100.0) - rank * 3

        results.append(DPEPositionMatch(
            address=r[0] or "",
            postcode=r[1] or "",
            city=r[2] or "",
            code_insee=r[3] or "",
            surface_habitable=surface,
            dpe_label=r[5],
            ges_label=r[6],
            date=str(r[7]) if r[7] else None,
            centroid_lat=lat,
            centroid_lon=lon,
            score=round(score, 1),
            rank=rank,
        ))

        if len(results) >= limit:
            break

    results.sort(key=lambda m: m.score, reverse=True)
    logger.info(f"[dpe_positions] {len(results)} position(s) DPE trouvée(s).")
    return results


# ---------------------------------------------------------------------------
# Enrichissement DPE des combos (chemin sans DPE)
# ---------------------------------------------------------------------------

def enrich_combos_dpe(
    combos: list[ComboMatch],
    db_path: Path = DB_PATH,
) -> None:
    """Enrichit les combos avec les labels DPE/GES via jointure spatiale (best-effort).

    Pour chaque combo, cherche un enregistrement DPE dont les coordonnées tombent
    dans l'une des parcelles ancres (bâti > 0). Modifie les combos en place.
    """
    if not combos:
        return

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        table_exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{DPE_TABLE}'"
        ).fetchone()[0]
        if not table_exists:
            return

        # Vérifie que les colonnes de coordonnées Lambert 93 BAN existent
        cols = {r[0].lower() for r in con.execute(f"DESCRIBE {DPE_TABLE}").fetchall()}
        if "coordonnee_cartographique_x_ban" not in cols or "coordonnee_cartographique_y_ban" not in cols:
            return

        # Collecte tous les IDs de parcelles ancres de tous les combos
        parcel_to_combo: dict[str, list[ComboMatch]] = {}
        for combo in combos:
            for p in combo.parts:
                if p.built_area and p.built_area > 0:
                    parcel_to_combo.setdefault(p.id_parcelle, []).append(combo)

        if not parcel_to_combo:
            return

        placeholders = ", ".join("?" * len(parcel_to_combo))
        # Les coordonnées BAN sont en Lambert 93 (EPSG:2154).
        # Les géométries parcelles sont en WGS84 ; ST_Transform inverse les axes,
        # d'où l'usage de ST_FlipCoordinates avant la transformation.
        # On sélectionne le DPE le plus proche (< 200 m du centroïde de la parcelle).
        rows = con.execute(f"""
            SELECT p.id, d.etiquette_dpe, d.etiquette_ges,
                ST_Distance(
                    ST_Point(d.coordonnee_cartographique_x_ban, d.coordonnee_cartographique_y_ban),
                    ST_Transform(ST_FlipCoordinates(ST_Centroid(p.geometry)), 'EPSG:4326', 'EPSG:2154')
                ) AS dist_m
            FROM parcelles p
            JOIN {DPE_TABLE} d
              ON d.code_insee_ban = p.code_insee
             AND d.coordonnee_cartographique_x_ban IS NOT NULL
             AND d.coordonnee_cartographique_y_ban IS NOT NULL
             AND ST_Distance(
                    ST_Point(d.coordonnee_cartographique_x_ban, d.coordonnee_cartographique_y_ban),
                    ST_Transform(ST_FlipCoordinates(ST_Centroid(p.geometry)), 'EPSG:4326', 'EPSG:2154')
                ) < 200
            WHERE p.id IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY dist_m) = 1
        """, list(parcel_to_combo.keys())).fetchall()

        for parcel_id, dpe, ges, _dist in rows:
            for combo in parcel_to_combo.get(parcel_id, []):
                if combo.dpe_label is None:
                    combo.dpe_label = dpe
                    combo.ges_label = ges
    except Exception as e:
        logger.debug(f"[dpe_match] Enrichissement DPE des combos échoué (ignoré) : {e}")
    finally:
        con.close()
