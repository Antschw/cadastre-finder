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

from cadastre_finder.config import DB_PATH, DPE_TABLE
from cadastre_finder.search.models import ComboMatch, ParcelMatch
from cadastre_finder.utils.geocoding import geocode_address, reverse_geocode_parcel


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
        table_exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{DPE_TABLE}'"
        ).fetchone()[0]
        if not table_exists:
            logger.warning(f"Table {DPE_TABLE} absente. Lancez l'ingestion DPE.")
            return []

        # On tente plusieurs niveaux de relâchement si aucun résultat
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
                break
        else:
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
            for r in rows
        ]
    except Exception as e:
        logger.error(f"[dpe_match] Erreur lors de la recherche DPE : {e}")
        return []
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Localisation parcelle pour un enregistrement DPE
# ---------------------------------------------------------------------------

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
) -> Optional[ComboMatch]:
    """BFS local autour d'une parcelle ancre, cherche le combo le plus proche de la cible.

    Retourne `None` si aucune combinaison de 2-6 parcelles n'atteint la fenêtre.
    """
    delta = target_terrain * tolerance_pct / 100.0
    lo, hi = target_terrain - delta, target_terrain + delta

    # Frontière initiale : voisins directs de l'ancre
    initial_neighbors = _fetch_neighbor_ids(con, anchor.id_parcelle)
    if not initial_neighbors:
        return None

    # On hydrate l'ancre + voisins de niveau 1 dans un seul aller-retour
    visited_ids: set[str] = {anchor.id_parcelle, *initial_neighbors}
    parcels_cache = _fetch_parcels_minimal(con, list(visited_ids))
    if anchor.id_parcelle not in parcels_cache:
        parcels_cache[anchor.id_parcelle] = anchor

    neighbors_cache: dict[str, list[str]] = {anchor.id_parcelle: initial_neighbors}
    best_combo: Optional[list[ParcelMatch]] = None
    best_delta: float = float("inf")
    nodes = 0

    def dfs(current: list[ParcelMatch], total: float, frontier: list[str]) -> None:
        nonlocal best_combo, best_delta, nodes
        nodes += 1
        if nodes > _MAX_BFS_NODES:
            return

        if len(current) >= 2 and lo <= total <= hi:
            d = abs(total - target_terrain)
            if d < best_delta:
                best_delta = d
                best_combo = list(current)
                logger.debug(f"[dpe_match] Nouveau meilleur combo : {total}m2 (delta {d})")
                if d <= target_terrain * 0.01:  # Match très proche trouvé (1%)
                    return

        if total >= hi or len(current) >= _MAX_AGGREGATE_PARTS:
            return

        # Trier la frontière par surface décroissante (heuristique sac à dos / convergence)
        # On favorise les parcelles qui nous rapprochent le plus de la cible.
        
        # S'assurer que tous les IDs de la frontière sont dans le cache
        missing = [pid for pid in frontier if pid not in parcels_cache]
        if missing:
            parcels_cache.update(_fetch_parcels_minimal(con, missing))

        candidates = []
        for pid in frontier:
            p = parcels_cache.get(pid)
            if p:
                candidates.append(p)
        
        # Heuristique : on trie pour prendre les plus grandes d'abord (greedy)
        # ou celles qui complètent le mieux la surface restante.
        remaining = target_terrain - total
        candidates.sort(key=lambda x: abs(x.contenance - remaining))

        for p in candidates:
            if p.id_parcelle in {x.id_parcelle for x in current}:
                continue
            new_total = total + p.contenance
            if new_total > hi:
                continue

            # Récupérer voisins via cache
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
            if best_delta == 0:  # Sortie précoce si match parfait
                return

    dfs([anchor], anchor.contenance, initial_neighbors)

    if best_combo is None:
        return None
    
    # Hydrater complètement les parcelles du best_combo avant de construire le ComboMatch
    full_parcels = _fetch_parcels_bulk(con, [p.id_parcelle for p in best_combo])
    hydrated_parts = [full_parcels[p.id_parcelle] for p in best_combo if p.id_parcelle in full_parcels]
    
    return _build_combo_from_parts(hydrated_parts, dpe_record)


def find_parcel_for_dpe_record(
    dpe_record: dict,
    target_terrain: Optional[float],
    tolerance_pct: float = 5.0,
    db_path: Path = DB_PATH,
) -> Optional[Union[ParcelMatch, ComboMatch]]:
    """Localise une parcelle (ou agrégat) pour un enregistrement DPE.

    1. Géocode l'adresse via la Géoplateforme.
    2. Cherche la parcelle locale qui contient ce point (`ST_Intersects`).
    3. Si la parcelle trouvée est trop petite vs `target_terrain` (hors tolérance basse),
       tente une agrégation BFS sur ses voisines.
    4. Si aucune parcelle locale, fallback `/reverse` Géoplateforme + lookup par id.
    """
    full_address = f"{dpe_record['address']}, {dpe_record['postcode']} {dpe_record['city']}"
    coords = geocode_address(
        full_address,
        city=dpe_record.get("city"),
        postcode=dpe_record.get("postcode"),
        citycode=dpe_record.get("code_insee"),
    )
    if not coords:
        return None
    lat, lon = coords

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")

        anchor = _fetch_parcel_at_point(con, lat, lon, dpe_record)
        if anchor is None:
            # Fallback : Géoplateforme reverse parcel pour récupérer un id officiel
            logger.debug(
                f"[dpe_match] Pas de parcelle locale pour ({lat}, {lon}), "
                "essai reverse geocoding API..."
            )
            parcel_id = reverse_geocode_parcel(lat, lon)
            if not parcel_id:
                return None
            anchor = _fetch_parcel_by_id(con, parcel_id, dpe_record)
            if anchor is None:
                return None

        if target_terrain is None:
            return anchor

        delta = target_terrain * tolerance_pct / 100.0
        lo, hi = target_terrain - delta, target_terrain + delta

        # Parcelle ancre déjà dans la fenêtre cible → on la prend telle quelle
        if lo <= anchor.contenance <= hi:
            return anchor

        # Parcelle ancre plus petite → tenter agrégation
        if anchor.contenance < lo:
            # On tente plusieurs niveaux de relâchement pour l'agrégation
            for attempt in range(3):
                current_tol = tolerance_pct
                if attempt == 1:
                    current_tol = max(tolerance_pct, 5.0)  # min 5%
                elif attempt == 2:
                    current_tol = max(tolerance_pct, 15.0) # min 15%

                combo = _aggregate_around(con, anchor, target_terrain, current_tol, dpe_record)
                if combo is not None:
                    logger.debug(
                        f"[dpe_match] Combo trouvé pour {target_terrain}m2 "
                        f"(trouvé: {combo.total_contenance}m2, tol: {current_tol}%, essai: {attempt+1})"
                    )
                    return combo

                # Si on n'a rien trouvé même avec 15% de tolérance, on s'arrête
                if attempt == 2:
                    break

        # Parcelle plus grande que la cible (ou agrégation infructueuse)
        # → on retourne l'ancre quand même ; les filtres durs trancheront.
        return anchor
    except Exception as e:
        logger.error(f"[dpe_match] Erreur localisation DPE : {e}")
        return None
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
        m = find_parcel_for_dpe_record(rec, target_terrain, tolerance_pct, db_path=db_path)
        if m is None:
            continue
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
