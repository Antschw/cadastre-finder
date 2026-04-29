"""Moteur de recherche — Étape 3 : périmètre par intersection de contraintes géométriques.

Usage :
    constraints = [
        NearPOI(category="poi_religious", name="Saint-Pierre", max_distance_m=1500),
        NearPOI(category="poi_transport", commune="Mortagne", max_distance_m=3000),
        AwayFromFeature(category="roads_major", min_distance_m=300),
        AwayFromFeature(category="railways", min_distance_m=150),
        InCommuneOrNeighbors(commune="Mortagne-au-Perche", rank=2),
    ]
    matches = search_by_proximity(constraints, min_surface=2500)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger
from shapely import wkt as shapely_wkt
from shapely.geometry import shape
from shapely.ops import unary_union

from cadastre_finder.config import CRS_LAMBERT93, CRS_WGS84, DB_PATH, MIN_TERRAIN_M2
from cadastre_finder.processing.adjacency import get_neighbors
from cadastre_finder.search.models import ParcelMatch
from cadastre_finder.utils.geocoding import resolve_commune

try:
    from pyproj import Transformer
    _HAS_PYPROJ = True
except ImportError:
    _HAS_PYPROJ = False


# ---------------------------------------------------------------------------
# Types de contraintes
# ---------------------------------------------------------------------------

@dataclass
class NearPOI:
    """Contrainte de proximité positive : être dans un rayon autour d'un POI."""
    category: str           # Table OSM : poi_religious, poi_transport, poi_admin, waterways…
    max_distance_m: float   # Rayon max en mètres
    name: Optional[str] = None      # Filtre sur le nom du POI (recherche partielle)
    commune: Optional[str] = None   # Filtre sur la commune du POI


@dataclass
class AwayFromFeature:
    """Contrainte d'exclusion : être à au moins N mètres d'une infrastructure."""
    category: str           # Table OSM : roads_major, railways, waterways…
    min_distance_m: float   # Distance minimale en mètres


@dataclass
class InCommuneOrNeighbors:
    """Contrainte de localisation : être dans la commune ou ses voisines."""
    commune: str
    rank: int = 1
    postal_code: Optional[str] = None


Constraint = NearPOI | AwayFromFeature | InCommuneOrNeighbors


# ---------------------------------------------------------------------------
# Helpers de reprojection
# ---------------------------------------------------------------------------

def _to_lambert93(geom):
    """Reprojette une géométrie Shapely de WGS84 vers Lambert-93."""
    if not _HAS_PYPROJ:
        raise RuntimeError("pyproj est requis pour les calculs de distance.")
    transformer = Transformer.from_crs(CRS_WGS84, CRS_LAMBERT93, always_xy=True)
    from shapely.ops import transform
    return transform(transformer.transform, geom)


def _to_wgs84(geom):
    """Reprojette une géométrie Shapely de Lambert-93 vers WGS84."""
    transformer = Transformer.from_crs(CRS_LAMBERT93, CRS_WGS84, always_xy=True)
    from shapely.ops import transform
    return transform(transformer.transform, geom)


# ---------------------------------------------------------------------------
# Résolution des contraintes en zones Shapely (WGS84 final)
# ---------------------------------------------------------------------------

def _resolve_near_poi(
    constraint: NearPOI, con: duckdb.DuckDBPyConnection
):
    """Calcule la zone tampon autour des POI correspondants."""
    table = constraint.category
    conditions = ["1=1"]
    params = []

    if constraint.name:
        conditions.append("LOWER(tags->>'name') LIKE LOWER(?)")
        params.append(f"%{constraint.name}%")

    where = " AND ".join(conditions)
    rows = con.execute(
        f"SELECT ST_AsText(geometry) FROM {table} WHERE {where}",
        params,
    ).fetchall()

    if not rows:
        logger.warning(f"[proximity] Aucun POI trouvé dans {table} (filtre: {constraint})")
        return None

    # Union de tous les POI → buffer en Lambert-93
    geoms = []
    for (wkt_str,) in rows:
        try:
            g = shapely_wkt.loads(wkt_str)
            g_l93 = _to_lambert93(g)
            geoms.append(g_l93.buffer(constraint.max_distance_m))
        except Exception as e:
            logger.debug(f"Géométrie ignorée : {e}")

    if not geoms:
        return None

    return unary_union(geoms)


def _resolve_away_from(
    constraint: AwayFromFeature, con: duckdb.DuckDBPyConnection
):
    """Calcule la zone à exclure (buffer autour des infrastructures)."""
    table = constraint.category
    rows = con.execute(
        f"SELECT ST_AsText(geometry) FROM {table}"
    ).fetchall()

    if not rows:
        logger.warning(f"[proximity] Table {table} vide.")
        return None

    geoms = []
    for (wkt_str,) in rows:
        try:
            g = shapely_wkt.loads(wkt_str)
            g_l93 = _to_lambert93(g)
            geoms.append(g_l93.buffer(constraint.min_distance_m))
        except Exception:
            pass

    if not geoms:
        return None

    return unary_union(geoms)


def _resolve_commune_zone(
    constraint: InCommuneOrNeighbors,
    con: duckdb.DuckDBPyConnection,
    db_path: Path,
):
    """Calcule la zone géométrique couvrant la commune et ses voisines."""
    result = resolve_commune(constraint.commune, constraint.postal_code, db_path)
    if not result.candidates:
        logger.warning(f"[proximity] Commune introuvable : '{constraint.commune}'")
        return None

    main_insee = result.best.code_insee
    neighbors = get_neighbors(main_insee, rang=constraint.rank, db_path=db_path)
    all_codes = [main_insee] + neighbors

    placeholders = ", ".join("?" * len(all_codes))
    rows = con.execute(
        f"SELECT ST_AsText(geometry) FROM communes WHERE code_insee IN ({placeholders})",
        all_codes,
    ).fetchall()

    geoms = []
    for (wkt_str,) in rows:
        try:
            g = shapely_wkt.loads(wkt_str)
            g_l93 = _to_lambert93(g)
            geoms.append(g_l93)
        except Exception:
            pass

    return unary_union(geoms) if geoms else None


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def search_by_proximity(
    constraints: list[Constraint],
    min_surface: float = MIN_TERRAIN_M2,
    db_path: Path = DB_PATH,
) -> list[ParcelMatch]:
    """Recherche les parcelles bâties dans la zone résultant des contraintes.

    Algorithme :
    1. Reprojeter en Lambert-93
    2. Calculer les zones positives (NearPOI, InCommuneOrNeighbors) et les intersecter
    3. Soustraire les zones d'exclusion (AwayFromFeature)
    4. Sélectionner les parcelles bâties dans la zone résultante
    5. Retourner en WGS84

    Returns:
        Liste de ParcelMatch dans la zone de recherche.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")

        positive_zones = []
        exclusion_zones = []

        for c in constraints:
            if isinstance(c, NearPOI):
                zone = _resolve_near_poi(c, con)
                if zone:
                    positive_zones.append(zone)
            elif isinstance(c, AwayFromFeature):
                zone = _resolve_away_from(c, con)
                if zone:
                    exclusion_zones.append(zone)
            elif isinstance(c, InCommuneOrNeighbors):
                zone = _resolve_commune_zone(c, con, db_path)
                if zone:
                    positive_zones.append(zone)

        if not positive_zones:
            logger.error("[proximity] Aucune zone positive calculée. Abandon.")
            return []

        # Intersection de toutes les zones positives
        search_zone_l93 = positive_zones[0]
        for z in positive_zones[1:]:
            search_zone_l93 = search_zone_l93.intersection(z)

        # Soustraction des zones d'exclusion
        for z in exclusion_zones:
            search_zone_l93 = search_zone_l93.difference(z)

        if search_zone_l93.is_empty:
            logger.warning("[proximity] Zone de recherche vide après application des contraintes.")
            return []

        # Reprojection en WGS84 pour la requête spatiale DuckDB
        search_zone_wgs84 = _to_wgs84(search_zone_l93)
        zone_wkt = search_zone_wgs84.wkt

        logger.info(
            f"[proximity] Zone de recherche : {search_zone_wgs84.area * 1e10:.0f} km² env."
        )

        # Sélection des parcelles bâties dans la zone
        # "Bâtie" = parcelle dont le centroïde intersecte un bâtiment OSM
        rows = con.execute("""
            SELECT
                p.id,
                p.code_insee,
                c.nom,
                p.contenance,
                ST_X(ST_Centroid(p.geometry)) AS lon,
                ST_Y(ST_Centroid(p.geometry)) AS lat,
                ST_AsGeoJSON(p.geometry)      AS geojson
            FROM parcelles p
            LEFT JOIN communes c ON c.code_insee = p.code_insee
            WHERE p.contenance >= ?
              AND ST_Within(ST_Centroid(p.geometry), ST_GeomFromText(?))
              AND EXISTS (
                  SELECT 1 FROM buildings b
                  WHERE ST_Intersects(b.geometry, p.geometry)
              )
            ORDER BY p.contenance DESC
        """, [min_surface, zone_wkt]).fetchall()

    finally:
        con.close()

    matches = []
    for id_parc, insee, nom, contenance, lon, lat, geojson in rows:
        matches.append(ParcelMatch(
            id_parcelle=id_parc,
            code_insee=insee,
            nom_commune=nom or insee,
            contenance=contenance,
            centroid_lat=lat,
            centroid_lon=lon,
            geometry_geojson=geojson or "{}",
            score=50.0,  # Score neutre, pas de surface de référence
            rank=0,
        ))

    logger.info(f"[proximity] {len(matches)} parcelle(s) trouvée(s) dans la zone.")
    return matches
