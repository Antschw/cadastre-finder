"""Recherche de parcelles cadastrales via API publiques.

Utilisé quand le département du bien est hors du périmètre de la base locale.
APIs utilisées :
  - Géoplateforme IGN (déjà dans geocoding.py) : résolution commune + géocodage adresse
  - ADEME Open Data : enregistrements DPE filtrables par commune / surface / label
  - IGN Apicarto : parcelles cadastrales d'une commune avec surface et géométrie
"""
from __future__ import annotations

import json
import math
from typing import Optional

import httpx
from loguru import logger
from pyproj import Transformer
from shapely.geometry import shape

from cadastre_finder.config import ADEME_API_URL, IGN_APICARTO_URL
from cadastre_finder.search.models import ComboMatch, ParcelMatch
from cadastre_finder.utils.geocoding import geocode_address, resolve_commune

_lambert93_to_wgs84 = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _query_ademe_api(
    insee_code: str,
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    tolerance_pct: float = 10.0,
    limit: int = 20,
) -> list[dict]:
    """Interroge l'API ADEME Open Data pour récupérer des enregistrements DPE.

    Filtre la surface habitable directement dans le qs Elasticsearch pour éviter
    de paginer des centaines de résultats quand un code INSEE est très actif.
    """
    qs_parts = [f"code_insee_ban:{insee_code}"]
    if dpe_label:
        qs_parts.append(f"etiquette_dpe:{dpe_label}")
    if ges_label:
        qs_parts.append(f"etiquette_ges:{ges_label}")
    if living_surface:
        delta = living_surface * tolerance_pct / 100.0
        lo, hi = int(living_surface - delta), int(living_surface + delta) + 1
        qs_parts.append(f"surface_habitable_logement:[{lo} TO {hi}]")

    params: dict = {
        "qs": " AND ".join(qs_parts),
        "size": limit * 3,
    }

    try:
        resp = httpx.get(f"{ADEME_API_URL}/lines", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[external_search] ADEME API indisponible : {e}")
        return []

    results = data.get("results", [])

    # Filtre Python de précision (la plage Elasticsearch est entière)
    if living_surface:
        delta = living_surface * tolerance_pct / 100.0
        lo_f, hi_f = living_surface - delta, living_surface + delta
        results = [
            r for r in results
            if r.get("surface_habitable_logement") is not None
            and lo_f <= float(r["surface_habitable_logement"]) <= hi_f
        ]

    return [
        {
            "address": r.get("adresse_brut", ""),
            "postcode": r.get("code_postal_brut", ""),
            "city": r.get("nom_commune_brut", ""),
            "code_insee": r.get("code_insee_ban", ""),
            "surface": r.get("surface_habitable_logement"),
            "dpe": r.get("etiquette_dpe"),
            "ges": r.get("etiquette_ges"),
            "date": r.get("date_etablissement_dpe"),
            "coord_x": r.get("coordonnee_cartographique_x_ban"),
            "coord_y": r.get("coordonnee_cartographique_y_ban"),
            "geopoint": r.get("_geopoint"),
        }
        for r in results[:limit]
    ]


def _query_parcels_ign(
    insee_code: str,
    target_surface: Optional[float] = None,
    tolerance_pct: float = 10.0,
    limit: int = 2000,
) -> list[dict]:
    """Récupère les parcelles d'une commune via IGN Apicarto.

    Si `target_surface` est fourni, filtre sur la contenance ±tolerance.
    Sinon retourne toutes les parcelles (utile pour la recherche par ancre DPE).
    """
    lo, hi = None, None
    if target_surface is not None:
        delta = target_surface * tolerance_pct / 100.0
        lo, hi = target_surface - delta, target_surface + delta

    params = {
        "code_insee": insee_code,
        "_limit": limit,
        "_offset": 0,
    }

    try:
        resp = httpx.get(f"{IGN_APICARTO_URL}/parcelle", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[external_search] IGN Apicarto indisponible : {e}")
        return []

    results = []
    seen_ids: set[str] = set()
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        contenance = props.get("contenance")
        if contenance is None:
            continue
        contenance = float(contenance)
        if lo is not None and not (lo <= contenance <= hi):
            continue

        geom = feat.get("geometry")
        if not geom:
            continue

        try:
            shapely_geom = shape(geom)
            centroid = shapely_geom.centroid
            clat, clon = centroid.y, centroid.x
            geojson_str = json.dumps(geom)
        except Exception:
            continue

        parcel_id = props.get("id") or (
            f"{props.get('commune','')}{props.get('prefixe','000')}"
            f"{props.get('section','')}{props.get('numero','')}"
        )

        if parcel_id in seen_ids:
            continue
        seen_ids.add(parcel_id)

        results.append({
            "id": parcel_id,
            "contenance": int(contenance),
            "centroid_lat": clat,
            "centroid_lon": clon,
            "geometry_geojson": geojson_str,
            "shapely_geom": shapely_geom,
        })

    return results


def _parcels_near_point(
    parcels: list[dict],
    lat: float,
    lon: float,
    radius_m: float = 300.0,
) -> list[dict]:
    """Filtre les parcelles par distance au point (Haversine sur centroïdes)."""
    return [
        p for p in parcels
        if _haversine_m(lat, lon, p["centroid_lat"], p["centroid_lon"]) <= radius_m
    ]


def _is_connected_ext(pool: list[dict]) -> bool:
    """Vérifie que les parcelles forment un bloc contigu (intersections Shapely)."""
    n = len(pool)
    if n <= 1:
        return True
    geoms = [p["shapely_geom"] for p in pool]
    adj: dict[int, set[int]] = {i: set() for i in range(n)}
    for i in range(n):
        for j in range(i + 1, n):
            if geoms[i] is not None and geoms[j] is not None and geoms[i].intersects(geoms[j]):
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


def _find_combos_near_anchor(
    pool: list[dict],
    target_surface: float,
    tolerance_pct: float,
    dpe_rec: dict,
    insee_code: str,
    nom_commune: str,
    max_parts: int = 3,
) -> list:
    """Cherche des combinaisons adjacentes dans le pool dont la surface somme ≈ target."""
    from itertools import combinations as _combos
    from shapely.ops import unary_union

    delta = target_surface * tolerance_pct / 100.0
    lo, hi = target_surface - delta, target_surface + delta
    results = []
    seen: set[frozenset] = set()

    for k in range(1, min(max_parts + 1, len(pool) + 1)):
        for combo in _combos(pool, k):
            total = sum(p["contenance"] for p in combo)
            if not (lo <= total <= hi):
                continue
            ids = frozenset(p["id"] for p in combo)
            if ids in seen:
                continue
            if k > 1 and not _is_connected_ext(list(combo)):
                continue
            seen.add(ids)

            if k == 1:
                p = combo[0]
                score = max(0.0, 100.0 - abs(p["contenance"] - target_surface) / target_surface * 100.0)
                from cadastre_finder.search.models import ParcelMatch
                results.append(ParcelMatch(
                    id_parcelle=p["id"],
                    code_insee=insee_code,
                    nom_commune=nom_commune,
                    contenance=p["contenance"],
                    centroid_lat=p["centroid_lat"],
                    centroid_lon=p["centroid_lon"],
                    geometry_geojson=p["geometry_geojson"],
                    score=round(score, 2),
                    dpe_label=dpe_rec.get("dpe"),
                    ges_label=dpe_rec.get("ges"),
                ))
            else:
                geoms = [p["shapely_geom"] for p in combo if p.get("shapely_geom")]
                try:
                    union = unary_union(geoms)
                    centroid = union.centroid
                    clat, clon = centroid.y, centroid.x
                    combined_geojson = json.dumps(union.__geo_interface__)
                    area = union.area
                    perim = union.length
                    compactness = (4 * math.pi * area / perim ** 2) if perim > 0 else 0.0
                except Exception:
                    clat = sum(p["centroid_lat"] for p in combo) / k
                    clon = sum(p["centroid_lon"] for p in combo) / k
                    combined_geojson = combo[0]["geometry_geojson"]
                    compactness = 0.0

                score = max(0.0, 100.0 - abs(total - target_surface) / target_surface * 100.0)
                from cadastre_finder.search.models import ComboMatch, ParcelMatch
                parts = [
                    ParcelMatch(
                        id_parcelle=p["id"],
                        code_insee=insee_code,
                        nom_commune=nom_commune,
                        contenance=p["contenance"],
                        centroid_lat=p["centroid_lat"],
                        centroid_lon=p["centroid_lon"],
                        geometry_geojson=p["geometry_geojson"],
                        score=0.0,
                        dpe_label=dpe_rec.get("dpe"),
                        ges_label=dpe_rec.get("ges"),
                    )
                    for p in combo
                ]
                results.append(ComboMatch(
                    parts=parts,
                    total_contenance=int(total),
                    centroid_lat=clat,
                    centroid_lon=clon,
                    combined_geojson=combined_geojson,
                    score=round(score, 2),
                    compactness=round(compactness, 3),
                    dpe_label=dpe_rec.get("dpe"),
                    ges_label=dpe_rec.get("ges"),
                ))
        if results:
            break

    return results


def search_external(
    commune: str,
    surface_m2: float,
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    postal_code: Optional[str] = None,
    tolerance_pct: float = 10.0,
) -> list:
    """Recherche de parcelles via API publiques (communes hors périmètre local).

    Flux :
    1. Résolution INSEE via Géoplateforme
    2. Toutes les parcelles IGN de la commune (sans filtre surface)
    3. Enregistrements DPE ADEME filtrés par commune / surface habitable / label
    4. Pour chaque DPE : trouver les parcelles à ≤300m, chercher combos adjacentes
       dont la somme = surface_m2 ±tolerance (1 à 3 parcelles)
    5. Fallback : si aucun ancrage DPE → parcelles individuelles filtrées par surface
    """
    res = resolve_commune(commune, postal_code)
    if not res.best:
        logger.warning(f"[external_search] Commune introuvable : '{commune}'")
        return []

    insee_code = res.best.code_insee
    nom_commune = res.best.nom or commune
    logger.info(f"[external_search] '{nom_commune}' ({insee_code}) — recherche via API publiques.")

    # Toutes les parcelles de la commune (nécessaire pour détecter les combos)
    all_parcels = _query_parcels_ign(insee_code, limit=2000)
    logger.info(f"[external_search] {len(all_parcels)} parcelle(s) IGN totales dans la commune.")

    if not all_parcels:
        return []

    dpe_records = _query_ademe_api(
        insee_code, living_surface, dpe_label, ges_label, tolerance_pct
    )
    logger.info(f"[external_search] {len(dpe_records)} enregistrement(s) DPE ADEME.")

    matched_ids: set[frozenset] = set()
    results: list = []

    for rec in dpe_records:
        lat, lon = None, None

        # Priorité : _geopoint WGS84 natif du dataset (lat,lon)
        if rec.get("geopoint"):
            try:
                gp_parts = str(rec["geopoint"]).split(",")
                if len(gp_parts) == 2:
                    lat_t, lon_t = float(gp_parts[0]), float(gp_parts[1])
                    if 41.0 <= lat_t <= 51.5 and -5.5 <= lon_t <= 10.0:
                        lat, lon = lat_t, lon_t
            except Exception:
                pass

        # Fallback : coordonnées Lambert93 BAN
        if lat is None and rec["coord_x"] and rec["coord_y"]:
            try:
                lon_t, lat_t = _lambert93_to_wgs84.transform(
                    float(rec["coord_x"]), float(rec["coord_y"])
                )
                if 41.0 <= lat_t <= 51.5 and -5.5 <= lon_t <= 10.0:
                    lat, lon = lat_t, lon_t
            except Exception:
                pass

        # Fallback géocodage adresse
        if lat is None and rec.get("address"):
            full_addr = f"{rec['address']}, {rec['postcode']} {rec['city']}"
            coords = geocode_address(
                full_addr,
                city=rec.get("city"),
                postcode=rec.get("postcode"),
                citycode=rec.get("code_insee"),
            )
            if coords:
                lat, lon = coords

        if lat is None:
            continue

        nearby = _parcels_near_point(all_parcels, lat, lon, radius_m=300.0)
        combos = _find_combos_near_anchor(
            nearby, surface_m2, tolerance_pct, rec, insee_code, nom_commune
        )
        for c in combos:
            from cadastre_finder.search.models import ComboMatch, ParcelMatch
            key = frozenset(c.ids if isinstance(c, ComboMatch) else [c.id_parcelle])
            if key in matched_ids:
                continue
            matched_ids.add(key)
            results.append(c)

    # Fallback : pas d'ancrage DPE → parcelles individuelles dans la fenêtre de surface
    if not results:
        logger.info("[external_search] Aucun ancrage DPE — retour des parcelles candidates par surface.")
        delta = surface_m2 * tolerance_pct / 100.0
        lo, hi = surface_m2 - delta, surface_m2 + delta
        for p in all_parcels:
            if not (lo <= p["contenance"] <= hi):
                continue
            score = max(0.0, 100.0 - abs(p["contenance"] - surface_m2) / surface_m2 * 100.0)
            from cadastre_finder.search.models import ParcelMatch
            results.append(ParcelMatch(
                id_parcelle=p["id"],
                code_insee=insee_code,
                nom_commune=nom_commune,
                contenance=p["contenance"],
                centroid_lat=p["centroid_lat"],
                centroid_lon=p["centroid_lon"],
                geometry_geojson=p["geometry_geojson"],
                score=round(score, 1),
                dpe_label=dpe_label,
                ges_label=ges_label,
            ))

    results.sort(key=lambda r: -r.score)
    logger.info(f"[external_search] {len(results)} résultat(s) retourné(s).")
    return results
