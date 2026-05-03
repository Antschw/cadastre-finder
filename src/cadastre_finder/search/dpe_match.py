"""Moteur de recherche — Croisement avec les données DPE de l'ADEME."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from cadastre_finder.config import DB_PATH, DPE_TABLE
from cadastre_finder.search.models import ParcelMatch
from cadastre_finder.utils.geocoding import geocode_address, resolve_commune


def search_dpe(
    commune: str,
    living_surface: float,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    tolerance_pct: float = 5.0,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """Recherche des enregistrements DPE correspondant aux critères.
    
    Retourne une liste de dicts contenant les infos DPE et l'adresse.
    """
    res = resolve_commune(commune, db_path=db_path)
    if not res.best:
        return []
    
    code_insee = res.best.code_insee
    delta = living_surface * tolerance_pct / 100.0
    surf_min = living_surface - delta
    surf_max = living_surface + delta

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Vérifie si la table existe
        table_exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{DPE_TABLE}'"
        ).fetchone()[0]
        if not table_exists:
            logger.warning(f"Table {DPE_TABLE} absente. Lancez l'ingestion DPE.")
            return []

        query = f"""
            SELECT
                adresse_brut, code_postal_brut, nom_commune_brut,
                surface_habitable_logement, etiquette_dpe, etiquette_ges,
                date_etablissement_dpe
            FROM {DPE_TABLE}
            WHERE code_insee_ban = ?
              AND surface_habitable_logement BETWEEN ? AND ?
        """
        params = [code_insee, surf_min, surf_max]

        if dpe_label:
            query += " AND etiquette_dpe = ?"
            params.append(dpe_label)
        if ges_label:
            query += " AND etiquette_ges = ?"
            params.append(ges_label)

        query += " ORDER BY date_etablissement_dpe DESC LIMIT 10"
        
        rows = con.execute(query, params).fetchall()
        
        results = []
        for r in rows:
            results.append({
                "address": r[0],
                "postcode": r[1],
                "city": r[2],
                "surface": r[3],
                "dpe": r[4],
                "ges": r[5],
                "date": r[6]
            })
        return results
    except Exception as e:
        logger.error(f"[dpe_match] Erreur lors de la recherche DPE : {e}")
        return []
    finally:
        con.close()


def find_parcel_for_dpe(dpe_record: dict, db_path: Path = DB_PATH) -> Optional[ParcelMatch]:
    """Trouve la parcelle cadastrale correspondant à un enregistrement DPE via géo-codage."""
    full_address = f"{dpe_record['address']}, {dpe_record['postcode']} {dpe_record['city']}"
    coords = geocode_address(full_address, city=dpe_record['city'], postcode=dpe_record['postcode'])
    
    if not coords:
        return None
    
    lat, lon = coords
    
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        # Trouve la parcelle qui contient ces coordonnées
        row = con.execute("""
            SELECT 
                id, code_insee, contenance,
                ST_X(ST_Centroid(geometry)) AS lon,
                ST_Y(ST_Centroid(geometry)) AS lat,
                ST_AsGeoJSON(geometry)      AS geojson
            FROM parcelles
            WHERE ST_Intersects(geometry, ST_Point(?, ?))
            LIMIT 1
        """, [lon, lat]).fetchone()
        
        if row:
            # Récupère le nom de la commune
            nom_commune = con.execute(
                "SELECT nom FROM communes WHERE code_insee = ?", [row[1]]
            ).fetchone()
            nom_commune = nom_commune[0] if nom_commune else row[1]
            
            return ParcelMatch(
                id_parcelle=row[0],
                code_insee=row[1],
                nom_commune=nom_commune,
                contenance=row[2],
                centroid_lon=row[3],
                centroid_lat=row[4],
                geometry_geojson=row[5],
                score=100.0,  # Match DPE = haute confiance
                dpe_label=dpe_record['dpe'],
                ges_label=dpe_record['ges']
            )
    except Exception as e:
        logger.error(f"[dpe_match] Erreur spatial mapping : {e}")
    finally:
        con.close()
    
    return None
