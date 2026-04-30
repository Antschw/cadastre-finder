"""Module d'ingestion des données DPE (Diagnostic de Performance Énergétique) de l'ADEME."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import duckdb
import requests
from loguru import logger

from cadastre_finder.config import DB_PATH, DPE_TABLE, DPE_URL, DATA_RAW


def download_dpe_data(output_path: Optional[Path] = None) -> Path:
    """Télécharge le fichier DPE de l'ADEME s'il n'existe pas déjà."""
    if output_path is None:
        output_path = DATA_RAW / "dpe-existants.csv"
    
    if output_path.exists():
        logger.info(f"Le fichier DPE existe déjà : {output_path}")
        return output_path

    logger.info(f"Téléchargement des données DPE depuis {DPE_URL}...")
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    
    response = requests.get(DPE_URL, stream=True)
    response.raise_for_status()
    
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    logger.info(f"Téléchargement terminé : {output_path}")
    return output_path


def load_dpe_to_duckdb(csv_path: Path, db_path: Path = DB_PATH) -> None:
    """Importe le CSV DPE dans DuckDB."""
    logger.info(f"Importation du DPE dans {db_path} (table {DPE_TABLE})...")
    
    con = duckdb.connect(str(db_path))
    try:
        # On utilise read_csv_auto pour laisser DuckDB détecter les types
        # On sélectionne les colonnes utiles pour optimiser la taille en base
        # Note: les noms de colonnes peuvent varier légèrement selon les exports ADEME.
        # Ici on utilise les noms standards de la V2.
        
        con.execute(f"DROP TABLE IF EXISTS {DPE_TABLE}")
        
        # Création de la table par échantillonnage du CSV
        # On filtre potentiellement sur les départements d'intérêt si le CSV est trop gros,
        # mais ici on importe tout et on pourra filtrer à la requête.
        con.execute(f"""
            CREATE TABLE {DPE_TABLE} AS 
            SELECT * FROM read_csv_auto('{csv_path}', 
                ignore_errors=True, 
                sample_size=20000)
        """)
        
        # Création d'index pour accélérer les recherches
        logger.info("Création des index sur la table DPE...")
        con.execute(f"CREATE INDEX idx_dpe_commune ON {DPE_TABLE} (code_insee_commune_actualisé)")
        con.execute(f"CREATE INDEX idx_dpe_surface ON {DPE_TABLE} (surface_habitable_logement)")
        
        count = con.execute(f"SELECT COUNT(*) FROM {DPE_TABLE}").fetchone()[0]
        logger.info(f"Importation réussie : {count} enregistrements DPE.")
        
    finally:
        con.close()


if __name__ == "__main__":
    # Test rapide ou usage script
    path = download_dpe_data()
    load_dpe_to_duckdb(path)
