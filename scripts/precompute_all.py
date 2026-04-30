#!/usr/bin/env python3
"""
Script de pré-calcul massif des adjacences (communes et parcelles)
pour tous les départements présents dans la base de données.
"""
from pathlib import Path
from cadastre_finder.config import DB_PATH
from cadastre_finder.processing.adjacency import build_adjacency_table
from cadastre_finder.processing.parcel_adjacency import build_parcel_adjacency
from loguru import logger

def main():
    db_path = DB_PATH
    if not db_path.exists():
        logger.error(f"La base de données {db_path} n'existe pas. Veuillez d'abord ingérer des données.")
        return

    logger.info("=== Début du pré-calcul des adjacences ===")

    # 1. Adjacences entre communes
    logger.info("Étape 1 : Calcul des adjacences entre communes...")
    build_adjacency_table(db_path=db_path, include_rank2=True)

    # 2. Adjacences entre parcelles
    logger.info("Étape 2 : Calcul des adjacences entre parcelles (tous départements)...")
    build_parcel_adjacency(db_path=db_path, force=False)

    logger.info("=== Pré-calculs terminés avec succès ===")

if __name__ == "__main__":
    main()
