#!/usr/bin/env python3
"""Script d'ingestion complète des 20 départements du périmètre.

Usage :
    python scripts/ingest_all.py [--dry-run] [--db chemin.duckdb]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Ajouter src/ au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from loguru import logger
from cadastre_finder.config import DB_PATH, DEPARTMENTS
from cadastre_finder.ingestion.cadastre import load_department_to_duckdb


# Taille approximative par département (GeoJSON compressé, en Mo)
# Basé sur des données réelles ; les grands depts peuvent dépasser ces valeurs
APPROX_SIZE_MB = {
    "76": 80, "27": 55, "14": 65, "50": 70, "61": 55,
    "28": 45, "72": 60, "53": 40, "35": 75, "22": 65,
    "29": 80, "56": 65, "44": 80, "49": 65, "85": 60,
    "79": 50, "86": 55, "37": 60, "41": 55, "45": 65,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestion cadastre — 20 départements")
    parser.add_argument("--dry-run", action="store_true", help="Lister sans télécharger")
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    parser.add_argument("--depts", nargs="*", help="Sous-ensemble de départements")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    level = "DEBUG" if args.verbose else "INFO"
    logger.add(sys.stderr, level=level,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")

    depts = args.depts if args.depts else DEPARTMENTS
    total_mb = sum(APPROX_SIZE_MB.get(d, 60) for d in depts)

    logger.info(f"Périmètre : {len(depts)} département(s)")
    logger.info(f"Taille disque estimée (raw) : ~{total_mb} Mo")
    logger.info(f"Base DuckDB : {args.db}")

    if args.dry_run:
        logger.info("\n--- Mode dry-run : liste des départements ---")
        for d in depts:
            mb = APPROX_SIZE_MB.get(d, 60)
            logger.info(f"  Dept {d.zfill(2)} : ~{mb} Mo")
        return 0

    db_path = Path(args.db)
    success, errors = [], []
    t_start = time.monotonic()

    for i, dept in enumerate(depts, 1):
        logger.info(f"\n[{i}/{len(depts)}] === Département {dept.zfill(2)} ===")
        t0 = time.monotonic()
        try:
            load_department_to_duckdb(dept, db_path=db_path)
            elapsed = time.monotonic() - t0
            logger.info(f"[{dept}] OK en {elapsed:.1f}s")
            success.append(dept)
        except Exception as e:
            logger.error(f"[{dept}] ERREUR : {e}")
            errors.append((dept, str(e)))

    total_elapsed = time.monotonic() - t_start
    logger.info(f"\n=== Bilan ===")
    logger.info(f"Succès : {len(success)}/{len(depts)} département(s) en {total_elapsed:.0f}s")
    if errors:
        logger.error(f"Erreurs ({len(errors)}) :")
        for dept, err in errors:
            logger.error(f"  Dept {dept} : {err}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
