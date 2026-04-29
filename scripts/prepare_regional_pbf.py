#!/usr/bin/env python3
"""Extrait un PBF régional couvrant les 20 départements du périmètre.

france-latest.osm.pbf (~5 Go) → ouest-france.osm.pbf (~400 Mo)

Usage :
    python scripts/prepare_regional_pbf.py
    python scripts/prepare_regional_pbf.py --pbf data/raw/france-latest.osm.pbf
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Bounding box englobant les 20 départements (WGS84 : lon_min,lat_min,lon_max,lat_max)
# 29 Finistère (ouest) → 45 Loiret (est)  /  85 Vendée (sud) → 76 Seine-Maritime (nord)
BBOX = "-5.20,46.00,2.80,50.10"

DEFAULT_IN  = Path("data/raw/france-latest.osm.pbf")
DEFAULT_OUT = Path("data/raw/ouest-france.osm.pbf")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extraction PBF régional")
    parser.add_argument("--pbf", default=str(DEFAULT_IN),  help="PBF source")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="PBF de sortie")
    args = parser.parse_args()

    pbf_in  = Path(args.pbf)
    pbf_out = Path(args.out)

    if not pbf_in.exists():
        print(f"ERREUR : fichier source introuvable : {pbf_in}", file=sys.stderr)
        return 1

    if pbf_out.exists():
        print(f"Déjà présent : {pbf_out}  ({pbf_out.stat().st_size / 1e6:.0f} Mo)")
        return 0

    pbf_out.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "osmium", "extract",
        "--bbox", BBOX,
        str(pbf_in),
        "-o", str(pbf_out),
        "--strategy=complete-ways",   # inclure les ways complets qui croisent la bbox
        "--overwrite",
    ]
    print(f"Extraction bbox {BBOX}")
    print(f"  entrée : {pbf_in}  ({pbf_in.stat().st_size / 1e6:.0f} Mo)")
    print(f"  sortie : {pbf_out}")
    print("  (quelques minutes…)")

    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        print("ERREUR osmium extract", file=sys.stderr)
        return 1

    size_mb = pbf_out.stat().st_size / 1e6
    print(f"OK — {pbf_out}  ({size_mb:.0f} Mo)")
    print(f"\nUtilisez maintenant :")
    print(f"  cadastre-finder ingest-osm --pbf {pbf_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
