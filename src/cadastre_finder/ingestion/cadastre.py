"""Module d'ingestion des données cadastre Etalab dans DuckDB.

Usage CLI : python -m cadastre_finder.ingestion.cadastre --dept 61
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import duckdb
import httpx
from loguru import logger
from tqdm import tqdm

from cadastre_finder.config import (
    CADASTRE_BASE_URL,
    DB_PATH,
    RAW_CADASTRE_COMMUNES_DIR,
    RAW_CADASTRE_PARCELLES_DIR,
)


def _cadastre_url(dept: str, kind: str) -> str:
    """Construit l'URL de téléchargement pour un département et un type de fichier."""
    dept_padded = dept.zfill(2)
    return (
        f"{CADASTRE_BASE_URL}/latest/geojson/departements/"
        f"{dept_padded}/cadastre-{dept_padded}-{kind}.json.gz"
    )


def _cadastre_path(dept: str, kind: str) -> Path:
    """Retourne le chemin local attendu pour un fichier cadastre."""
    if kind == "communes":
        return RAW_CADASTRE_COMMUNES_DIR / f"cadastre-{dept}-communes.json.gz"
    if kind == "parcelles":
        return RAW_CADASTRE_PARCELLES_DIR / f"cadastre-{dept}-parcelles.json.gz"
    raise ValueError(f"Type de fichier cadastre inconnu : {kind}")


def download_department(dept_code: str) -> dict[str, Path]:
    """Télécharge les fichiers GeoJSON cadastre (parcelles + communes) pour un département.

    Returns:
        dict avec les clés 'parcelles' et 'communes' pointant vers les fichiers locaux.
    """
    RAW_CADASTRE_COMMUNES_DIR.mkdir(parents=True, exist_ok=True)
    RAW_CADASTRE_PARCELLES_DIR.mkdir(parents=True, exist_ok=True)
    dept = dept_code.zfill(2)
    files: dict[str, Path] = {}

    for kind in ("parcelles", "communes"):
        dest = _cadastre_path(dept, kind)

        if dest.exists():
            files[kind] = dest
            continue

        url = _cadastre_url(dept, kind)
        logger.info(f"[{dept}] Téléchargement {kind} depuis {url}")
        tmp = dest.with_suffix(".tmp")
        try:
            with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(tmp, "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True,
                    desc=f"{dept}-{kind}", leave=False
                ) as bar:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)
                        bar.update(len(chunk))
            tmp.rename(dest)
            logger.info(f"[{dept}] {kind} téléchargé → {dest}")
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        files[kind] = dest

    return files


def _load_geojson_gz(path: Path) -> dict:
    """Décompresse et parse un fichier GeoJSON.gz."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def load_department_to_duckdb(
    dept_code: str,
    db_path: Path = DB_PATH,
) -> None:
    """Télécharge (si nécessaire) et charge les données d'un département dans DuckDB.

    Tables créées/mises à jour :
    - parcelles (id, code_insee, code_dept, prefixe, section, numero, contenance, geometry)
    - communes  (code_insee, nom, code_dept, geometry)
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    dept = dept_code.zfill(2)

    files = download_department(dept)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        _ensure_tables(con)

        # Vérification idempotence
        existing = con.execute(
            "SELECT COUNT(*) FROM parcelles WHERE code_dept = ?", [dept]
        ).fetchone()[0]
        if existing > 0:
            logger.info(f"[{dept}] Déjà chargé ({existing} parcelles). Skip.")
            return

        logger.info(f"[{dept}] Chargement des parcelles...")
        _load_parcelles(con, dept, files["parcelles"])

        logger.info(f"[{dept}] Chargement des communes...")
        _load_communes(con, dept, files["communes"])

        logger.info(f"[{dept}] Chargement terminé.")
    finally:
        con.close()


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Crée les tables et index si inexistants."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS parcelles (
            id          VARCHAR PRIMARY KEY,
            code_insee  VARCHAR NOT NULL,
            code_dept   VARCHAR NOT NULL,
            prefixe     VARCHAR,
            section     VARCHAR,
            numero      VARCHAR,
            contenance  INTEGER,
            geometry    GEOMETRY
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS communes (
            code_insee  VARCHAR PRIMARY KEY,
            nom         VARCHAR,
            code_dept   VARCHAR NOT NULL,
            geometry    GEOMETRY
        )
    """)
    # Index composite pour les requêtes métier
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcelles_insee_contenance
        ON parcelles (code_insee, contenance)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcelles_dept
        ON parcelles (code_dept)
    """)


def _load_parcelles(
    con: duckdb.DuckDBPyConnection, dept: str, gz_path: Path
) -> None:
    """Insère les parcelles depuis le GeoJSON dans DuckDB."""
    import pandas as pd

    data = _load_geojson_gz(gz_path)
    features = data.get("features", [])

    rows = []
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        geom_wkt = _geom_to_wkt(geom)

        rows.append({
            "id": props.get("id", ""),
            "code_insee": props.get("commune", ""),
            "code_dept": dept,
            "prefixe": props.get("prefixe", ""),
            "section": props.get("section", ""),
            "numero": props.get("numero", ""),
            "contenance": props.get("contenance"),
            "geometry_wkt": geom_wkt,
        })

    if not rows:
        logger.warning(f"[{dept}] Aucune parcelle trouvée dans {gz_path}")
        return

    df = pd.DataFrame(rows)
    con.register("_staging_parcelles", df)
    con.execute("""
        INSERT OR IGNORE INTO parcelles
        (id, code_insee, code_dept, prefixe, section, numero, contenance, geometry)
        SELECT id, code_insee, code_dept, prefixe, section, numero, contenance,
               CASE WHEN geometry_wkt IS NOT NULL THEN ST_GeomFromText(geometry_wkt) END
        FROM _staging_parcelles
    """)
    con.unregister("_staging_parcelles")
    # Log plus discret supprimé ici car build_all gère maintenant le log par département


def _load_communes(
    con: duckdb.DuckDBPyConnection, dept: str, gz_path: Path
) -> None:
    """Insère les communes depuis le GeoJSON dans DuckDB."""
    import pandas as pd

    data = _load_geojson_gz(gz_path)
    features = data.get("features", [])

    rows = []
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        geom_wkt = _geom_to_wkt(geom)

        rows.append({
            "code_insee": props.get("id", props.get("insee", "")),
            "nom": props.get("nom", ""),
            "code_dept": dept,
            "geometry_wkt": geom_wkt,
        })

    if not rows:
        logger.warning(f"[{dept}] Aucune commune trouvée dans {gz_path}")
        return

    df = pd.DataFrame(rows)
    con.register("_staging_communes", df)
    con.execute("""
        INSERT OR IGNORE INTO communes (code_insee, nom, code_dept, geometry)
        SELECT code_insee, nom, code_dept,
               CASE WHEN geometry_wkt IS NOT NULL THEN ST_GeomFromText(geometry_wkt) END
        FROM _staging_communes
    """)
    con.unregister("_staging_communes")
    # Log plus discret supprimé ici car build_all gère maintenant le log par département


def _geom_to_wkt(geom: dict | None) -> str | None:
    """Convertit un objet GeoJSON geometry en WKT minimal via Shapely."""
    if geom is None:
        return None
    try:
        from shapely.geometry import shape
        return shape(geom).wkt
    except Exception:
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingestion cadastre Etalab → DuckDB")
    parser.add_argument("--dept", required=True, help="Code département (ex: 61)")
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    args = parser.parse_args()

    load_department_to_duckdb(args.dept, db_path=Path(args.db))
