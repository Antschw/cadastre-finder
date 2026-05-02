"""Module d'ingestion des données DPE (Diagnostic de Performance Énergétique) de l'ADEME."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import duckdb
import httpx
from loguru import logger

from cadastre_finder.config import (
    DB_PATH,
    DPE_CSV_PATH,
    DPE_TABLE,
    DPE_URL,
    RAW_ADEME_DIR,
)


def download_dpe_data(output_path: Optional[Path] = None) -> Path:
    """Télécharge le fichier DPE de l'ADEME s'il n'existe pas déjà."""
    if output_path is None:
        output_path = DPE_CSV_PATH

    if output_path.exists() and output_path.stat().st_size > 0:
        size_mb = output_path.stat().st_size / 1e6
        logger.info(f"[dpe] Fichier déjà présent : {output_path} ({size_mb:.0f} Mo)")
        return output_path

    logger.info(f"Téléchargement des données DPE depuis {DPE_URL}...")
    RAW_ADEME_DIR.mkdir(parents=True, exist_ok=True)

    tmp = output_path.with_suffix(output_path.suffix + ".part")
    resume_offset = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={resume_offset}-"} if resume_offset else {}

    with httpx.stream("GET", DPE_URL, headers=headers, follow_redirects=True, timeout=600.0) as r:
        if r.status_code == 404:
            raise RuntimeError(
                f"URL DPE introuvable (404) : {DPE_URL}\n"
                "Vérifiez le dataset sur https://data.ademe.fr/datasets/dpe03existant"
            )
        r.raise_for_status()

        content_type = r.headers.get("content-type", "")
        if "json" in content_type and "csv" not in content_type:
            raise RuntimeError(
                f"La réponse ADEME est du JSON au lieu d'un CSV (content-type: {content_type}).\n"
                f"Vérifiez l'URL : {DPE_URL}"
            )

        remaining = int(r.headers.get("content-length", 0))
        total = remaining + resume_offset
        mode = "ab" if resume_offset else "wb"
        downloaded = resume_offset
        last_log = time.monotonic()

        with open(tmp, mode) as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
                downloaded += len(chunk)
                now = time.monotonic()
                if now - last_log >= 10:
                    if total:
                        pct = 100 * downloaded / total
                        logger.info(
                            f"[dpe] Téléchargement : {downloaded / 1e6:.0f} / {total / 1e6:.0f} Mo "
                            f"({pct:.1f} %)"
                        )
                    else:
                        logger.info(f"[dpe] Téléchargement : {downloaded / 1e6:.0f} Mo reçus...")
                    last_log = now

    tmp.rename(output_path)
    size_mb = output_path.stat().st_size / 1e6
    logger.info(f"Téléchargement DPE terminé : {output_path} ({size_mb:.0f} Mo)")
    return output_path


def load_dpe_to_duckdb(csv_path: Path, db_path: Path = DB_PATH) -> None:
    """Importe le CSV DPE dans DuckDB.

    Active la barre de progression native de DuckDB (affichée sur stdout, mise à jour
    en continu à mesure que les bytes du CSV sont lus). Pour 28 Go de CSV, ça prend
    plusieurs minutes : la barre est indispensable.
    """
    size_mb = csv_path.stat().st_size / 1e6
    logger.info(f"[dpe] Importation de {csv_path.name} ({size_mb:,.0f} Mo) dans {db_path} (table {DPE_TABLE})...")
    logger.info("[dpe] La barre de progression DuckDB s'affiche sur stdout (peut prendre 1-2 s à apparaître).")

    t0 = time.monotonic()
    con = duckdb.connect(str(db_path))
    try:
        # Barre de progression native DuckDB (affichage console, granularité ligne)
        con.execute("PRAGMA enable_progress_bar = true")
        con.execute("PRAGMA progress_bar_time = 1000")  # ms avant affichage

        con.execute(f"DROP TABLE IF EXISTS {DPE_TABLE}")

        con.execute(f"""
            CREATE TABLE {DPE_TABLE} AS
            SELECT * FROM read_csv_auto('{csv_path}',
                ignore_errors=True,
                sample_size=20000)
        """)

        elapsed = time.monotonic() - t0
        logger.info(f"[dpe] CSV importé en {elapsed:.0f}s.")

        logger.info("[dpe] Création des index...")
        con.execute(f"CREATE INDEX idx_dpe_commune ON {DPE_TABLE} (code_insee_ban)")
        con.execute(f"CREATE INDEX idx_dpe_surface ON {DPE_TABLE} (surface_habitable_logement)")

        count = con.execute(f"SELECT COUNT(*) FROM {DPE_TABLE}").fetchone()[0]
        logger.info(f"[dpe] Importation réussie : {count:,} enregistrements.")

    finally:
        con.close()


if __name__ == "__main__":
    path = download_dpe_data()
    load_dpe_to_duckdb(path)
