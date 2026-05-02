"""Orchestrateur d'ingestion globale.

Une seule commande pour construire l'intégralité de la base de données :
    1. Vérification des PBF régionaux Geofabrik (téléchargement uniquement de ce qui manque)
    2. Fusion + extraction d'un PBF couvrant les 21 départements via `osmium merge` / `osmium extract`
    3. Ingestion cadastre Etalab pour les 21 départements
    4. Ingestion OSM (POI, routes, hydrographie, bâtiments)
    5. Ingestion DPE ADEME
    6. Pré-calculs : adjacence des communes + adjacence des parcelles

Caractéristiques :
    - Optimisations DuckDB : threads=N_CPU, memory_limit configurable,
      temp_directory sur le volume de la DB (NVMe).
    - Idempotence : reprise sans recommencer ce qui est déjà fait.
    - Progression visible via `tqdm` aux étapes longues.

Usage :
    python -m cadastre_finder.cli build-database
    python -m cadastre_finder.cli build-database --skip-osm --skip-dpe
"""
from __future__ import annotations

import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import duckdb
import httpx
from loguru import logger
from tqdm import tqdm

from cadastre_finder.config import (
    DB_PATH,
    DEPARTMENTS,
    RAW_CADASTRE_COMMUNES_DIR,
    RAW_CADASTRE_PARCELLES_DIR,
    RAW_OSM_DIR,
)
from cadastre_finder.ingestion.cadastre import (
    download_department,
    load_department_to_duckdb,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Anciennes régions administratives (les fichiers Geofabrik existent toujours sous ces noms)
# qui couvrent les 21 départements du périmètre.
GEOFABRIK_BASE = "https://download.geofabrik.de/europe/france"
GEOFABRIK_REGIONS = (
    "bretagne",          # 22, 29, 35, 56
    "basse-normandie",   # 14, 50, 61
    "haute-normandie",   # 27, 76
    "pays-de-la-loire",  # 44, 49, 53, 72, 85
    "centre",            # 28, 37, 41, 45
    "poitou-charentes",  # 79, 86
    "picardie",          # 60 (Oise uniquement)
)

# Bounding box englobant les 21 départements (lon_min,lat_min,lon_max,lat_max)
# Étendue à l'est pour inclure la Picardie/Oise (~3.2°E max)
PERIMETER_BBOX = "-5.20,46.00,3.30,50.10"

# Téléchargements cadastre en parallèle (I/O bound, filet de sécurité uniquement).
CADASTRE_DOWNLOAD_WORKERS = 8


def _default_threads() -> int:
    return os.cpu_count() or 8


# ---------------------------------------------------------------------------
# Téléchargement HTTP avec reprise (utilisé uniquement si un fichier manque)
# ---------------------------------------------------------------------------

def _download_with_resume(
    url: str,
    dest: Path,
    chunk_size: int = 1 << 20,  # 1 MiB
) -> Path:
    """Télécharge `url` vers `dest` avec reprise (HTTP Range)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    if dest.exists() and dest.stat().st_size > 0:
        return dest

    resume_offset = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={resume_offset}-"} if resume_offset else {}

    with httpx.stream(
        "GET", url, headers=headers, follow_redirects=True, timeout=600.0
    ) as r:
        r.raise_for_status()
        remaining = int(r.headers.get("content-length", 0))
        total = remaining + resume_offset
        mode = "ab" if resume_offset else "wb"
        downloaded = resume_offset

        with open(tmp, mode) as f, tqdm(
            total=total, initial=downloaded, unit="B", unit_scale=True,
            desc=dest.name, leave=False
        ) as bar:
            for chunk in r.iter_bytes(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                bar.update(len(chunk))

    tmp.rename(dest)
    size_mb = dest.stat().st_size / 1e6
    logger.info(f"[download] OK : {dest.name} ({size_mb:.0f} Mo)")
    return dest


# ---------------------------------------------------------------------------
# Étape OSM : validation des PBF régionaux + merge + extract
# ---------------------------------------------------------------------------

def _check_osmium() -> str:
    for cmd in ("osmium", "osmium-tool"):
        if shutil.which(cmd) is not None:
            return cmd
    raise RuntimeError(
        "osmium-tool est requis. "
        "Installation : sudo dnf install osmium-tool  (ou apt install osmium-tool)"
    )


def _validate_pbf(path: Path, osmium_cmd: str) -> bool:
    """Vérifie qu'un fichier PBF est lisible par osmium. Retourne False si corrompu."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        r = subprocess.run(
            [osmium_cmd, "fileinfo", str(path)],
            capture_output=True, timeout=120
        )
        return r.returncode == 0
    except Exception:
        return False


def prepare_regional_pbf(
    osm_dir: Path = RAW_OSM_DIR,
    keep_intermediate: bool = True,
) -> Path:
    """Produit un PBF couvrant les 21 départements à partir des PBF régionaux.

    Stratégie :
        1. Valide les PBF régionaux déjà présents dans `osm_dir`.
        2. Télécharge ce qui manque (filet de sécurité — l'utilisateur les a normalement déjà).
        3. `osmium merge` → un seul PBF.
        4. `osmium extract --bbox` → ne garde que la zone d'intérêt.

    Retourne le chemin du PBF régional final.
    """
    osmium = _check_osmium()
    osm_dir.mkdir(parents=True, exist_ok=True)

    final_pbf = osm_dir / "ouest-france.osm.pbf"
    if final_pbf.exists():
        if _validate_pbf(final_pbf, osmium):
            size_mb = final_pbf.stat().st_size / 1e6
            logger.info(f"[osm] PBF final déjà présent et valide : {final_pbf} ({size_mb:.0f} Mo)")
            return final_pbf
        logger.warning("[osm] PBF final corrompu, reconstruction...")
        final_pbf.unlink()

    # 1. Vérification des PBF régionaux (l'utilisateur les a normalement déjà)
    region_files: list[Path] = []
    missing: list[str] = []
    for region in GEOFABRIK_REGIONS:
        path = osm_dir / f"{region}-latest.osm.pbf"
        if _validate_pbf(path, osmium):
            logger.info(f"[osm] OK : {path.name} ({path.stat().st_size / 1e6:.0f} Mo)")
            region_files.append(path)
        else:
            missing.append(region)

    # 2. Téléchargement uniquement de ce qui manque
    if missing:
        logger.info(f"[osm] Téléchargement de {len(missing)} régions manquantes : {missing}")
        with ThreadPoolExecutor(max_workers=4) as exe:
            futs = {
                exe.submit(
                    _download_with_resume,
                    f"{GEOFABRIK_BASE}/{region}-latest.osm.pbf",
                    osm_dir / f"{region}-latest.osm.pbf",
                ): region
                for region in missing
            }
            for fut in as_completed(futs):
                region = futs[fut]
                path = fut.result()
                if not _validate_pbf(path, osmium):
                    raise RuntimeError(f"[osm] Re-téléchargement de {region} toujours invalide : {path}")
                region_files.append(path)

    # 3. Merge → un seul PBF
    merged = osm_dir / "_merged-regions.osm.pbf"
    if merged.exists() and not _validate_pbf(merged, osmium):
        logger.warning("[osm] Fichier fusionné corrompu, suppression...")
        merged.unlink()
    if not merged.exists():
        logger.info(f"[osm] osmium merge des {len(region_files)} PBF régionaux...")
        cmd = [osmium, "merge", *(str(p) for p in region_files), "-o", str(merged), "--overwrite"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if r.returncode != 0:
            raise RuntimeError(f"osmium merge a échoué : {r.stderr}")
        logger.info(f"[osm] Merge OK : {merged} ({merged.stat().st_size / 1e6:.0f} Mo)")

    # 4. Extract bbox
    logger.info(f"[osm] osmium extract bbox={PERIMETER_BBOX}")
    cmd = [
        osmium, "extract",
        "--bbox", PERIMETER_BBOX,
        str(merged),
        "-o", str(final_pbf),
        "--strategy=complete_ways",
        "--overwrite",
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if r.returncode != 0:
        raise RuntimeError(f"osmium extract a échoué : {r.stderr}")
    logger.info(f"[osm] Extract OK : {final_pbf} ({final_pbf.stat().st_size / 1e6:.0f} Mo)")

    # 5. Nettoyage du fichier intermédiaire merged si demandé
    if not keep_intermediate:
        merged.unlink(missing_ok=True)
        logger.info("[osm] Fichier merged supprimé (libère ~1.5 Go).")

    return final_pbf


# ---------------------------------------------------------------------------
# Étape cadastre : vérification présence + chargement séquentiel
# ---------------------------------------------------------------------------

def _ensure_cadastre_files(depts: Iterable[str], workers: int = CADASTRE_DOWNLOAD_WORKERS) -> None:
    """Vérifie la présence des fichiers cadastre et télécharge ceux qui manquent."""
    depts = list(depts)
    missing: list[str] = []
    for dept in depts:
        d = dept.zfill(2)
        comm = RAW_CADASTRE_COMMUNES_DIR / f"cadastre-{d}-communes.json.gz"
        parc = RAW_CADASTRE_PARCELLES_DIR / f"cadastre-{d}-parcelles.json.gz"
        if not comm.exists() or not parc.exists():
            missing.append(dept)

    if not missing:
        logger.info(f"[cadastre] Tous les fichiers présents ({len(depts)} dépts).")
        return

    logger.info(f"[cadastre] Téléchargement de {len(missing)} dépts manquants : {missing}")
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futs = {exe.submit(download_department, d): d for d in missing}
        with tqdm(total=len(missing), desc="Téléchargement cadastre", unit="dept") as bar:
            for fut in as_completed(futs):
                d = futs[fut]
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"[cadastre] Échec téléchargement dept {d} : {e}")
                    raise
                bar.update(1)


def _sequential_load_cadastre(depts: Iterable[str], db_path: Path) -> None:
    """Charge les départements dans DuckDB un par un (single connection)."""
    depts = list(depts)
    for i, dept in enumerate(depts, 1):
        t0 = time.monotonic()
        try:
            load_department_to_duckdb(dept, db_path=db_path)
            # Log plus discret pour éviter les heartbeats massifs
            if i % 5 == 0 or i == len(depts):
                logger.info(f"[cadastre] Progression : {i}/{len(depts)} départements chargés.")
        except Exception as e:
            logger.error(f"[cadastre] dept {dept} ÉCHEC : {e}")
            raise


# ---------------------------------------------------------------------------
# Étape DuckDB : optimisations
# ---------------------------------------------------------------------------

def apply_duckdb_pragmas(
    db_path: Path,
    threads: int | None = None,
    memory_limit: str = "24GB",
) -> None:
    """Applique des PRAGMA d'optimisation (threads, mémoire, temp dir)."""
    threads = threads or _default_threads()
    temp_dir = db_path.parent / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"PRAGMA threads={threads}")
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        con.execute(f"PRAGMA temp_directory='{temp_dir}'")
        logger.info(
            f"[duckdb] PRAGMA appliqués : threads={threads}, "
            f"memory_limit={memory_limit}, temp={temp_dir}"
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

@dataclass
class BuildOptions:
    db_path: Path = DB_PATH
    osm_dir: Path = RAW_OSM_DIR
    departments: list[str] = field(default_factory=lambda: list(DEPARTMENTS))
    skip_cadastre: bool = False
    skip_osm: bool = False
    skip_dpe: bool = False
    skip_adjacency: bool = False
    skip_parcel_adjacency: bool = False
    keep_intermediate_pbf: bool = True
    duckdb_threads: int | None = None
    duckdb_memory_limit: str = "24GB"
    cadastre_download_workers: int = CADASTRE_DOWNLOAD_WORKERS
    parcel_adjacency_workers: int | None = None


def build_database(opts: BuildOptions | None = None) -> dict:
    """Construit l'intégralité de la base de données en une seule commande.

    Étapes (séquentielles) :
        1. Préparation environnement & PRAGMA DuckDB
        2. Cadastre (vérification + chargement)
        3. OSM (validation PBF + merge + extract + ingestion)
        4. DPE
        5. Adjacence communes
        6. Adjacence parcelles

    Returns:
        dict avec un bilan par étape (durée, statut).
    """
    opts = opts or BuildOptions()
    opts.db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("CADASTRE-FINDER — Construction complète de la base de données")
    logger.info("=" * 70)
    logger.info(f"  Base       : {opts.db_path}")
    logger.info(f"  OSM dir    : {opts.osm_dir}")
    logger.info(f"  Dépts      : {len(opts.departments)} ({', '.join(opts.departments)})")
    logger.info(f"  DuckDB     : threads={opts.duckdb_threads or _default_threads()}, "
                f"memory_limit={opts.duckdb_memory_limit}")

    apply_duckdb_pragmas(
        opts.db_path,
        threads=opts.duckdb_threads,
        memory_limit=opts.duckdb_memory_limit,
    )

    bilan: dict[str, dict] = {}
    t_global = time.monotonic()

    # ----- 1. Cadastre ----------------------------------------------------
    if not opts.skip_cadastre:
        logger.info("=== Cadastre ===")
        t0 = time.monotonic()
        try:
            _ensure_cadastre_files(opts.departments, workers=opts.cadastre_download_workers)
            _sequential_load_cadastre(opts.departments, opts.db_path)
            bilan["cadastre"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
        except Exception as e:
            bilan["cadastre"] = {"status": "error", "error": str(e),
                                 "elapsed_s": time.monotonic() - t0}
            raise
    else:
        logger.info("[cadastre] Étape ignorée (--skip-cadastre).")
        bilan["cadastre"] = {"status": "skipped"}

    # ----- 2. OSM ---------------------------------------------------------
    if not opts.skip_osm:
        logger.info("=== OSM ===")
        t0 = time.monotonic()
        try:
            pbf = prepare_regional_pbf(
                osm_dir=opts.osm_dir,
                keep_intermediate=opts.keep_intermediate_pbf,
            )
            from cadastre_finder.ingestion.osm import load_osm_to_duckdb
            load_osm_to_duckdb(pbf_path=pbf, db_path=opts.db_path)
            bilan["osm"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
        except Exception as e:
            bilan["osm"] = {"status": "error", "error": str(e),
                            "elapsed_s": time.monotonic() - t0}
            logger.error(f"[osm] Échec : {e}")
    else:
        logger.info("[osm] Étape ignorée (--skip-osm).")
        bilan["osm"] = {"status": "skipped"}

    # ----- 3. DPE ---------------------------------------------------------
    if not opts.skip_dpe:
        logger.info("=== DPE ===")
        t0 = time.monotonic()
        try:
            from cadastre_finder.ingestion.dpe import (
                download_dpe_data,
                load_dpe_to_duckdb,
            )
            csv_path = download_dpe_data()
            load_dpe_to_duckdb(csv_path, db_path=opts.db_path)
            bilan["dpe"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
        except Exception as e:
            bilan["dpe"] = {"status": "error", "error": str(e),
                            "elapsed_s": time.monotonic() - t0}
            logger.error(f"[dpe] Échec : {e}")
    else:
        logger.info("[dpe] Étape ignorée (--skip-dpe).")
        bilan["dpe"] = {"status": "skipped"}

    # ----- 4. Adjacence communes ------------------------------------------
    if not opts.skip_adjacency:
        logger.info("=== Adjacence communes ===")
        t0 = time.monotonic()
        try:
            from cadastre_finder.processing.adjacency import build_adjacency_table
            build_adjacency_table(db_path=opts.db_path, include_rank2=True)
            bilan["adjacency"] = {"status": "ok",
                                  "elapsed_s": time.monotonic() - t0}
        except Exception as e:
            bilan["adjacency"] = {"status": "error", "error": str(e),
                                  "elapsed_s": time.monotonic() - t0}
            logger.error(f"[adjacency] Échec : {e}")
    else:
        logger.info("[adjacency] Étape ignorée.")
        bilan["adjacency"] = {"status": "skipped"}

    # ----- 5. Adjacence parcelles -----------------------------------------
    if not opts.skip_parcel_adjacency:
        logger.info("=== Adjacence parcelles ===")
        t0 = time.monotonic()
        try:
            from cadastre_finder.processing.parcel_adjacency import build_parcel_adjacency
            build_parcel_adjacency(
                db_path=opts.db_path,
                departments=opts.departments,
                force=False,
                workers=opts.parcel_adjacency_workers,
            )
            bilan["parcel_adjacency"] = {"status": "ok",
                                         "elapsed_s": time.monotonic() - t0}
        except Exception as e:
            bilan["parcel_adjacency"] = {"status": "error", "error": str(e),
                                         "elapsed_s": time.monotonic() - t0}
            logger.error(f"[parcel_adjacency] Échec : {e}")
    else:
        logger.info("[parcel_adjacency] Étape ignorée.")
        bilan["parcel_adjacency"] = {"status": "skipped"}

    # ----- Bilan ----------------------------------------------------------
    total = time.monotonic() - t_global
    logger.info("=" * 70)
    logger.info(f"BILAN — durée totale : {int(total // 60)}m{int(total % 60):02d}s")
    logger.info("=" * 70)
    for stage, info in bilan.items():
        status = info.get("status", "?")
        if status == "ok":
            logger.info(f"  {stage:<22} OK    ({info.get('elapsed_s', 0):.1f}s)")
        elif status == "skipped":
            logger.info(f"  {stage:<22} SKIP")
        else:
            logger.error(
                f"  {stage:<22} ERREUR ({info.get('elapsed_s', 0):.1f}s) — "
                f"{info.get('error', '')}"
            )
    logger.info("=" * 70)

    return bilan
