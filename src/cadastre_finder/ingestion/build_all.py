"""Orchestrateur d'ingestion globale.

Une seule commande pour construire l'intégralité de la base de données :
    1. Téléchargement automatique des PBF régionaux Geofabrik (couverture des 20 départements)
    2. Fusion + extraction d'un PBF régional via `osmium merge` / `osmium extract`
    3. Ingestion cadastre Etalab pour les 20 départements (téléchargements parallèles)
    4. Ingestion OSM (POI, routes, hydrographie, bâtiments)
    5. Ingestion DPE ADEME
    6. Pré-calculs : adjacence des communes + adjacence des parcelles

Caractéristiques :
    - Heartbeat (un log toutes les 60 secondes, jamais > 5 min de silence)
    - Optimisations DuckDB : threads=N_CPU, memory_limit configurable,
      temp_directory sur le volume de la DB (NVMe).
    - Idempotence : reprise sans recommencer ce qui est déjà fait.
    - Logs structurés avec ETA et bilan final.

Usage :
    python -m cadastre_finder.cli build-database
    python -m cadastre_finder.cli build-database --skip-osm --skip-dpe
"""
from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import duckdb
import httpx
from loguru import logger

from cadastre_finder.config import (
    DATA_RAW,
    DB_PATH,
    DEPARTMENTS,
)
from cadastre_finder.ingestion.cadastre import (
    download_department,
    load_department_to_duckdb,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# 5 régions Geofabrik qui couvrent les 20 départements du périmètre
GEOFABRIK_BASE = "https://download.geofabrik.de/europe/france"
GEOFABRIK_REGIONS = (
    "bretagne",
    "normandie",
    "pays-de-la-loire",
    "centre-val-de-loire",
    "nouvelle-aquitaine",  # large, contient 79 et 86
)

# Bounding box englobant les 20 départements (lon_min,lat_min,lon_max,lat_max)
PERIMETER_BBOX = "-5.20,46.00,2.80,50.10"

# Téléchargements cadastre en parallèle (I/O bound).
# 8 connexions concurrentes saturent un débit consommateur sans surcharger les serveurs Etalab.
CADASTRE_DOWNLOAD_WORKERS = 8

# Heartbeat : un message au moins toutes les 60 secondes
HEARTBEAT_INTERVAL_S = 60

# Optimisations DuckDB :
#   - Threads = nombre de cœurs logiques (Ryzen 9 7950X3D = 32)
#   - memory_limit ~ 75 % de la RAM disponible (32 Go DDR5 → 24 Go)
#   - temp_directory sur le volume DB (NVMe Gen5 → I/O massif)
def _default_threads() -> int:
    return os.cpu_count() or 8


# ---------------------------------------------------------------------------
# Heartbeat — thread qui garantit un log régulier
# ---------------------------------------------------------------------------

@dataclass
class _StageState:
    """État partagé entre le thread principal et le heartbeat."""
    stage: str = "init"
    started_at: float = field(default_factory=time.monotonic)
    detail: str = ""
    finished: bool = False


class HeartbeatLogger:
    """Émet périodiquement un log « still alive » pendant les phases longues.

    Usage :
        hb = HeartbeatLogger(interval=60)
        with hb.stage("Téléchargement PBF", "Geofabrik"):
            ...long task...
    """

    def __init__(self, interval: float = HEARTBEAT_INTERVAL_S) -> None:
        self._interval = interval
        self._state = _StageState()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="heartbeat", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            with self._lock:
                if self._state.finished:
                    continue
                stage = self._state.stage
                detail = self._state.detail
                elapsed = time.monotonic() - self._state.started_at
            mins, secs = divmod(int(elapsed), 60)
            logger.info(
                f"[heartbeat] Étape « {stage} » en cours depuis {mins}m{secs:02d}s"
                + (f" — {detail}" if detail else "")
            )

    @contextmanager
    def stage(self, name: str, detail: str = ""):
        with self._lock:
            self._state = _StageState(stage=name, detail=detail)
        try:
            yield self
        finally:
            with self._lock:
                self._state.finished = True

    def update(self, detail: str) -> None:
        """Mise à jour du libellé de détail (sans réinitialiser le chrono)."""
        with self._lock:
            self._state.detail = detail


# ---------------------------------------------------------------------------
# Téléchargement HTTP (resume + heartbeat)
# ---------------------------------------------------------------------------

def _download_with_resume(
    url: str,
    dest: Path,
    hb: HeartbeatLogger | None = None,
    chunk_size: int = 1 << 20,  # 1 MiB
) -> Path:
    """Télécharge `url` vers `dest` avec reprise (HTTP Range) et heartbeat.

    Idempotent : si `dest` existe déjà avec une taille cohérente, ne re-télécharge pas.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    # Si dest existe déjà → on suppose terminé (Geofabrik n'expose pas de checksum simple,
    # mais la cohérence sera vérifiée par osmium plus tard).
    if dest.exists() and dest.stat().st_size > 0:
        size_mb = dest.stat().st_size / 1e6
        logger.info(f"[download] Déjà présent : {dest.name} ({size_mb:.0f} Mo)")
        return dest

    resume_offset = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={resume_offset}-"} if resume_offset else {}

    with httpx.stream(
        "GET", url, headers=headers, follow_redirects=True, timeout=600.0
    ) as r:
        r.raise_for_status()
        # Total = ce qu'il reste à télécharger + ce qui est déjà présent
        remaining = int(r.headers.get("content-length", 0))
        total = remaining + resume_offset
        mode = "ab" if resume_offset else "wb"
        downloaded = resume_offset
        last_log = time.monotonic()

        with open(tmp, mode) as f:
            for chunk in r.iter_bytes(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                now = time.monotonic()
                if hb is not None and (now - last_log) >= 5:
                    pct = (100 * downloaded / total) if total else 0
                    hb.update(
                        f"{dest.name} : {downloaded / 1e6:.0f} / {total / 1e6:.0f} Mo "
                        f"({pct:.1f} %)"
                    )
                    last_log = now

    tmp.rename(dest)
    size_mb = dest.stat().st_size / 1e6
    logger.info(f"[download] OK : {dest.name} ({size_mb:.0f} Mo)")
    return dest


# ---------------------------------------------------------------------------
# Étape OSM : téléchargement Geofabrik + fusion + extraction
# ---------------------------------------------------------------------------

def _check_osmium() -> str:
    for cmd in ("osmium", "osmium-tool"):
        if shutil.which(cmd) is not None:
            return cmd
    raise RuntimeError(
        "osmium-tool est requis. "
        "Installation : sudo dnf install osmium-tool  (ou apt install osmium-tool)"
    )


def prepare_regional_pbf(
    raw_dir: Path = DATA_RAW,
    hb: HeartbeatLogger | None = None,
    keep_intermediate: bool = False,
) -> Path:
    """Télécharge les 5 régions Geofabrik et produit un PBF couvrant les 20 départements.

    Stratégie :
        1. Télécharge en parallèle les 5 .osm.pbf régionaux.
        2. `osmium merge` → un seul PBF.
        3. `osmium extract --bbox` → ne garde que la zone d'intérêt.
        4. Supprime les intermédiaires (sauf si --keep-intermediate).

    Retourne le chemin du PBF régional final.
    """
    osmium = _check_osmium()
    raw_dir.mkdir(parents=True, exist_ok=True)

    final_pbf = raw_dir / "ouest-france.osm.pbf"
    if final_pbf.exists():
        size_mb = final_pbf.stat().st_size / 1e6
        logger.info(f"[osm] PBF régional déjà présent : {final_pbf} ({size_mb:.0f} Mo)")
        return final_pbf

    # 1. Téléchargement parallèle (4 workers max, fichiers volumineux ⇒ I/O réseau)
    hb_local = hb or HeartbeatLogger()
    region_files: list[Path] = []
    with hb_local.stage("Téléchargement PBF régionaux Geofabrik"):
        with ThreadPoolExecutor(max_workers=4) as exe:
            futs = {}
            for region in GEOFABRIK_REGIONS:
                url = f"{GEOFABRIK_BASE}/{region}-latest.osm.pbf"
                dest = raw_dir / f"{region}-latest.osm.pbf"
                futs[exe.submit(_download_with_resume, url, dest, hb_local)] = region
            for fut in as_completed(futs):
                region = futs[fut]
                try:
                    region_files.append(fut.result())
                except Exception as e:
                    logger.error(f"[osm] Échec téléchargement {region} : {e}")
                    raise

    # 2. Merge → un seul PBF
    merged = raw_dir / "_merged-regions.osm.pbf"
    if not merged.exists():
        with hb_local.stage("Fusion PBF (osmium merge)"):
            cmd = [osmium, "merge", *(str(p) for p in region_files), "-o", str(merged), "--overwrite"]
            logger.info(f"[osm] {' '.join(cmd[:3])} … (5 fichiers)")
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
            if r.returncode != 0:
                raise RuntimeError(f"osmium merge a échoué : {r.stderr}")
        logger.info(f"[osm] Merge OK : {merged} ({merged.stat().st_size / 1e6:.0f} Mo)")

    # 3. Extract bbox
    with hb_local.stage("Extraction bbox (osmium extract)"):
        cmd = [
            osmium, "extract",
            "--bbox", PERIMETER_BBOX,
            str(merged),
            "-o", str(final_pbf),
            "--strategy=complete-ways",
            "--overwrite",
        ]
        logger.info(f"[osm] osmium extract bbox={PERIMETER_BBOX}")
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if r.returncode != 0:
            raise RuntimeError(f"osmium extract a échoué : {r.stderr}")
    logger.info(f"[osm] Extract OK : {final_pbf} ({final_pbf.stat().st_size / 1e6:.0f} Mo)")

    # 4. Nettoyage des intermédiaires
    if not keep_intermediate:
        merged.unlink(missing_ok=True)
        for p in region_files:
            p.unlink(missing_ok=True)
        logger.info("[osm] Intermédiaires supprimés (libère ~3 Go).")

    return final_pbf


# ---------------------------------------------------------------------------
# Étape cadastre : téléchargements parallèles + chargement séquentiel
# ---------------------------------------------------------------------------

def _parallel_download_cadastre(
    depts: Iterable[str],
    raw_dir: Path,
    hb: HeartbeatLogger,
    workers: int = CADASTRE_DOWNLOAD_WORKERS,
) -> None:
    """Télécharge en parallèle les .json.gz cadastre des départements."""
    depts = list(depts)
    with hb.stage("Téléchargement cadastre Etalab", f"{len(depts)} dépts"):
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as exe:
            futs = {
                exe.submit(download_department, d, raw_dir): d for d in depts
            }
            for fut in as_completed(futs):
                d = futs[fut]
                try:
                    fut.result()
                    done += 1
                    hb.update(f"{done}/{len(depts)} départements téléchargés")
                except Exception as e:
                    logger.error(f"[cadastre] Échec téléchargement dept {d} : {e}")
                    raise
    logger.info(f"[cadastre] Tous les téléchargements OK ({done}/{len(depts)})")


def _sequential_load_cadastre(
    depts: Iterable[str],
    db_path: Path,
    hb: HeartbeatLogger,
) -> None:
    """Charge les départements dans DuckDB un par un (single connection)."""
    depts = list(depts)
    with hb.stage("Chargement cadastre → DuckDB"):
        for i, dept in enumerate(depts, 1):
            hb.update(f"dept {dept} ({i}/{len(depts)})")
            t0 = time.monotonic()
            try:
                load_department_to_duckdb(dept, db_path=db_path)
                logger.info(
                    f"[cadastre] [{i}/{len(depts)}] dept {dept} OK "
                    f"({time.monotonic() - t0:.1f}s)"
                )
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
    raw_dir: Path = DATA_RAW
    departments: list[str] = field(default_factory=lambda: list(DEPARTMENTS))
    skip_cadastre: bool = False
    skip_osm: bool = False
    skip_dpe: bool = False
    skip_adjacency: bool = False
    skip_parcel_adjacency: bool = False
    keep_intermediate_pbf: bool = False
    duckdb_threads: int | None = None
    duckdb_memory_limit: str = "24GB"
    cadastre_download_workers: int = CADASTRE_DOWNLOAD_WORKERS


def build_database(opts: BuildOptions | None = None) -> dict:
    """Construit l'intégralité de la base de données en une seule commande.

    Étapes (séquentielles) :
        1. Préparation environnement & PRAGMA DuckDB
        2. Cadastre (parallèle download + sequential load)
        3. OSM (download + merge + extract + ingestion)
        4. DPE
        5. Adjacence communes
        6. Adjacence parcelles

    Returns:
        dict avec un bilan par étape (durée, statut).
    """
    opts = opts or BuildOptions()
    opts.db_path.parent.mkdir(parents=True, exist_ok=True)
    opts.raw_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("CADASTRE-FINDER — Construction complète de la base de données")
    logger.info("=" * 70)
    logger.info(f"  Base       : {opts.db_path}")
    logger.info(f"  Raw dir    : {opts.raw_dir}")
    logger.info(f"  Dépts      : {len(opts.departments)} ({', '.join(opts.departments)})")
    logger.info(f"  DuckDB     : threads={opts.duckdb_threads or _default_threads()}, "
                f"memory_limit={opts.duckdb_memory_limit}")

    # PRAGMAs (créent la DB si elle n'existe pas)
    apply_duckdb_pragmas(
        opts.db_path,
        threads=opts.duckdb_threads,
        memory_limit=opts.duckdb_memory_limit,
    )

    hb = HeartbeatLogger(interval=HEARTBEAT_INTERVAL_S)
    hb.start()
    bilan: dict[str, dict] = {}
    t_global = time.monotonic()

    try:
        # ----- 1. Cadastre ------------------------------------------------
        if not opts.skip_cadastre:
            t0 = time.monotonic()
            try:
                _parallel_download_cadastre(
                    opts.departments, opts.raw_dir, hb,
                    workers=opts.cadastre_download_workers,
                )
                _sequential_load_cadastre(opts.departments, opts.db_path, hb)
                bilan["cadastre"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
            except Exception as e:
                bilan["cadastre"] = {"status": "error", "error": str(e),
                                     "elapsed_s": time.monotonic() - t0}
                raise
        else:
            logger.info("[cadastre] Étape ignorée (--skip-cadastre).")
            bilan["cadastre"] = {"status": "skipped"}

        # ----- 2. OSM -----------------------------------------------------
        if not opts.skip_osm:
            t0 = time.monotonic()
            try:
                pbf = prepare_regional_pbf(
                    raw_dir=opts.raw_dir,
                    hb=hb,
                    keep_intermediate=opts.keep_intermediate_pbf,
                )
                with hb.stage("Ingestion OSM → DuckDB"):
                    from cadastre_finder.ingestion.osm import load_osm_to_duckdb
                    load_osm_to_duckdb(pbf_path=pbf, db_path=opts.db_path)
                bilan["osm"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
            except Exception as e:
                bilan["osm"] = {"status": "error", "error": str(e),
                                "elapsed_s": time.monotonic() - t0}
                logger.error(f"[osm] Échec : {e}")
                # On continue les autres étapes : OSM n'est pas bloquant pour la suite.
        else:
            logger.info("[osm] Étape ignorée (--skip-osm).")
            bilan["osm"] = {"status": "skipped"}

        # ----- 3. DPE -----------------------------------------------------
        if not opts.skip_dpe:
            t0 = time.monotonic()
            try:
                with hb.stage("Ingestion DPE ADEME"):
                    from cadastre_finder.ingestion.dpe import (
                        download_dpe_data,
                        load_dpe_to_duckdb,
                    )
                    csv_path = download_dpe_data()
                    hb.update(f"chargement {csv_path.name}")
                    load_dpe_to_duckdb(csv_path, db_path=opts.db_path)
                bilan["dpe"] = {"status": "ok", "elapsed_s": time.monotonic() - t0}
            except Exception as e:
                bilan["dpe"] = {"status": "error", "error": str(e),
                                "elapsed_s": time.monotonic() - t0}
                logger.error(f"[dpe] Échec : {e}")
                # Non bloquant
        else:
            logger.info("[dpe] Étape ignorée (--skip-dpe).")
            bilan["dpe"] = {"status": "skipped"}

        # ----- 4. Adjacence communes --------------------------------------
        if not opts.skip_adjacency:
            t0 = time.monotonic()
            try:
                with hb.stage("Adjacence communes"):
                    from cadastre_finder.processing.adjacency import (
                        build_adjacency_table,
                    )
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

        # ----- 5. Adjacence parcelles -------------------------------------
        if not opts.skip_parcel_adjacency:
            t0 = time.monotonic()
            try:
                with hb.stage("Adjacence parcelles", "Shapely STRtree"):
                    from cadastre_finder.processing.parcel_adjacency import (
                        build_parcel_adjacency,
                    )
                    build_parcel_adjacency(
                        db_path=opts.db_path,
                        departments=opts.departments,
                        force=False,
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

    finally:
        hb.stop()

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
