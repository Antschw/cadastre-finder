"""Pré-calcul de la table d'adjacence des parcelles cadastrales.

Approche : Python + Shapely STRtree (O(n log n) par commune, mémoire contrôlée),
parallélisé sur N processus (un par cœur). Les géométries sont pré-chargées en
mémoire dans le thread principal avant de lancer les workers — les workers n'ouvrent
jamais DuckDB, ce qui évite le verrou exclusif sur Windows.

Usage CLI : cadastre-finder build-parcel-adjacency [--dept 61] [--workers 8]
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
from loguru import logger
from shapely.geometry import shape
from shapely.strtree import STRtree
from tqdm import tqdm

from cadastre_finder.config import DB_PATH

_CHECKPOINT_EVERY = 50
_BATCH_INSERT = 20_000
_LOAD_BATCH = 500       # communes chargées par requête SQL (évite un IN() trop long)
# Buffer en degrés WGS84 pour combler les micro-gaps cadastraux (~1 m à 47°N)
_BUFFER_DEG = 0.000009


def build_parcel_adjacency(
    db_path: Path = DB_PATH,
    departments: list[str] | None = None,
    communes: list[str] | None = None,
    force: bool = False,
    workers: int | None = None,
) -> None:
    """Pré-calcule les paires de parcelles adjacentes en parallèle.

    Args:
        departments: limiter à ces codes département (tous si None).
        communes:    reconstruire uniquement ces codes INSEE (prioritaire sur departments).
        force:       effacer et recalculer les communes déjà traitées.
        workers:     nombre de processus parallèles (défaut : os.cpu_count() - 1).
    """
    workers = workers or max(1, (os.cpu_count() or 2) - 1)

    # 1. Liste des communes cibles + filtre idempotence
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        _ensure_table(con)

        all_communes = _list_target_communes(con, departments, communes)
        if not all_communes:
            logger.warning("[parcel_adj] Aucune commune cible.")
            return

        if force:
            _delete_existing(con, all_communes)
            todo = all_communes
        else:
            already = _list_already_done(con)
            todo = [c for c in all_communes if c not in already]

        logger.info(
            f"[parcel_adj] {len(todo)} commune(s) à traiter "
            f"({len(all_communes) - len(todo)} déjà OK), "
            f"{workers} workers."
        )

        if not todo:
            return

        # Pré-chargement de toutes les géométries en mémoire.
        # Les workers ne toucheront jamais DuckDB → pas de conflit de verrou Windows.
        logger.info("[parcel_adj] Chargement des géométries en mémoire...")
        commune_data = _load_commune_geometries(con, todo)
    finally:
        con.close()

    # 2. Calcul parallèle — workers reçoivent des données Python pures
    write_con = duckdb.connect(str(db_path))
    try:
        write_con.execute("INSTALL spatial; LOAD spatial;")
        total_pairs = 0
        completed = 0

        tasks = [
            (code, commune_data.get(code, []))
            for code in todo
            if len(commune_data.get(code, [])) >= 2
        ]

        with ProcessPoolExecutor(max_workers=workers) as exe:
            futs = {
                exe.submit(_compute_commune_pairs_worker, code, data): code
                for code, data in tasks
            }
            with tqdm(total=len(futs), desc="Adjacence parcelles", unit="commune") as pbar:
                for fut in as_completed(futs):
                    code = futs[fut]
                    try:
                        pairs = fut.result()
                    except Exception as e:
                        logger.error(f"[parcel_adj] {code} ÉCHEC : {e}")
                        pbar.update(1)
                        continue

                    if pairs:
                        _bulk_insert(write_con, pairs)
                        total_pairs += len(pairs)
                    completed += 1
                    pbar.update(1)
                    pbar.set_postfix(pairs=f"{total_pairs:,}")

                    if completed % _CHECKPOINT_EVERY == 0:
                        write_con.execute("CHECKPOINT")

        write_con.execute("CHECKPOINT")
        logger.info(f"[parcel_adj] Terminé : {total_pairs:,} paires sur {completed} communes.")
    finally:
        write_con.close()


# ---------------------------------------------------------------------------
# Helpers SQL (main thread)
# ---------------------------------------------------------------------------

def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS parcelles_adjacency (
            id_a VARCHAR NOT NULL,
            id_b VARCHAR NOT NULL,
            PRIMARY KEY (id_a, id_b)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_parcel_adj_a ON parcelles_adjacency (id_a)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_parcel_adj_b ON parcelles_adjacency (id_b)")


def _list_target_communes(
    con: duckdb.DuckDBPyConnection,
    departments: list[str] | None,
    communes: list[str] | None,
) -> list[str]:
    if communes:
        rows = con.execute(
            f"SELECT DISTINCT code_insee FROM parcelles "
            f"WHERE code_insee IN ({', '.join('?' * len(communes))}) "
            f"ORDER BY code_insee",
            communes,
        ).fetchall()
        return [r[0] for r in rows]

    if departments:
        rows = con.execute(
            f"SELECT DISTINCT code_insee FROM parcelles "
            f"WHERE code_dept IN ({', '.join('?' * len(departments))}) "
            f"ORDER BY code_dept, code_insee",
            departments,
        ).fetchall()
        return [r[0] for r in rows]

    rows = con.execute(
        "SELECT DISTINCT code_insee FROM parcelles ORDER BY code_dept, code_insee"
    ).fetchall()
    return [r[0] for r in rows]


def _load_commune_geometries(
    con: duckdb.DuckDBPyConnection,
    todo: list[str],
) -> dict[str, list[tuple[str, str]]]:
    """Charge id + GeoJSON de toutes les parcelles des communes cibles, par lots."""
    commune_data: dict[str, list[tuple[str, str]]] = {}
    total = len(todo)

    with tqdm(total=total, desc="Chargement géométries", unit="commune") as pbar:
        for start in range(0, total, _LOAD_BATCH):
            batch = todo[start : start + _LOAD_BATCH]
            placeholders = ", ".join("?" * len(batch))
            rows = con.execute(
                f"SELECT code_insee, id, ST_AsGeoJSON(geometry) "
                f"FROM parcelles WHERE code_insee IN ({placeholders})",
                batch,
            ).fetchall()
            for code_insee, pid, geojson in rows:
                if geojson:
                    commune_data.setdefault(code_insee, []).append((pid, geojson))
            pbar.update(len(batch))

    parcel_count = sum(len(v) for v in commune_data.values())
    logger.info(f"[parcel_adj] {parcel_count:,} parcelles chargées pour {len(commune_data)} communes.")
    return commune_data


def _list_already_done(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute("""
        SELECT DISTINCT p.code_insee
        FROM parcelles_adjacency pa
        JOIN parcelles p ON p.id = pa.id_a
    """).fetchall()
    return {r[0] for r in rows}


def _delete_existing(con: duckdb.DuckDBPyConnection, codes: list[str]) -> None:
    if not codes:
        return
    placeholders = ", ".join("?" * len(codes))
    con.execute(
        f"""DELETE FROM parcelles_adjacency
            WHERE id_a IN (SELECT id FROM parcelles WHERE code_insee IN ({placeholders}))
               OR id_b IN (SELECT id FROM parcelles WHERE code_insee IN ({placeholders}))""",
        codes + codes,
    )


def _bulk_insert(con: duckdb.DuckDBPyConnection, pairs: list[tuple[str, str]]) -> None:
    if not pairs:
        return
    for start in range(0, len(pairs), _BATCH_INSERT):
        batch = pairs[start : start + _BATCH_INSERT]
        con.executemany("INSERT OR IGNORE INTO parcelles_adjacency VALUES (?, ?)", batch)


# ---------------------------------------------------------------------------
# Worker (process séparé) — reçoit des données Python pures, pas de DuckDB
# ---------------------------------------------------------------------------

def _compute_commune_pairs_worker(
    code_insee: str,
    rows: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Calcule les paires adjacentes depuis des données pré-chargées (sans DuckDB)."""
    if len(rows) < 2:
        return []

    ids: list[str] = []
    geoms = []
    for pid, geojson in rows:
        try:
            geoms.append(shape(json.loads(geojson)))
            ids.append(pid)
        except Exception:
            continue

    if len(geoms) < 2:
        return []

    buffered = [g.buffer(_BUFFER_DEG) for g in geoms]
    tree = STRtree(buffered)
    pairs: set[tuple[str, str]] = set()

    for i, buf in enumerate(buffered):
        for j in tree.query(buf):
            if j <= i:
                continue
            if buf.intersects(buffered[j]) and not geoms[i].equals(geoms[j]):
                a, b = ids[i], ids[j]
                pairs.add((a, b) if a < b else (b, a))

    return list(pairs)


# ---------------------------------------------------------------------------
# API publique de lecture
# ---------------------------------------------------------------------------

def get_parcel_neighbors(
    parcel_ids: list[str],
    db_path: Path = DB_PATH,
) -> dict[str, set[str]]:
    """Retourne le graphe d'adjacence pour un ensemble d'IDs de parcelles."""
    if not parcel_ids:
        return {}

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table_exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'parcelles_adjacency'"
        ).fetchone()[0]
        if not table_exists:
            return {}

        id_set = set(parcel_ids)
        placeholders = ", ".join("?" * len(parcel_ids))
        rows = con.execute(
            f"SELECT id_a, id_b FROM parcelles_adjacency "
            f"WHERE id_a IN ({placeholders}) OR id_b IN ({placeholders})",
            parcel_ids + parcel_ids,
        ).fetchall()

        graph: dict[str, set[str]] = {}
        for id_a, id_b in rows:
            if id_a in id_set and id_b in id_set:
                graph.setdefault(id_a, set()).add(id_b)
                graph.setdefault(id_b, set()).add(id_a)
        return graph
    finally:
        con.close()


def has_precomputed_adjacency(db_path: Path = DB_PATH) -> bool:
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        n = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'parcelles_adjacency'"
        ).fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calcul parallèle adjacence parcelles")
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    parser.add_argument("--dept", nargs="+", help="Limiter à ces départements")
    parser.add_argument("--communes", nargs="+", help="Reconstruire uniquement ces codes INSEE")
    parser.add_argument("--force", action="store_true", help="Effacer et recalculer")
    parser.add_argument("--workers", type=int, default=None, help="Nb workers (défaut : cpu-1)")
    args = parser.parse_args()

    build_parcel_adjacency(
        db_path=Path(args.db),
        departments=args.dept,
        communes=args.communes,
        force=args.force,
        workers=args.workers,
    )
