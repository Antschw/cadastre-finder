"""Pré-calcul de la table d'adjacence des parcelles cadastrales.

Approche : Python + Shapely STRtree (O(n log n) par commune, mémoire contrôlée),
parallélisé sur N processus (un par cœur). Les géométries sont chargées par lots
(_COMPUTE_BATCH communes) en WKB binaire pour limiter l'empreinte RAM. Les workers
reçoivent des données Python pures et n'ouvrent jamais DuckDB.

Usage CLI : cadastre-finder build-parcel-adjacency [--dept 61] [--workers 8]
"""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
from loguru import logger
from shapely.strtree import STRtree
from shapely.wkb import loads as wkb_loads
from tqdm import tqdm

from cadastre_finder.config import DB_PATH

_CHECKPOINT_EVERY = 50
_BATCH_INSERT = 50_000
_LOAD_BATCH = 500       # communes chargées par requête SQL (évite un IN() trop long)
_COMPUTE_BATCH = 200    # communes traitées par lot (contrôle l'empreinte mémoire)
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

        total_pairs = 0
        completed = 0

        # Le pool est créé UNE SEULE FOIS : sur Windows (spawn), chaque création
        # du pool engendre N nouveaux processus Python (~5-10 s chacun). Recréer
        # le pool à chaque lot de 200 communes coûtait N×nb_lots spawns inutiles.
        with ProcessPoolExecutor(max_workers=workers) as exe:
            with tqdm(total=len(todo), desc="Adjacence parcelles", unit="commune") as pbar:
                for batch_start in range(0, len(todo), _COMPUTE_BATCH):
                    batch_codes = todo[batch_start : batch_start + _COMPUTE_BATCH]

                    # Charge uniquement ce lot en WKB binaire (50 % plus compact que GeoJSON,
                    # parsing ~5× plus rapide dans les workers)
                    commune_data = _load_commune_geometries_wkb(con, batch_codes)

                    tasks = [
                        (code, commune_data.get(code, []))
                        for code in batch_codes
                        if len(commune_data.get(code, [])) >= 2
                    ]

                    futs = {
                        exe.submit(_compute_commune_pairs_worker, code, data): code
                        for code, data in tasks
                    }
                    for fut in as_completed(futs):
                        code = futs[fut]
                        try:
                            pairs = fut.result()
                        except Exception as e:
                            logger.error(f"[parcel_adj] {code} ÉCHEC : {e}")
                            pbar.update(1)
                            continue

                        if pairs:
                            _bulk_insert(con, pairs)
                            total_pairs += len(pairs)
                        completed += 1
                        pbar.update(1)
                        pbar.set_postfix(pairs=f"{total_pairs:,}")

                        if completed % _CHECKPOINT_EVERY == 0:
                            con.execute("CHECKPOINT")

                    del commune_data  # libère la mémoire du lot avant le suivant
                    con.execute("CHECKPOINT")

        logger.info(f"[parcel_adj] Terminé : {total_pairs:,} paires sur {completed} communes.")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Helpers SQL
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


def _load_commune_geometries_wkb(
    con: duckdb.DuckDBPyConnection,
    batch: list[str],
) -> dict[str, list[tuple[str, bytes]]]:
    """Charge id + WKB binaire pour les communes du lot, par sous-requêtes SQL."""
    commune_data: dict[str, list[tuple[str, bytes]]] = {}

    for start in range(0, len(batch), _LOAD_BATCH):
        sub = batch[start : start + _LOAD_BATCH]
        placeholders = ", ".join("?" * len(sub))
        rows = con.execute(
            f"SELECT code_insee, id, ST_AsWKB(geometry) "
            f"FROM parcelles WHERE code_insee IN ({placeholders})",
            sub,
        ).fetchall()
        for code_insee, pid, wkb_bytes in rows:
            if wkb_bytes:
                commune_data.setdefault(code_insee, []).append((pid, bytes(wkb_bytes)))

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
    rows: list[tuple[str, bytes]],
) -> list[tuple[str, str]]:
    """Calcule les paires adjacentes depuis des données WKB pré-chargées."""
    if len(rows) < 2:
        return []

    ids: list[str] = []
    geoms = []
    for pid, wkb_bytes in rows:
        try:
            geoms.append(wkb_loads(wkb_bytes))
            ids.append(pid)
        except Exception:
            continue

    if len(geoms) < 2:
        return []

    buffered = [g.buffer(_BUFFER_DEG) for g in geoms]
    tree = STRtree(buffered)
    pairs: set[tuple[str, str]] = set()

    for i, buf in enumerate(buffered):
        for j in tree.query(buf, predicate="intersects"):
            if j <= i:
                continue
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
