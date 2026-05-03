"""Pré-calcul de la table d'adjacence des parcelles cadastrales.

Approche : Python + Shapely STRtree (O(n log n) par commune, mémoire contrôlée),
parallélisé sur N processus (un par cœur). Les géométries sont chargées par lots
(_COMPUTE_BATCH communes) en WKB binaire pour limiter l'empreinte RAM. Les workers
reçoivent des données Python pures et n'ouvrent jamais DuckDB.

Usage CLI : cadastre-finder build-parcel-adjacency [--dept 61] [--workers 8]
"""
from __future__ import annotations

import os
import queue
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from loguru import logger
from shapely import simplify
from shapely.strtree import STRtree
from shapely.wkb import loads as wkb_loads
from tqdm import tqdm

from cadastre_finder.config import DB_PATH

_CHECKPOINT_EVERY = 1000
_FLUSH_EVERY_PAIRS = 200_000   # ~10 Mo de DataFrame, ~0.1 s d'INSERT côté DuckDB
_QUEUE_MAXSIZE = 64            # backpressure : ~600 k paires en attente max
_LOAD_BATCH = 500       # communes chargées par requête SQL (évite un IN() trop long)
_COMPUTE_BATCH = 200    # communes traitées par lot (contrôle l'empreinte mémoire)
# Buffer en degrés WGS84 pour combler les micro-gaps cadastraux (~1 m à 47°N)
_BUFFER_DEG = 0.000009
# Tolérance de simplification Douglas-Peucker (~11 cm en WGS84) — sans impact
# sémantique pour de l'adjacence à 1 m, élimine 70-90 % des sommets de bruit.
_SIMPLIFY_TOL = 1e-6


class _AdjacencyWriter:
    """Thread d'écriture : découple les INSERT DuckDB du chemin critique main.

    Le main pousse des batches de paires via `submit()` ; un thread interne
    accumule en RAM jusqu'à `flush_every_pairs` puis fait un `INSERT INTO ...
    SELECT * FROM df` (50× plus rapide que `executemany`). Le CHECKPOINT est
    déclenché par le writer toutes les `checkpoint_every_communes` communes.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        flush_every_pairs: int = _FLUSH_EVERY_PAIRS,
        queue_maxsize: int = _QUEUE_MAXSIZE,
        checkpoint_every_communes: int = _CHECKPOINT_EVERY,
    ) -> None:
        self._con = con
        self._q: queue.Queue[list[tuple[str, str]] | None] = queue.Queue(maxsize=queue_maxsize)
        self._buf: list[tuple[str, str]] = []
        self._flush_threshold = flush_every_pairs
        self._ckpt_every = checkpoint_every_communes
        self._communes_since_ckpt = 0
        self.total_pairs_inserted = 0
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="adj-writer", daemon=False)

    def start(self) -> None:
        self._thread.start()

    def submit(self, pairs: list[tuple[str, str]]) -> None:
        if self.error is not None:
            raise self.error
        # `put` bloque si la queue est pleine → backpressure naturelle sur le main.
        self._q.put(pairs)

    def stop_and_join(self, timeout: float = 600) -> None:
        # Sentinelle de fin : déclenche le flush résiduel + checkpoint final.
        self._q.put(None)
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise RuntimeError("[parcel_adj] writer hang (timeout)")
        if self.error is not None:
            raise self.error

    def _flush(self, do_checkpoint: bool) -> None:
        if self._buf:
            df = pd.DataFrame(self._buf, columns=["id_a", "id_b"])
            self._con.register("_padj_stage", df)
            try:
                self._con.execute(
                    "INSERT INTO parcelles_adjacency SELECT id_a, id_b FROM _padj_stage"
                )
            finally:
                self._con.unregister("_padj_stage")
            self.total_pairs_inserted += len(self._buf)
            self._buf.clear()
        if do_checkpoint:
            self._con.execute("CHECKPOINT")
            self._communes_since_ckpt = 0

    def _run(self) -> None:
        try:
            while True:
                item = self._q.get()
                if item is None:
                    self._flush(do_checkpoint=False)
                    if self._communes_since_ckpt > 0:
                        self._con.execute("CHECKPOINT")
                    return
                self._buf.extend(item)
                self._communes_since_ckpt += 1
                if len(self._buf) >= self._flush_threshold:
                    self._flush(do_checkpoint=False)
                if self._communes_since_ckpt >= self._ckpt_every:
                    self._flush(do_checkpoint=True)
        except BaseException as e:
            self.error = e
            # Vide la queue pour ne pas bloquer le main sur un `put` saturé.
            try:
                while True:
                    self._q.get_nowait()
            except queue.Empty:
                pass


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

        # Cursor thread-local DuckDB : le main lit via `read_cur` pendant que le
        # writer écrit via `con`. DuckDB sérialise les `execute()` en interne →
        # pas besoin de Lock Python explicite.
        read_cur = con.cursor()
        read_cur.execute("LOAD spatial;")

        all_communes = _list_target_communes(read_cur, departments, communes)
        if not all_communes:
            logger.warning("[parcel_adj] Aucune commune cible.")
            return

        if force:
            _delete_existing(read_cur, all_communes)
            todo = all_communes
        else:
            already = _list_already_done(read_cur)
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

        # Drop des index secondaires APRÈS _list_already_done (qui en bénéficie).
        # Maintenir 2 index sur des millions de lignes pendant 6000 communes coûte
        # bien plus cher que de les reconstruire en bloc à la fin.
        logger.info("[parcel_adj] Drop des index (recréation in-fine)")
        con.execute("DROP INDEX IF EXISTS idx_parcel_adj_a")
        con.execute("DROP INDEX IF EXISTS idx_parcel_adj_b")

        n_db_before = con.execute("SELECT COUNT(*) FROM parcelles_adjacency").fetchone()[0]

        # Découple les inserts du chemin critique main thread : sans ça, le main
        # bloque ~3 s par commune sur `executemany` et le pool de 31 workers reste
        # massivement sous-saturé. Le writer flushe par batch DataFrame (~50× plus
        # rapide que `executemany`) et déclenche les CHECKPOINT.
        writer = _AdjacencyWriter(con)
        writer.start()

        try:
            # Le pool est créé UNE SEULE FOIS : sur Windows (spawn), chaque création
            # du pool engendre N nouveaux processus Python (~5-10 s chacun). Recréer
            # le pool à chaque lot de 200 communes coûtait N×nb_lots spawns inutiles.
            with ProcessPoolExecutor(max_workers=workers) as exe:
                with tqdm(total=len(todo), desc="Adjacence parcelles", unit="commune") as pbar:
                    for batch_start in range(0, len(todo), _COMPUTE_BATCH):
                        batch_codes = todo[batch_start : batch_start + _COMPUTE_BATCH]

                        # Charge uniquement ce lot en WKB binaire (50 % plus compact que GeoJSON,
                        # parsing ~5× plus rapide dans les workers)
                        commune_data = _load_commune_geometries_wkb(read_cur, batch_codes)

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
                                writer.submit(pairs)
                                total_pairs += len(pairs)
                            completed += 1
                            pbar.update(1)
                            pbar.set_postfix(pairs=f"{total_pairs:,}")

                        del commune_data  # libère la mémoire du lot avant le suivant
        finally:
            try:
                writer.stop_and_join(timeout=600)
            finally:
                logger.info("[parcel_adj] Reconstruction des index secondaires")
                con.execute("CREATE INDEX IF NOT EXISTS idx_parcel_adj_a ON parcelles_adjacency (id_a)")
                con.execute("CREATE INDEX IF NOT EXISTS idx_parcel_adj_b ON parcelles_adjacency (id_b)")
                con.execute("CHECKPOINT")

        n_db_after = con.execute("SELECT COUNT(*) FROM parcelles_adjacency").fetchone()[0]
        delta_db = n_db_after - n_db_before
        logger.info(
            f"[parcel_adj] Terminé : {writer.total_pairs_inserted:,} paires écrites "
            f"par le writer (+{delta_db:,} en table) sur {completed} communes."
        )
        if writer.total_pairs_inserted != delta_db:
            logger.warning(
                f"[parcel_adj] Mismatch : writer={writer.total_pairs_inserted:,} vs "
                f"DB delta={delta_db:,}. Possible perte de paires."
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Helpers SQL
# ---------------------------------------------------------------------------

def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    # Migration une fois : retire la PRIMARY KEY (id_a, id_b) si elle est encore
    # présente. La PK est redondante (paires uniques par construction) et coûte
    # très cher à maintenir à chaque INSERT sur une table de plusieurs millions
    # de lignes. DuckDB 1.x ne supporte pas ALTER TABLE DROP CONSTRAINT → on
    # passe par CREATE TABLE AS SELECT + DROP + RENAME.
    has_table = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'parcelles_adjacency'"
    ).fetchone()[0] > 0

    if has_table:
        has_pk = con.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_name = 'parcelles_adjacency'
              AND constraint_type = 'PRIMARY KEY'
        """).fetchone()[0] > 0

        if has_pk:
            n_before = con.execute("SELECT COUNT(*) FROM parcelles_adjacency").fetchone()[0]
            logger.info(
                f"[parcel_adj] Migration : suppression de la PRIMARY KEY "
                f"({n_before:,} paires à préserver)"
            )
            con.execute("DROP TABLE IF EXISTS _padj_new")
            con.execute("CREATE TABLE _padj_new AS SELECT id_a, id_b FROM parcelles_adjacency")
            n_after = con.execute("SELECT COUNT(*) FROM _padj_new").fetchone()[0]
            if n_after != n_before:
                con.execute("DROP TABLE _padj_new")
                raise RuntimeError(
                    f"[parcel_adj] Migration KO : {n_before:,} → {n_after:,} lignes"
                )
            con.execute("DROP TABLE parcelles_adjacency")
            con.execute("ALTER TABLE _padj_new RENAME TO parcelles_adjacency")
            logger.info("[parcel_adj] Migration OK")

    con.execute("""
        CREATE TABLE IF NOT EXISTS parcelles_adjacency (
            id_a VARCHAR NOT NULL,
            id_b VARCHAR NOT NULL
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
            g = wkb_loads(wkb_bytes)
            # Élimine 70-90 % des sommets (bruit numérique sub-millimétrique
            # du cadastre) — sans impact pour de l'adjacence à 1 m.
            g = simplify(g, _SIMPLIFY_TOL, preserve_topology=False)
            geoms.append(g)
            ids.append(pid)
        except Exception:
            continue

    if len(geoms) < 2:
        return []

    # `dwithin` calcule la distance directement au niveau GEOS sans matérialiser
    # de polygones tampons (coûteux sur des polygones complexes). La requête
    # vectorisée retourne toutes les paires intersectantes en un seul appel C.
    geoms_arr = np.asarray(geoms, dtype=object)
    tree = STRtree(geoms_arr)
    qi_arr, ti_arr = tree.query(geoms_arr, predicate="dwithin", distance=_BUFFER_DEG)

    pairs: set[tuple[str, str]] = set()
    for qi, ti in zip(qi_arr, ti_arr):
        if qi >= ti:
            continue
        a, b = ids[qi], ids[ti]
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
