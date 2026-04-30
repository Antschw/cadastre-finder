"""Pré-calcul de la table d'adjacence des parcelles cadastrales.

Approche : Python + Shapely STRtree (O(n log n) par commune, mémoire contrôlée).
Le calcul DuckDB/spatial était trop gourmand en RAM (SIGKILL à l'exécution).

Usage CLI : cadastre-finder build-parcel-adjacency [--dept 61]
"""
from __future__ import annotations

import gc
import json
from pathlib import Path

import duckdb
from loguru import logger
from shapely.geometry import shape
from shapely.strtree import STRtree
from tqdm import tqdm

from cadastre_finder.config import DB_PATH

_CHECKPOINT_EVERY = 30
_BATCH_INSERT = 20_000   # paires insérées par executemany
_MEMORY_LIMIT  = "600MB" # DuckDB garde peu de mémoire ; le gros travail est en Python
# Buffer en degrés WGS84 pour combler les micro-gaps cadastraux (~1 m à 47°N)
_BUFFER_DEG = 0.000009


def build_parcel_adjacency(
    db_path: Path = DB_PATH,
    departments: list[str] | None = None,
    communes: list[str] | None = None,
    force: bool = False,
) -> None:
    """Pré-calcule les paires de parcelles adjacentes.

    Args:
        departments: limiter à ces codes département (tous si None).
        communes:    reconstruire uniquement ces codes INSEE (prioritaire sur departments).
        force:       effacer et recalculer les communes déjà traitées.
    """
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
        _ensure_table(con)

        if communes:
            # Mode ciblé : reconstruire uniquement les communes demandées
            rows = con.execute(
                f"SELECT DISTINCT code_insee, code_dept FROM parcelles "
                f"WHERE code_insee IN ({', '.join('?' * len(communes))})"
                f" ORDER BY code_insee",
                communes,
            ).fetchall()
            logger.info(f"Calcul adjacence parcellaire : {len(rows)} commune(s) ciblée(s) (--communes)")
        else:
            target_depts = departments or _loaded_departments(con)
            if not target_depts:
                logger.warning("Aucun département chargé dans la base.")
                return
            rows = con.execute(
                f"SELECT DISTINCT code_insee, code_dept FROM parcelles "
                f"WHERE code_dept IN ({', '.join('?' * len(target_depts))})"
                f" ORDER BY code_dept, code_insee",
                target_depts,
            ).fetchall()
            logger.info(
                f"Calcul adjacence parcellaire : {len(rows)} commune(s) "
                f"sur {len(target_depts)} département(s)"
            )

        total_pairs = 0
        for i, (code_insee, _dept) in enumerate(tqdm(rows, desc="Adjacence parcelles")):
            if force:
                # Supprimer les paires existantes pour cette commune avant recalcul
                con.execute("""
                    DELETE FROM parcelles_adjacency
                    WHERE id_a IN (SELECT id FROM parcelles WHERE code_insee = ?)
                       OR id_b IN (SELECT id FROM parcelles WHERE code_insee = ?)
                """, [code_insee, code_insee])
            else:
                already = con.execute(
                    """SELECT COUNT(*) FROM parcelles_adjacency pa
                       JOIN parcelles p ON p.id = pa.id_a
                       WHERE p.code_insee = ? LIMIT 1""",
                    [code_insee],
                ).fetchone()[0]
                if already:
                    continue

            n = _compute_commune_adjacency(con, code_insee)
            total_pairs += n

            if (i + 1) % _CHECKPOINT_EVERY == 0:
                con.execute("CHECKPOINT")
                logger.debug(
                    f"[parcel_adj] Checkpoint après {i + 1} communes "
                    f"({total_pairs:,} paires total)."
                )

        con.execute("CHECKPOINT")
        logger.info(f"Adjacence parcellaire terminée : {total_pairs:,} paires calculées.")
    finally:
        con.close()


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


def _loaded_departments(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in con.execute("SELECT DISTINCT code_dept FROM parcelles").fetchall()]


def _compute_commune_adjacency(con: duckdb.DuckDBPyConnection, code_insee: str) -> int:
    """Calcule l'adjacence d'une commune via Shapely STRtree. Retourne le nombre de paires."""
    rows = con.execute(
        "SELECT id, ST_AsGeoJSON(geometry) FROM parcelles WHERE code_insee = ?",
        [code_insee],
    ).fetchall()

    if len(rows) < 2:
        return 0

    # Construction des objets Shapely
    ids: list[str] = []
    geoms = []
    for pid, geojson in rows:
        if not geojson:
            continue
        try:
            geoms.append(shape(json.loads(geojson)))
            ids.append(pid)
        except Exception:
            pass

    if len(geoms) < 2:
        return 0

    # STRtree : O(n log n), mémoire proportionnelle à n
    # On bâtit le tree sur les géométries buffées pour capter les micro-gaps cadastraux
    buffered = [g.buffer(_BUFFER_DEG) for g in geoms]
    tree = STRtree(buffered)
    pairs: set[tuple[str, str]] = set()

    for i, buf_geom in enumerate(buffered):
        for j in tree.query(buf_geom):
            if j <= i:
                continue
            if buf_geom.intersects(buffered[j]) and not geoms[i].equals(geoms[j]):
                a, b = ids[i], ids[j]
                pairs.add((a, b) if a < b else (b, a))

    del buffered

    # Libération mémoire explicite avant l'écriture
    del geoms, tree
    gc.collect()

    if not pairs:
        return 0

    # Insertion en batches
    pair_list = list(pairs)
    for start in range(0, len(pair_list), _BATCH_INSERT):
        batch = pair_list[start : start + _BATCH_INSERT]
        con.executemany("INSERT OR IGNORE INTO parcelles_adjacency VALUES (?, ?)", batch)

    return len(pairs)


# ---------------------------------------------------------------------------
# API publique
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
