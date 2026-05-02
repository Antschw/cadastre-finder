"""Calcul de la table d'adjacence des communes.

Les communes voisines (rang 1) sont celles dont les géométries se touchent
ou se croisent (avec un buffer de 1m pour absorber les imprécisions topologiques).

Approche : Python + Shapely STRtree (même pattern que parcel_adjacency.py).
La jointure SQL cartésienne O(N²) sans index spatial était trop lente (~1-3h pour 7 600 communes).
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
from loguru import logger
from shapely.geometry import shape
from shapely.strtree import STRtree
from tqdm import tqdm

from cadastre_finder.config import DB_PATH

_BUFFER_DEG = 0.00001   # ≈ 1 m à la latitude de la France
_BATCH_INSERT = 10_000  # paires par executemany


def build_adjacency_table(
    db_path: Path = DB_PATH,
    include_rank2: bool = True,
) -> None:
    """Construit la table `communes_adjacency` (code_insee_a, code_insee_b, rang).

    - rang=1 : communes qui se touchent (voisines directes)
    - rang=2 : voisines des voisines (calculé si include_rank2=True)

    La relation est symétrique : si (A, B) existe alors (B, A) existe.
    """
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")

        count = con.execute("SELECT COUNT(*) FROM communes").fetchone()[0]
        if count == 0:
            raise RuntimeError("La table communes est vide. Lancez d'abord l'ingestion cadastre.")

        # Idempotence : skip si déjà construit
        existing = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'communes_adjacency'"
        ).fetchone()[0]
        if existing > 0:
            n = con.execute(
                "SELECT COUNT(*) FROM communes_adjacency WHERE rang = 1"
            ).fetchone()[0]
            if n > 0:
                logger.info(f"Table d'adjacence déjà construite ({n} paires rang 1). Skip.")
                return

        # --- Chargement des géométries ---
        logger.info(f"Chargement de {count} communes depuis DuckDB...")
        rows = con.execute(
            "SELECT code_insee, ST_AsGeoJSON(geometry) FROM communes ORDER BY code_insee"
        ).fetchall()

        codes: list[str] = []
        geoms = []
        for code, geojson in rows:
            if not geojson:
                continue
            codes.append(code)
            geoms.append(shape(json.loads(geojson)))

        n_communes = len(codes)
        logger.info(f"{n_communes} communes chargées.")

        # --- Bufferisation unique ---
        logger.info("Bufferisation des géométries (une fois par commune)...")
        buffered = [g.buffer(_BUFFER_DEG) for g in tqdm(geoms, desc="Buffer", unit="commune", leave=False)]

        # --- STRtree ---
        logger.info("Construction du STRtree spatial...")
        tree = STRtree(buffered)

        # --- Rang 1 ---
        logger.info(f"Calcul rang 1 sur {n_communes} communes...")
        pairs_rang1: set[tuple[str, str]] = set()

        for i in tqdm(range(n_communes), desc="Rang 1 — voisins directs", unit="commune"):
            buf_i = buffered[i]
            for j in tree.query(buf_i):
                if j <= i:
                    continue
                if geoms[i].equals(geoms[j]):
                    continue
                if buf_i.intersects(buffered[j]):
                    pairs_rang1.add((codes[i], codes[j]))

        n1 = len(pairs_rang1)
        logger.info(f"Rang 1 : {n1} paires trouvées. Insertion en base (+ symétrique)...")

        _ensure_table(con)
        _batch_insert(con, list(pairs_rang1), rang=1)
        _batch_insert(con, [(b, a) for a, b in pairs_rang1], rang=1)

        n1_db = con.execute("SELECT COUNT(*) FROM communes_adjacency WHERE rang = 1").fetchone()[0]
        logger.info(f"Rang 1 : {n1_db} paires insérées (symétrisées).")

        # --- Rang 2 ---
        if include_rank2:
            logger.info("Calcul rang 2 (voisins des voisins)...")

            # Graphe d'adjacence rang 1 (symétrique)
            adj: dict[str, set[str]] = {}
            for a, b in pairs_rang1:
                adj.setdefault(a, set()).add(b)
                adj.setdefault(b, set()).add(a)

            # Ensemble de toutes les paires rang 1 (dans les deux sens)
            rang1_set: set[tuple[str, str]] = set()
            for a, b in pairs_rang1:
                rang1_set.add((a, b))
                rang1_set.add((b, a))

            pairs_rang2: set[tuple[str, str]] = set()
            for code_a in tqdm(adj, desc="Rang 2 — voisins des voisins", unit="commune"):
                for code_mid in adj[code_a]:
                    for code_b in adj[code_mid]:
                        if code_b == code_a:
                            continue
                        if (code_a, code_b) in rang1_set:
                            continue
                        # Stockage canonique pour éviter les doublons
                        pairs_rang2.add((min(code_a, code_b), max(code_a, code_b)))

            n2 = len(pairs_rang2)
            logger.info(f"Rang 2 : {n2} paires trouvées. Insertion en base (+ symétrique)...")
            _batch_insert(con, list(pairs_rang2), rang=2)
            _batch_insert(con, [(b, a) for a, b in pairs_rang2], rang=2)

            n2_db = con.execute("SELECT COUNT(*) FROM communes_adjacency WHERE rang = 2").fetchone()[0]
            logger.info(f"Rang 2 : {n2_db} paires insérées (symétrisées).")

        # --- Index ---
        logger.info("Création des index...")
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_communes_adj_a
            ON communes_adjacency (code_insee_a)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_communes_adj_a_rang
            ON communes_adjacency (code_insee_a, rang)
        """)
        con.execute("CHECKPOINT")
        logger.info("Table d'adjacence construite avec succès.")

    finally:
        con.close()


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS communes_adjacency (
            code_insee_a VARCHAR,
            code_insee_b VARCHAR,
            rang         INTEGER
        )
    """)


def _batch_insert(
    con: duckdb.DuckDBPyConnection,
    pairs: list[tuple[str, str]],
    rang: int,
) -> None:
    if not pairs:
        return
    data = [(a, b, rang) for a, b in pairs]
    for start in range(0, len(data), _BATCH_INSERT):
        chunk = data[start : start + _BATCH_INSERT]
        con.executemany(
            "INSERT INTO communes_adjacency (code_insee_a, code_insee_b, rang) VALUES (?, ?, ?)",
            chunk,
        )


def get_neighbors(
    code_insee: str,
    rang: int = 1,
    db_path: Path = DB_PATH,
) -> list[str]:
    """Retourne les codes INSEE des communes voisines jusqu'au rang donné."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT code_insee_b FROM communes_adjacency "
            "WHERE code_insee_a = ? AND rang <= ?",
            [code_insee, rang],
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calcul table d'adjacence communes")
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    parser.add_argument("--no-rank2", action="store_true", help="Ne pas calculer le rang 2")
    args = parser.parse_args()

    build_adjacency_table(db_path=Path(args.db), include_rank2=not args.no_rank2)
