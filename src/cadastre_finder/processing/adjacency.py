"""Calcul de la table d'adjacence des communes.

Les communes voisines (rang 1) sont celles dont les géométries se touchent
ou se croisent (avec un buffer de 1m pour absorber les imprécisions topologiques).
"""
from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger

from cadastre_finder.config import DB_PATH


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

        # Vérification idempotence
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

        logger.info("Construction de la table d'adjacence (rang 1)...")

        # Calcul des adjacences rang 1
        # Buffer de 1m en EPSG:4326 ≈ 9e-6 degrés, suffisant pour les imprécisions
        # On utilise ST_Intersects avec un tiny buffer pour gérer les touches exactes
        con.execute("""
            CREATE OR REPLACE TABLE communes_adjacency AS
            SELECT DISTINCT
                a.code_insee AS code_insee_a,
                b.code_insee AS code_insee_b,
                1            AS rang
            FROM communes a
            JOIN communes b ON (
                a.code_insee < b.code_insee
                AND ST_Intersects(
                    ST_Buffer(a.geometry, 0.00001),
                    ST_Buffer(b.geometry, 0.00001)
                )
                AND NOT ST_Equals(a.geometry, b.geometry)
            )
        """)

        # Symétrisation : ajouter les paires inverses
        con.execute("""
            INSERT INTO communes_adjacency (code_insee_a, code_insee_b, rang)
            SELECT code_insee_b, code_insee_a, 1
            FROM communes_adjacency
            WHERE rang = 1
        """)

        n1 = con.execute(
            "SELECT COUNT(*) FROM communes_adjacency WHERE rang = 1"
        ).fetchone()[0]
        logger.info(f"Rang 1 : {n1} paires d'adjacence calculées.")

        if include_rank2:
            logger.info("Calcul du rang 2...")
            con.execute("""
                INSERT INTO communes_adjacency (code_insee_a, code_insee_b, rang)
                SELECT DISTINCT
                    r1.code_insee_a,
                    r1b.code_insee_b,
                    2 AS rang
                FROM communes_adjacency r1
                JOIN communes_adjacency r1b ON r1.code_insee_b = r1b.code_insee_a
                WHERE r1b.code_insee_b != r1.code_insee_a
                  AND r1.rang = 1
                  AND r1b.rang = 1
                  AND NOT EXISTS (
                      SELECT 1 FROM communes_adjacency ex
                      WHERE ex.code_insee_a = r1.code_insee_a
                        AND ex.code_insee_b = r1b.code_insee_b
                  )
            """)
            n2 = con.execute(
                "SELECT COUNT(*) FROM communes_adjacency WHERE rang = 2"
            ).fetchone()[0]
            logger.info(f"Rang 2 : {n2} paires supplémentaires calculées.")

        # Index sur code_insee_a pour les lookups rapides
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_communes_adj_a
            ON communes_adjacency (code_insee_a)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_communes_adj_a_rang
            ON communes_adjacency (code_insee_a, rang)
        """)

        logger.info("Table d'adjacence construite avec succès.")
    finally:
        con.close()


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
