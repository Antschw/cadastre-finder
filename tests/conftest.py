"""Fixtures pytest partagées."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Base DuckDB temporaire avec tables cadastre pré-remplies (données synthétiques)."""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL spatial; LOAD spatial;")

    # Table communes
    con.execute("""
        CREATE TABLE communes (
            code_insee  VARCHAR PRIMARY KEY,
            nom         VARCHAR,
            code_dept   VARCHAR,
            geometry    GEOMETRY
        )
    """)

    communes_data = [
        ("61293", "Mortagne-au-Perche", "61",
         "POLYGON((0.54 48.50, 0.60 48.50, 0.60 48.55, 0.54 48.55, 0.54 48.50))"),
        ("61001", "LAigle", "61",
         "POLYGON((0.60 48.75, 0.66 48.75, 0.66 48.80, 0.60 48.80, 0.60 48.75))"),
        ("61002", "Alencon", "61",
         "POLYGON((0.08 48.42, 0.14 48.42, 0.14 48.47, 0.08 48.47, 0.08 48.42))"),
        ("61100", "Tourouvre-au-Perche", "61",
         "POLYGON((0.60 48.50, 0.66 48.50, 0.66 48.55, 0.60 48.55, 0.60 48.50))"),
    ]
    for code, nom, dept, wkt in communes_data:
        con.execute(
            "INSERT INTO communes VALUES (?, ?, ?, ST_GeomFromText(?))",
            [code, nom, dept, wkt],
        )

    # Table parcelles
    con.execute("""
        CREATE TABLE parcelles (
            id          VARCHAR PRIMARY KEY,
            code_insee  VARCHAR,
            code_dept   VARCHAR,
            prefixe     VARCHAR,
            section     VARCHAR,
            numero      VARCHAR,
            contenance  INTEGER,
            geometry    GEOMETRY
        )
    """)
    con.execute("CREATE INDEX idx_parcelles_insee_contenance ON parcelles (code_insee, contenance)")

    parcelles_data = [
        ("61293000AB0042", "61293", "61", "000", "AB", "0042", 4200,
         "POLYGON((0.55 48.51, 0.56 48.51, 0.56 48.52, 0.55 48.52, 0.55 48.51))"),
        ("61293000AB0043", "61293", "61", "000", "AB", "0043", 3500,
         "POLYGON((0.56 48.51, 0.57 48.51, 0.57 48.52, 0.56 48.52, 0.56 48.51))"),
        ("61293000AB0044", "61293", "61", "000", "AB", "0044", 8000,
         "POLYGON((0.57 48.51, 0.58 48.51, 0.58 48.52, 0.57 48.52, 0.57 48.51))"),
        # Parcelle < 2500 m² — ne doit jamais apparaître
        ("61293000AB0045", "61293", "61", "000", "AB", "0045", 1200,
         "POLYGON((0.58 48.51, 0.59 48.51, 0.59 48.52, 0.58 48.52, 0.58 48.51))"),
        # Voisine (commune 61100) avec surface identique 4200 m²
        ("61100000CD0010", "61100", "61", "000", "CD", "0010", 4200,
         "POLYGON((0.61 48.51, 0.62 48.51, 0.62 48.52, 0.61 48.52, 0.61 48.51))"),
    ]
    for pid, insee, dept, pref, sec, num, cont, wkt in parcelles_data:
        con.execute(
            "INSERT INTO parcelles VALUES (?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))",
            [pid, insee, dept, pref, sec, num, cont, wkt],
        )

    # Table d'adjacence
    con.execute("""
        CREATE TABLE communes_adjacency (
            code_insee_a VARCHAR,
            code_insee_b VARCHAR,
            rang         INTEGER
        )
    """)
    con.execute("CREATE INDEX idx_communes_adj_a ON communes_adjacency (code_insee_a)")
    con.executemany(
        "INSERT INTO communes_adjacency VALUES (?, ?, ?)",
        [("61293", "61100", 1), ("61100", "61293", 1)],
    )

    con.close()
    return db_path
