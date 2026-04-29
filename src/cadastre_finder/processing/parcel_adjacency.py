"""Pré-calcul de la table d'adjacence des parcelles cadastrales.

Traite commune par commune pour éviter les self-joins O(n²) sur l'ensemble du département.
Résultat : table `parcelles_adjacency(id_a, id_b)` utilisée par search_combos.

Usage CLI : cadastre-finder build-parcel-adjacency [--dept 61]
"""
from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger
from tqdm import tqdm

from cadastre_finder.config import DB_PATH, DEPARTMENTS

# Flush WAL to disk toutes les N communes pour libérer la mémoire
_CHECKPOINT_EVERY = 50
# Seuil en nombre de parcelles au-delà duquel on traite en tuiles spatiales
_TILE_THRESHOLD = 3_000
# Limite mémoire DuckDB (laisse de la place pour l'OS)
_MEMORY_LIMIT = "1200MB"


def build_parcel_adjacency(
    db_path: Path = DB_PATH,
    departments: list[str] | None = None,
) -> None:
    """Pré-calcule les paires de parcelles adjacentes pour tous les départements chargés."""
    tmp_dir = db_path.parent / "tmp_adj"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory='{tmp_dir}'")
        _ensure_table(con)

        target_depts = departments or _loaded_departments(con)
        if not target_depts:
            logger.warning("Aucun département chargé dans la base.")
            return

        communes = con.execute(
            f"SELECT DISTINCT code_insee, code_dept FROM parcelles "
            f"WHERE code_dept IN ({', '.join('?' * len(target_depts))})"
            f" ORDER BY code_dept, code_insee",
            target_depts,
        ).fetchall()

        logger.info(
            f"Calcul adjacence parcellaire : {len(communes)} commune(s) "
            f"sur {len(target_depts)} département(s)"
        )

        total_pairs = 0
        for i, (code_insee, code_dept) in enumerate(tqdm(communes, desc="Adjacence parcelles")):
            existing = con.execute(
                """SELECT COUNT(*) FROM parcelles_adjacency pa
                   JOIN parcelles p ON p.id = pa.id_a
                   WHERE p.code_insee = ? LIMIT 1""",
                [code_insee],
            ).fetchone()[0]
            if existing > 0:
                continue

            n = _compute_commune_adjacency(con, code_insee)
            total_pairs += n

            # Checkpoint périodique : vide le WAL et libère la RAM
            if (i + 1) % _CHECKPOINT_EVERY == 0:
                con.execute("CHECKPOINT")
                logger.debug(f"[parcel_adj] Checkpoint après {i + 1} communes ({total_pairs:,} paires).")

        con.execute("CHECKPOINT")
        logger.info(f"Adjacence parcellaire : {total_pairs:,} nouvelles paires calculées.")
    finally:
        con.close()
        # Nettoyage du répertoire temporaire
        import shutil
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS parcelles_adjacency (
            id_a VARCHAR NOT NULL,
            id_b VARCHAR NOT NULL,
            PRIMARY KEY (id_a, id_b)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcel_adj_a ON parcelles_adjacency (id_a)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcel_adj_b ON parcelles_adjacency (id_b)
    """)


def _loaded_departments(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute("SELECT DISTINCT code_dept FROM parcelles").fetchall()
    return [r[0] for r in rows]


def _compute_commune_adjacency(
    con: duckdb.DuckDBPyConnection,
    code_insee: str,
) -> int:
    """Calcule et insère les paires adjacentes pour une commune. Retourne le nombre de paires."""
    n_parcels = con.execute(
        "SELECT COUNT(*) FROM parcelles WHERE code_insee = ?", [code_insee]
    ).fetchone()[0]

    if n_parcels <= _TILE_THRESHOLD:
        _insert_adjacency_for_bbox(con, code_insee, lon_min=None, lon_max=None, lat_min=None, lat_max=None)
    else:
        # Grande commune : découpage en tuiles 2×2 pour limiter la charge mémoire
        bbox = con.execute("""
            SELECT MIN(ST_X(ST_Centroid(geometry))), MAX(ST_X(ST_Centroid(geometry))),
                   MIN(ST_Y(ST_Centroid(geometry))), MAX(ST_Y(ST_Centroid(geometry)))
            FROM parcelles WHERE code_insee = ?
        """, [code_insee]).fetchone()
        lon_min, lon_max, lat_min, lat_max = bbox
        lon_mid = (lon_min + lon_max) / 2
        lat_mid = (lat_min + lat_max) / 2
        tiles = [
            (lon_min, lon_mid, lat_min, lat_mid),
            (lon_mid, lon_max, lat_min, lat_mid),
            (lon_min, lon_mid, lat_mid, lat_max),
            (lon_mid, lon_max, lat_mid, lat_max),
        ]
        for (x0, x1, y0, y1) in tiles:
            _insert_adjacency_for_bbox(con, code_insee, x0, x1, y0, y1)

    n = con.execute(
        """SELECT COUNT(*) FROM parcelles_adjacency pa
           JOIN parcelles p ON p.id = pa.id_a
           WHERE p.code_insee = ?""",
        [code_insee],
    ).fetchone()[0]
    return n


def _insert_adjacency_for_bbox(
    con: duckdb.DuckDBPyConnection,
    code_insee: str,
    lon_min: float | None,
    lon_max: float | None,
    lat_min: float | None,
    lat_max: float | None,
) -> None:
    """Insère les paires adjacentes pour une commune, éventuellement filtrées par bbox du centroïde."""
    if lon_min is None:
        # Pas de filtre spatial — traitement intégral de la commune
        con.execute("""
            INSERT OR IGNORE INTO parcelles_adjacency (id_a, id_b)
            SELECT DISTINCT
                CASE WHEN a.id < b.id THEN a.id ELSE b.id END,
                CASE WHEN a.id < b.id THEN b.id ELSE a.id END
            FROM parcelles a
            JOIN parcelles b ON (
                a.code_insee = b.code_insee AND a.id != b.id
                AND ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
            )
            WHERE a.code_insee = ?
        """, [code_insee])
    else:
        # Filtre sur le centroïde de `a` pour restreindre la tuile
        # Les voisins de `b` peuvent déborder de la tuile : c'est voulu, on les capture quand même
        con.execute("""
            INSERT OR IGNORE INTO parcelles_adjacency (id_a, id_b)
            SELECT DISTINCT
                CASE WHEN a.id < b.id THEN a.id ELSE b.id END,
                CASE WHEN a.id < b.id THEN b.id ELSE a.id END
            FROM parcelles a
            JOIN parcelles b ON (
                a.code_insee = b.code_insee AND a.id != b.id
                AND ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
            )
            WHERE a.code_insee = ?
              AND ST_X(ST_Centroid(a.geometry)) BETWEEN ? AND ?
              AND ST_Y(ST_Centroid(a.geometry)) BETWEEN ? AND ?
        """, [code_insee, lon_min, lon_max, lat_min, lat_max])


def get_parcel_neighbors(
    parcel_ids: list[str],
    db_path: Path = DB_PATH,
) -> dict[str, set[str]]:
    """Retourne le graphe d'adjacence pour un ensemble d'IDs de parcelles.

    Utilise la table pré-calculée `parcelles_adjacency` si elle existe.
    """
    if not parcel_ids:
        return {}

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Vérifier que la table existe et est peuplée
        table_exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'parcelles_adjacency'"
        ).fetchone()[0]
        if not table_exists:
            return {}

        id_set = set(parcel_ids)
        placeholders = ", ".join("?" * len(parcel_ids))

        rows = con.execute(f"""
            SELECT id_a, id_b FROM parcelles_adjacency
            WHERE id_a IN ({placeholders}) OR id_b IN ({placeholders})
        """, parcel_ids + parcel_ids).fetchall()

        graph: dict[str, set[str]] = {}
        for id_a, id_b in rows:
            if id_a in id_set and id_b in id_set:
                graph.setdefault(id_a, set()).add(id_b)
                graph.setdefault(id_b, set()).add(id_a)
        return graph
    finally:
        con.close()


def has_precomputed_adjacency(db_path: Path = DB_PATH) -> bool:
    """Retourne True si la table d'adjacence pré-calculée existe et est peuplée."""
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
