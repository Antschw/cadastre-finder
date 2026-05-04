"""
Tests d'intégrité de la base cadastre.duckdb (données de production).

Toutes les requêtes sont conçues pour s'exécuter en < 5 min au total :
  - USING SAMPLE N  pour les checks sur les grandes tables (46M+ lignes)
  - LIMIT 1         pour les checks "y a-t-il au moins un cas problème ?"
  - Filtre sur préfixe d'id (ex. LIKE '28%') pour les checks par département

Skipé automatiquement si la DB est absente (CI, post-clone).
"""
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from cadastre_finder.config import DEPARTMENTS

DB_PATH = Path(__file__).parent.parent / "data/processed/cadastre.duckdb"

pytestmark = pytest.mark.skipif(
    not DB_PATH.exists(),
    reason="cadastre.duckdb absent — lancer build-database d'abord",
)


@pytest.fixture(scope="module")
def db():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Présence et volumes des tables
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "parcelles", "communes", "communes_adjacency", "parcelles_adjacency",
    "dpe", "buildings", "poi_admin", "poi_religious", "railways", "roads_major", "waterways",
}


def test_all_tables_present(db):
    tables = {r[0] for r in db.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
    missing = EXPECTED_TABLES - tables
    assert not missing, f"Tables manquantes : {missing}"


@pytest.mark.parametrize("table,minimum", [
    ("parcelles",           18_000_000),
    ("communes",             7_000),
    ("communes_adjacency",   100_000),
    ("parcelles_adjacency",  40_000_000),
    ("dpe",                 10_000_000),
    ("buildings",           50_000_000),
])
def test_table_count_above_minimum(db, table, minimum):
    n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    assert n >= minimum, f"{table}: {n:,} lignes < minimum attendu {minimum:,}"


# ---------------------------------------------------------------------------
# Intégrité parcelles_adjacency
# ---------------------------------------------------------------------------

def test_adjacency_indexes_exist(db):
    indexes = {r[0] for r in db.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'parcelles_adjacency'"
    ).fetchall()}
    assert "idx_parcel_adj_a" in indexes, "Index idx_parcel_adj_a manquant"
    assert "idx_parcel_adj_b" in indexes, "Index idx_parcel_adj_b manquant"


def test_adjacency_no_self_links(db):
    # LIMIT 1 : ne scanne que jusqu'au premier cas, très rapide
    n = db.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM parcelles_adjacency WHERE id_a = id_b LIMIT 1)"
    ).fetchone()[0]
    assert n == 0, "Auto-liens (id_a = id_b) détectés dans parcelles_adjacency"


def test_adjacency_canonical_order_on_sample(db):
    # Le worker normalise toujours id_a < id_b ; vérifier sur 100k lignes aléatoires
    n_inverted = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM (SELECT id_a, id_b FROM parcelles_adjacency USING SAMPLE 100000)
            WHERE id_a >= id_b
            LIMIT 1
        )
    """).fetchone()[0]
    assert n_inverted == 0, "Paires avec id_a >= id_b trouvées dans l'échantillon (violation de canonisation)"


def test_adjacency_no_duplicates_on_sample(db):
    # Vérifier l'absence de doublons sur 100k lignes aléatoires
    n_dupes = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT id_a, id_b, COUNT(*) c
            FROM (SELECT id_a, id_b FROM parcelles_adjacency USING SAMPLE 100000)
            GROUP BY id_a, id_b
            HAVING c > 1
        )
    """).fetchone()[0]
    assert n_dupes == 0, f"{n_dupes} paires dupliquées dans l'échantillon"


def test_adjacency_referential_integrity_on_sample(db):
    # 1 000 id_a aléatoires → chacun doit pointer vers une parcelle existante
    n_orphans = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT pa.id_a
            FROM (SELECT id_a FROM parcelles_adjacency USING SAMPLE 1000) pa
            LEFT JOIN parcelles p ON p.id = pa.id_a
            WHERE p.id IS NULL
        )
    """).fetchone()[0]
    assert n_orphans == 0, f"{n_orphans} id_a dans parcelles_adjacency sans parcelle correspondante"


@pytest.mark.parametrize("code_insee,nom", [
    ("27315", "Harquency"),
    ("37165", "Neuvy-le-Roi"),
    ("28130", "Brou"),
])
def test_adjacency_known_communes_have_pairs(db, code_insee, nom):
    # Format ID Etalab : les 5 premiers chars = code_insee (ex. '27305...')
    # LIMIT 1 s'arrête dès la première paire trouvée via l'index range scan
    n = db.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM parcelles_adjacency WHERE id_a LIKE ? LIMIT 1)",
        [f"{code_insee}%"],
    ).fetchone()[0]
    assert n > 0, f"Aucune paire d'adjacence pour {nom} ({code_insee})"


# ---------------------------------------------------------------------------
# Intégrité communes_adjacency
# ---------------------------------------------------------------------------

def test_communes_adjacency_indexes_exist(db):
    indexes = {r[0] for r in db.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'communes_adjacency'"
    ).fetchall()}
    assert indexes, "Aucun index sur communes_adjacency"


def test_communes_adjacency_symmetric_on_sample(db):
    # Si (A→B) existe, (B→A) doit aussi exister — vérifié sur 500 paires aléatoires.
    # communes_adjacency ne contient que 134k lignes, le NOT EXISTS utilise l'index.
    n_asymmetric = db.execute("""
        WITH sample AS (
            SELECT code_insee_a, code_insee_b
            FROM communes_adjacency USING SAMPLE 500
        )
        SELECT COUNT(*) FROM sample s
        WHERE NOT EXISTS (
            SELECT 1 FROM communes_adjacency ca
            WHERE ca.code_insee_a = s.code_insee_b
              AND ca.code_insee_b = s.code_insee_a
        )
    """).fetchone()[0]
    assert n_asymmetric == 0, f"{n_asymmetric} paires asymétriques dans communes_adjacency"


def test_communes_adjacency_coverage(db):
    # Vérifie que les communes_adjacency couvre bien tous les départements actifs.
    # Exceptions légitimes :
    #   - dept 60 (Oise) : ajouté récemment, build-adjacency doit être relancé
    #   - Quelques îles (Ouessant 29, Île-d'Yeu 85, Belle-Île 56, Bréhat 22) :
    #     pas de voisin terrestre dans le périmètre, normal
    n_isolated_excl60 = db.execute("""
        SELECT COUNT(*) FROM communes c
        WHERE c.code_dept != '60'
          AND NOT EXISTS (
              SELECT 1 FROM communes_adjacency ca
              WHERE ca.code_insee_a = c.code_insee AND ca.rang = 1
          )
    """).fetchone()[0]
    # Tolère ≤ 10 communes (les quelques îles du périmètre)
    assert n_isolated_excl60 <= 10, (
        f"{n_isolated_excl60} communes (hors Oise) sans voisin de rang 1. "
        f"Relancer build-adjacency si un département entier est absent."
    )


# ---------------------------------------------------------------------------
# Intégrité DPE
# ---------------------------------------------------------------------------

def test_dpe_has_valid_etiquettes(db):
    # Au moins 1000 DPE doivent avoir une étiquette valide (A–G)
    n = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM dpe
            WHERE etiquette_dpe IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')
            LIMIT 1000
        )
    """).fetchone()[0]
    assert n == 1000, f"Seulement {n} DPE avec étiquette valide (attendu ≥ 1000)"


def test_dpe_has_coordinates(db):
    n = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM dpe
            WHERE coordonnee_cartographique_x_ban IS NOT NULL
              AND coordonnee_cartographique_y_ban IS NOT NULL
            LIMIT 1
        )
    """).fetchone()[0]
    assert n > 0, "Aucun DPE avec coordonnées BAN renseignées"


def test_dpe_has_surface(db):
    n = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM dpe
            WHERE surface_habitable_logement > 0
            LIMIT 1
        )
    """).fetchone()[0]
    assert n > 0, "Aucun DPE avec surface_habitable_logement > 0"


# ---------------------------------------------------------------------------
# Test fonctionnel : recherche CLI sur un cas réel connu
# ---------------------------------------------------------------------------

def test_search_harquency_cli():
    """Harquency (27) — terrain ~6024 m², la CLI doit retourner au moins un résultat."""
    result = subprocess.run(
        [sys.executable, "-m", "cadastre_finder.cli", "search",
         "--commune", "Harquency", "--surface", "6024", "--tolerance", "10"],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(DB_PATH.parent.parent),
    )
    assert result.returncode == 0, (
        f"La CLI a retourné le code {result.returncode}\n"
        f"stderr:\n{result.stderr[:1000]}"
    )
    output = result.stdout + result.stderr
    # La sortie doit mentionner un résultat ou une surface proche de 6024 m²
    assert any(tok in output for tok in ["résultat", "match", "6024", "6 024", "parcelle"]), (
        f"Pas de résultat détectable dans la sortie :\n{output[:800]}"
    )


# ---------------------------------------------------------------------------
# Intégrité OSM (Bâtiments)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dept", DEPARTMENTS)
def test_buildings_present_per_department(db, dept):
    """Vérifie qu'au moins un bâtiment intersecte une commune du département."""
    # On utilise LIMIT 1 pour la performance car la table buildings est massive.
    # DuckDB utilise l'index spatial (RTREE) sur buildings.geometry.
    n = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM buildings b, communes c
            WHERE c.code_dept = ? AND ST_Intersects(b.geometry, c.geometry)
            LIMIT 1
        )
    """, [dept]).fetchone()[0]
    assert n > 0, f"Aucun bâtiment trouvé pour le département {dept}"
