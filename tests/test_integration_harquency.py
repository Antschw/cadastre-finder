"""Test d'intégration — Propriété de Harquency (Les Andelys, 27315).

Cas réel : maison répartie sur 5 parcelles adjacentes
  AB0172 (492 m²) + AB0006 (1237 m²) + AB0007 (4090 m²) + AB0173 (152 m²) + AB0174 (53 m²) = 6024 m²
  Annonce : surface 6024 m² (tolérance 0)
"""
from __future__ import annotations

import pytest

from cadastre_finder.config import DB_PATH

# ── Données de la propriété cible ────────────────────────────────────────────
COMMUNE = "Les Andelys"
CODE_INSEE = "27315"
SURFACE_CIBLE = 6024
TOLERANCE_PCT = 0.0
PARCELLES_ATTENDUES = frozenset({
    "27315000AB0172",
    "27315000AB0006",
    "27315000AB0007",
    "27315000AB0173",
    "27315000AB0174"
})
SURFACE_REELLE = 492 + 1237 + 4090 + 152 + 53  # 6024 m²


def _db_has_dept27() -> bool:
    """Vérifie que la base contient bien des données pour le département 27."""
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        n = con.execute(
            "SELECT COUNT(*) FROM parcelles WHERE code_dept = '27' LIMIT 1"
        ).fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


needs_dept27 = pytest.mark.skipif(
    not DB_PATH.exists() or not _db_has_dept27(),
    reason="Base de production avec département 27 requise",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_ctx():
    """Retourne (candidates, graph) pour la commune 27315."""
    import duckdb
    from cadastre_finder.search.combo_match import _fetch_candidates, _get_adjacency, MIN_PART_M2, MAX_PARTS

    surface = float(SURFACE_CIBLE)
    tol = TOLERANCE_PCT
    max_parts = MAX_PARTS
    # Calcul des bornes de contenance pour les candidats
    max_single = surface * (1 + tol / 100) - (max_parts - 1) * MIN_PART_M2
    min_single = MIN_PART_M2

    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    all_codes = [CODE_INSEE]
    commune_noms = {CODE_INSEE: COMMUNE}
    candidates = _fetch_candidates(con, all_codes, min_single, max_single, commune_noms)
    graph = _get_adjacency(con, candidates, all_codes, max_single)
    con.close()
    return candidates, graph


# ── Tests ─────────────────────────────────────────────────────────────────────

@needs_dept27
def test_harquency_candidates_present(pipeline_ctx):
    """Les 5 parcelles cibles doivent faire partie des candidats."""
    candidates, _ = pipeline_ctx
    ids_found = {c.id_parcelle for c in candidates}
    missing = PARCELLES_ATTENDUES - ids_found
    assert not missing, f"Parcelles manquantes dans les candidats : {missing}"


@needs_dept27
def test_harquency_adjacency_correct(pipeline_ctx):
    """Vérifie que les parcelles attendues sont connectées dans le graphe."""
    _, graph = pipeline_ctx
    
    # On vérifie au moins quelques connexions critiques
    # AB0006 et AB0007 sont les plus grosses, elles devraient être adjacentes
    ab0006 = "27315000AB0006"
    ab0007 = "27315000AB0007"
    
    assert ab0007 in graph.get(ab0006, set()), f"{ab0006} et {ab0007} ne sont pas adjacents"


@needs_dept27
def test_harquency_dfs_finds_combo(pipeline_ctx):
    """Le DFS doit trouver le combo cible."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=500)
    ids_par_combo = [frozenset(c.ids) for c in combos]
    assert PARCELLES_ATTENDUES in ids_par_combo, (
        f"Combo {sorted(PARCELLES_ATTENDUES)} non trouvé dans le DFS.\n"
        f"Total combos trouvés : {len(combos)}"
    )


@needs_dept27
def test_harquency_combo_surface(pipeline_ctx):
    """Le combo trouvé par le DFS doit avoir la surface exacte de 6024 m²."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=500)
    for c in combos:
        if frozenset(c.ids) == PARCELLES_ATTENDUES:
            assert c.total_contenance == SURFACE_REELLE, (
                f"Surface attendue {SURFACE_REELLE} m², obtenu {c.total_contenance} m²"
            )
            return
    pytest.skip("Combo non trouvé — testé par test_harquency_dfs_finds_combo")
