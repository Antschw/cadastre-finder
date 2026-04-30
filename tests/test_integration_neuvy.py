"""Test d'intégration — Propriété de Neuvy-le-Roi (37170).

Cas réel : maison répartie sur 6 parcelles adjacentes
  0D1436 (3135 m²) + 0D1290 (787 m²) + 0D1435 (1363 m²) + 0D1437 (15 m²) + 0D1434 (92 m²) + 0D1289 (23 m²) = 5415 m²
  Annonce : surface 5415 m² (tolérance 0)
"""
from __future__ import annotations

import pytest

from cadastre_finder.config import DB_PATH

# ── Données de la propriété cible ────────────────────────────────────────────
COMMUNE = "Neuvy-le-Roi"
CODE_INSEE = "37170"
SURFACE_CIBLE = 5415
TOLERANCE_PCT = 0.0
PARCELLES_ATTENDUES = frozenset({
    "371700000D1436",
    "371700000D1290",
    "371700000D1435",
    "371700000D1437",
    "371700000D1434",
    "371700000D1289"
})
SURFACE_REELLE = 3135 + 787 + 1363 + 15 + 92 + 23  # 5415 m²


def _db_has_dept37() -> bool:
    """Vérifie que la base contient bien des données pour le département 37."""
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        n = con.execute(
            "SELECT COUNT(*) FROM parcelles WHERE code_dept = '37' LIMIT 1"
        ).fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


needs_dept37 = pytest.mark.skipif(
    not DB_PATH.exists() or not _db_has_dept37(),
    reason="Base de production avec département 37 requise",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_ctx():
    """Retourne (candidates, graph) pour la commune 37170."""
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

@needs_dept37
def test_neuvy_candidates_present(pipeline_ctx):
    """Les 6 parcelles cibles doivent faire partie des candidats."""
    candidates, _ = pipeline_ctx
    ids_found = {c.id_parcelle for c in candidates}
    missing = PARCELLES_ATTENDUES - ids_found
    assert not missing, f"Parcelles manquantes dans les candidats : {missing}"


@needs_dept37
def test_neuvy_adjacency_correct(pipeline_ctx):
    """Vérifie que les parcelles attendues sont connectées dans le graphe."""
    _, graph = pipeline_ctx
    
    # 0D1436 et 0D1435 sont les plus grosses
    p1 = "371700000D1436"
    p2 = "371700000D1435"
    
    assert p2 in graph.get(p1, set()), f"{p1} et {p2} ne sont pas adjacents"


@needs_dept37
def test_neuvy_dfs_finds_combo(pipeline_ctx):
    """Le DFS doit trouver le combo cible."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=500)
    ids_par_combo = [frozenset(c.ids) for c in combos]
    assert PARCELLES_ATTENDUES in ids_par_combo, (
        f"Combo {sorted(PARCELLES_ATTENDUES)} non trouvé dans le DFS.\n"
        f"Total combos trouvés : {len(combos)}"
    )


@needs_dept37
def test_neuvy_combo_surface(pipeline_ctx):
    """Le combo trouvé par le DFS doit avoir la surface exacte de 5415 m²."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=500)
    for c in combos:
        if frozenset(c.ids) == PARCELLES_ATTENDUES:
            assert c.total_contenance == SURFACE_REELLE, (
                f"Surface attendue {SURFACE_REELLE} m², obtenu {c.total_contenance} m²"
            )
            return
    pytest.skip("Combo non trouvé — testé par test_neuvy_dfs_finds_combo")
