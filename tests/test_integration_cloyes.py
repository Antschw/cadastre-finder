"""Test d'intégration — Propriété de Cloyes-sur-le-Loir (28103).

Cas réel : maison répartie sur 3 parcelles adjacentes
  AB0322 (442 m²) + AB0321 (1 540 m²) + AB0280 (1 273 m²) = 3 255 m²
  Annonce : surface ~3 300 m²

Note : "Cloyes-sur-le-Loir" a été fusionnée dans "Cloyes-les-Trois-Rivières" (28103).
La commune fusionnée contient de nombreuses sous-communes, donc le pipeline complet
(search_combos top 20) ne suffit pas à isoler la bonne parcelle — la cible se
retrouve en position ~2720 parmi des milliers de combos valides.
Ces tests vérifient les couches inférieures du pipeline : candidats, adjacence, DFS.

Ce test requiert la base de production avec les données du département 28.
Il est ignoré automatiquement si la base est absente.
"""
from __future__ import annotations

import pytest

from cadastre_finder.config import DB_PATH

# ── Données de la propriété cible ────────────────────────────────────────────
COMMUNE = "Cloyes-les-Trois-Rivières"   # nom actuel de la commune fusionnée
CODE_INSEE = "28103"
SURFACE_CIBLE = 3300
TOLERANCE_PCT = 5.0
PARCELLES_ATTENDUES = frozenset({"28103000AB0322", "28103000AB0321", "28103000AB0280"})
SURFACE_REELLE = 442 + 1540 + 1273     # 3 255 m²


def _db_has_dept28() -> bool:
    """Vérifie que la base contient bien des données pour le département 28."""
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        n = con.execute(
            "SELECT COUNT(*) FROM parcelles WHERE code_dept = '28' LIMIT 1"
        ).fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


needs_dept28 = pytest.mark.skipif(
    not DB_PATH.exists() or not _db_has_dept28(),
    reason="Base de production avec département 28 requise",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_ctx():
    """Retourne (candidates, graph) pour la commune 28103."""
    import duckdb
    from cadastre_finder.search.combo_match import _fetch_candidates, _get_adjacency, MIN_PART_M2, MAX_PARTS

    surface = float(SURFACE_CIBLE)
    tol = TOLERANCE_PCT
    max_parts = MAX_PARTS
    max_single = surface * (1 + tol / 100) - (max_parts - 1) * MIN_PART_M2
    min_single = max(MIN_PART_M2, int(surface / (max_parts * 4)))

    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    all_codes = [CODE_INSEE]
    commune_noms = {CODE_INSEE: COMMUNE}
    candidates = _fetch_candidates(con, all_codes, min_single, max_single, commune_noms)
    graph = _get_adjacency(con, candidates, all_codes, max_single)
    con.close()
    return candidates, graph


# ── Tests ─────────────────────────────────────────────────────────────────────

@needs_dept28
def test_cloyes_candidates_present(pipeline_ctx):
    """Les 3 parcelles cibles doivent faire partie des candidats."""
    candidates, _ = pipeline_ctx
    ids_found = {c.id_parcelle for c in candidates}
    missing = PARCELLES_ATTENDUES - ids_found
    assert not missing, f"Parcelles manquantes dans les candidats : {missing}"


@needs_dept28
def test_cloyes_adjacency_correct(pipeline_ctx):
    """AB0280 ↔ AB0321 et AB0321 ↔ AB0322 doivent être dans le graphe d'adjacence."""
    _, graph = pipeline_ctx
    ab0280 = "28103000AB0280"
    ab0321 = "28103000AB0321"
    ab0322 = "28103000AB0322"

    assert ab0321 in graph.get(ab0280, set()), "AB0280 et AB0321 ne sont pas adjacents"
    assert ab0280 in graph.get(ab0321, set()), "AB0321 et AB0280 ne sont pas adjacents (symétrie)"
    assert ab0322 in graph.get(ab0321, set()), "AB0321 et AB0322 ne sont pas adjacents"
    assert ab0321 in graph.get(ab0322, set()), "AB0322 et AB0321 ne sont pas adjacents (symétrie)"


@needs_dept28
def test_cloyes_dfs_finds_combo(pipeline_ctx):
    """Le DFS doit trouver le combo cible en cherchant parmi suffisamment de résultats.

    La commune fusionnée a ~6 900 candidats → la cible se positionne ~2 700e.
    On vérifie que le DFS peut la trouver avec top_n=3000 (test de non-régression
    pour le bug 'early-exit DFS').
    """
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=3000, min_compactness=0.15)
    ids_par_combo = [frozenset(c.ids) for c in combos]
    assert PARCELLES_ATTENDUES in ids_par_combo, (
        f"Combo {sorted(PARCELLES_ATTENDUES)} non trouvé dans le DFS (top_n=3000).\n"
        f"Total combos trouvés : {len(combos)}"
    )


@needs_dept28
def test_cloyes_combo_surface(pipeline_ctx):
    """Le combo trouvé par le DFS doit avoir la surface exacte de 3 255 m²."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=3000, min_compactness=0.15)
    for c in combos:
        if frozenset(c.ids) == PARCELLES_ATTENDUES:
            assert c.total_contenance == SURFACE_REELLE, (
                f"Surface attendue {SURFACE_REELLE} m², obtenu {c.total_contenance} m²"
            )
            return
    pytest.skip("Combo non trouvé — testé par test_cloyes_dfs_finds_combo")


@needs_dept28
def test_cloyes_no_duplicate_ids(pipeline_ctx):
    """Aucun combo ne doit être sous-ensemble ou sur-ensemble d'un autre après dédup."""
    from cadastre_finder.search.combo_match import _find_combos_dfs, _deduplicate_combos

    candidates, graph = pipeline_ctx
    raw = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=60)
    combos = _deduplicate_combos(raw, SURFACE_CIBLE)

    sets = [frozenset(c.ids) for c in combos]
    for i, a in enumerate(sets):
        for j, b in enumerate(sets):
            if i >= j:
                continue
            assert not a.issubset(b) and not a.issuperset(b), (
                f"Combos redondants détectés :\n"
                f"  combo[{i}] = {sorted(a)}\n"
                f"  combo[{j}] = {sorted(b)}"
            )
