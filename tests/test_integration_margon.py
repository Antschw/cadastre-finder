"""Test d'intégration — Propriété à Arcisses (28236), commune "Margon" résolue.

Cas réel : demeure du XVIe siècle répartie sur 2 parcelles adjacentes (section 0B, préfixe 063)
  282360630B0083 (1 623 m²) + 282360630B0081 (250 m²) = 1 873 m²
  Annonce : terrain paysagé d'environ 2 000 m², surface habitable 222 m², DPE D / GES B
  DPE en base : "2 La Gentillere, 28400 Arcisses" (197.1 m², DPE D / GES B)

Note : la commune "Margon" est automatiquement résolue vers "Arcisses" (INSEE 28236)
par le géocodeur (Géoplateforme). Ce test valide que le pipeline DPE-led + combo
fonctionne pour des propriétés dont le DPE est disponible en base locale.

Ce test requiert la base de production avec les données du département 28.
Il est ignoré automatiquement si la base est absente.
"""
from __future__ import annotations

import pytest

from cadastre_finder.config import DB_PATH

# ── Données de la propriété cible ────────────────────────────────────────────
COMMUNE = "Arcisses"
CODE_INSEE = "28236"
SURFACE_CIBLE = 2000
TOLERANCE_PCT = 10.0
PARCELLES_ATTENDUES = frozenset({"282360630B0083", "282360630B0081"})
SURFACE_REELLE = 1623 + 250   # 1 873 m²


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
    """Retourne (candidates, graph) pour la commune 28236 (Arcisses)."""
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
def test_margon_candidates_present(pipeline_ctx):
    """Les 2 parcelles cibles doivent faire partie des candidats."""
    candidates, _ = pipeline_ctx
    ids_found = {c.id_parcelle for c in candidates}
    missing = PARCELLES_ATTENDUES - ids_found
    assert not missing, f"Parcelles manquantes dans les candidats : {missing}"


@needs_dept28
def test_margon_adjacency_correct(pipeline_ctx):
    """282360630B0083 ↔ 282360630B0081 doivent être dans le graphe d'adjacence."""
    _, graph = pipeline_ctx
    p083 = "282360630B0083"
    p081 = "282360630B0081"

    assert p081 in graph.get(p083, set()), "0B0083 et 0B0081 ne sont pas adjacents"
    assert p083 in graph.get(p081, set()), "0B0081 et 0B0083 ne sont pas adjacents (symétrie)"


@needs_dept28
def test_margon_dfs_finds_combo(pipeline_ctx):
    """Le DFS doit trouver le combo des 2 parcelles cibles.

    Arcisses a ~2 600 candidats → la cible se positionne ~2 003e.
    On vérifie que le DFS peut la trouver avec top_n=3000.
    """
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=3000)
    ids_par_combo = [frozenset(c.ids) for c in combos]
    assert PARCELLES_ATTENDUES in ids_par_combo, (
        f"Combo {sorted(PARCELLES_ATTENDUES)} non trouvé dans le DFS (top_n=3000).\n"
        f"Total combos trouvés : {len(combos)}"
    )


@needs_dept28
def test_margon_combo_surface(pipeline_ctx):
    """Le combo trouvé par le DFS doit avoir la surface exacte de 1 873 m²."""
    from cadastre_finder.search.combo_match import _find_combos_dfs

    candidates, graph = pipeline_ctx
    combos = _find_combos_dfs(candidates, graph, SURFACE_CIBLE, TOLERANCE_PCT, 6, top_n=3000)
    for c in combos:
        if frozenset(c.ids) == PARCELLES_ATTENDUES:
            assert c.total_contenance == SURFACE_REELLE, (
                f"Surface attendue {SURFACE_REELLE} m², obtenu {c.total_contenance} m²"
            )
            return
    pytest.skip("Combo non trouvé — testé par test_margon_dfs_finds_combo")


@needs_dept28
def test_margon_dpe_led_pipeline():
    """Vérification de bout en bout : le pipeline DPE-led trouve la propriété d'Arcisses.

    Ce test valide que search_orchestrated avec DPE D / GES B retourne au moins un
    résultat incluant la parcelle principale 282360630B0083.
    """
    from cadastre_finder.search.orchestrator import search_orchestrated
    from cadastre_finder.search.models import ComboMatch, NeighborMode, ParcelMatch

    results = search_orchestrated(
        commune=COMMUNE,
        surface_m2=float(SURFACE_CIBLE),
        living_surface=222.0,
        dpe_label="D",
        ges_label="B",
        postal_code="28160",
        tolerance_pct=15.0,
        neighbor_mode=NeighborMode.NONE,
        db_path=DB_PATH,
    )

    assert results, "Aucun résultat retourné par search_orchestrated pour Arcisses"

    all_ids: set[str] = set()
    for r in results:
        if isinstance(r, ComboMatch):
            all_ids.update(r.ids)
        else:
            all_ids.add(r.id_parcelle)

    assert "282360630B0083" in all_ids, (
        f"Parcelle principale 282360630B0083 absente des résultats.\n"
        f"IDs trouvés : {sorted(all_ids)}"
    )
