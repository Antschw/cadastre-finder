"""Tests du moteur de recherche — étape 2 (voisines + tolérance)."""
from cadastre_finder.search.neighbor_match import search_with_neighbors


def test_neighbor_match_includes_neighboring_communes(tmp_db):
    """Avec voisines, les parcelles des communes adjacentes doivent apparaître."""
    matches = search_with_neighbors(
        "Mortagne-au-Perche", surface_m2=4200, tolerance_pct=5.0, db_path=tmp_db
    )
    codes_communes = {m.code_insee for m in matches}
    # La commune annoncée ET sa voisine doivent être présentes
    assert "61293" in codes_communes
    assert "61100" in codes_communes


def test_neighbor_match_commune_annoncee_has_higher_score(tmp_db):
    """Les parcelles de la commune annoncée doivent avoir un score plus élevé."""
    matches = search_with_neighbors(
        "Mortagne-au-Perche", surface_m2=4200, tolerance_pct=5.0, db_path=tmp_db
    )
    by_commune = {}
    for m in matches:
        by_commune.setdefault(m.code_insee, []).append(m.score)

    if "61293" in by_commune and "61100" in by_commune:
        max_score_main = max(by_commune["61293"])
        max_score_neighbor = max(by_commune["61100"])
        assert max_score_main > max_score_neighbor


def test_neighbor_match_min_surface_filter(tmp_db):
    """Les parcelles < 2500 m² ne doivent jamais apparaître."""
    matches = search_with_neighbors(
        "Mortagne-au-Perche", surface_m2=4200, tolerance_pct=50.0, db_path=tmp_db
    )
    assert all(m.contenance >= 2500 for m in matches)


def test_neighbor_match_top_n(tmp_db):
    """Le paramètre top_n limite le nombre de résultats."""
    matches = search_with_neighbors(
        "Mortagne-au-Perche", surface_m2=4200, tolerance_pct=50.0, top_n=2, db_path=tmp_db
    )
    assert len(matches) <= 2
