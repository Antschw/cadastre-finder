"""Tests du moteur de recherche — étape 1 (match strict)."""
import pytest
from cadastre_finder.search.strict_match import search_strict


def test_strict_match_exact_surface(tmp_db):
    """Une parcelle avec surface exacte doit être trouvée."""
    matches = search_strict("Mortagne-au-Perche", surface_m2=4200, db_path=tmp_db)
    assert len(matches) >= 1
    ids = [m.id_parcelle for m in matches]
    assert "61293000AB0042" in ids


def test_strict_match_below_min_surface_excluded(tmp_db):
    """Les parcelles < 2500 m² ne doivent jamais apparaître."""
    matches = search_strict("Mortagne-au-Perche", surface_m2=1200, db_path=tmp_db)
    assert all(m.contenance >= 2500 for m in matches)


def test_strict_match_wrong_commune_returns_empty(tmp_db):
    """Une commune inconnue retourne une liste vide."""
    matches = search_strict("Commune-Inexistante-XYZ", surface_m2=4200, db_path=tmp_db)
    assert matches == []


def test_strict_match_no_cross_commune(tmp_db):
    """Le match strict ne doit pas retourner de parcelles d'autres communes."""
    matches = search_strict("Mortagne-au-Perche", surface_m2=4200, db_path=tmp_db)
    for m in matches:
        assert m.code_insee == "61293"


def test_strict_match_score_exact_is_100(tmp_db):
    """Score 100 pour une correspondance exacte (tolérance=0)."""
    matches = search_strict("Mortagne-au-Perche", surface_m2=4200, db_path=tmp_db)
    exact = [m for m in matches if m.id_parcelle == "61293000AB0042"]
    assert exact
    assert exact[0].score == pytest.approx(100.0)


def test_strict_match_with_tolerance(tmp_db):
    """Avec une tolérance de 20%, les parcelles de 3500 m² doivent aussi apparaître."""
    matches = search_strict(
        "Mortagne-au-Perche", surface_m2=4200, tolerance_pct=20.0, db_path=tmp_db
    )
    ids = [m.id_parcelle for m in matches]
    assert "61293000AB0043" in ids  # 3500 m², dans les ±20% de 4200
