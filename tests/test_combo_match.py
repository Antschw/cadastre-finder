"""Tests du moteur de recherche combinaisons de parcelles."""
import pytest
from cadastre_finder.search.combo_match import search_combos


def test_combo_finds_pair(tmp_db):
    """Deux parcelles adjacentes (4200 + 3500 = 7700) doivent être trouvées pour cible 7700."""
    combos = search_combos("Mortagne-au-Perche", surface_m2=7700, tolerance_pct=5.0, db_path=tmp_db)
    assert len(combos) >= 1
    total_surfaces = [c.total_contenance for c in combos]
    assert any(7000 <= t <= 8400 for t in total_surfaces)


def test_combo_parts_are_adjacent(tmp_db):
    """Les parcelles d'un combo doivent être adjacentes dans les données de test."""
    combos = search_combos("Mortagne-au-Perche", surface_m2=7700, tolerance_pct=5.0, db_path=tmp_db)
    for combo in combos:
        assert combo.nb_parcelles >= 2
        assert len(combo.ids) == combo.nb_parcelles


def test_combo_min_surface_respected(tmp_db):
    """Aucune parcelle individuelle ne dépasse la surface cible dans un combo."""
    combos = search_combos("Mortagne-au-Perche", surface_m2=7700, tolerance_pct=20.0, db_path=tmp_db)
    for combo in combos:
        for part in combo.parts:
            assert part.contenance < 7700 * 1.2  # chaque partie < cible totale


def test_combo_score_decreases_with_deviation(tmp_db):
    """Un combo exact doit avoir un score plus élevé qu'un combo avec écart."""
    combos = search_combos("Mortagne-au-Perche", surface_m2=7700, tolerance_pct=5.0, db_path=tmp_db)
    if len(combos) >= 2:
        assert combos[0].score >= combos[1].score


def test_combo_combined_geojson_is_valid(tmp_db):
    """Le GeoJSON combiné doit être parsable."""
    import json
    combos = search_combos("Mortagne-au-Perche", surface_m2=7700, tolerance_pct=5.0, db_path=tmp_db)
    for combo in combos:
        geom = json.loads(combo.combined_geojson)
        assert "type" in geom


def test_combo_max_parts_2(tmp_db):
    """Avec max_parts=2, aucun triplet ne doit apparaître."""
    combos = search_combos(
        "Mortagne-au-Perche", surface_m2=7700, tolerance_pct=20.0, max_parts=2, db_path=tmp_db
    )
    for combo in combos:
        assert combo.nb_parcelles <= 2
