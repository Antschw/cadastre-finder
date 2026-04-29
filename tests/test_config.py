"""Tests du module de configuration."""
from cadastre_finder.config import DEPARTMENTS, MIN_TERRAIN_M2, CRS_LAMBERT93, CRS_WGS84


def test_departments_count():
    assert len(DEPARTMENTS) == 20


def test_departments_include_key_depts():
    assert "61" in DEPARTMENTS  # Orne
    assert "76" in DEPARTMENTS  # Seine-Maritime
    assert "86" in DEPARTMENTS  # Vienne


def test_min_surface():
    assert MIN_TERRAIN_M2 == 2500


def test_projections():
    assert "2154" in CRS_LAMBERT93
    assert "4326" in CRS_WGS84
