"""Tests du module de géocodage (recherche locale uniquement — pas de réseau)."""
import pytest
from unittest.mock import patch
from cadastre_finder.utils.geocoding import resolve_commune, CommuneInfo


def test_resolve_commune_local_exact(tmp_db):
    """Résolution exacte depuis la table locale."""
    result = resolve_commune("Mortagne-au-Perche", db_path=tmp_db)
    assert result.unique is not None
    assert result.unique.code_insee == "61293"


def test_resolve_commune_local_case_insensitive(tmp_db):
    """La recherche locale est insensible à la casse."""
    result = resolve_commune("mortagne-au-perche", db_path=tmp_db)
    assert result.best is not None
    assert result.best.code_insee == "61293"


def test_resolve_commune_unknown_uses_api(tmp_db):
    """Une commune inconnue localement doit interroger l'API Adresse."""
    api_result = [CommuneInfo(code_insee="61293", nom="Mortagne-au-Perche", code_dept="61", score=0.9)]
    with patch("cadastre_finder.utils.geocoding._search_api", return_value=api_result) as mock_api:
        result = resolve_commune("Commune-Totalement-Inconnue", db_path=tmp_db)
        mock_api.assert_called_once()
    assert result.best is not None


def test_resolve_commune_not_found(tmp_db):
    """Commune introuvable → ResolveResult vide."""
    with patch("cadastre_finder.utils.geocoding._search_api", return_value=[]):
        result = resolve_commune("ZZZ-Inexistant", db_path=tmp_db)
    assert result.candidates == []
    assert result.unique is None
    assert result.best is None
