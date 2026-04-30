"""Orchestrateur de recherche multi-phase avec scoring."""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import duckdb
from loguru import logger

from cadastre_finder.config import DB_PATH, DEFAULT_TOLERANCE_PCT, DEFAULT_TOP_N, MIN_ANCHOR_BUILT_M2
from cadastre_finder.search.ad_parser import parse_ad_text, SearchCriteria
from cadastre_finder.search.building_filter import filter_anchors
from cadastre_finder.search.combo_match import search_combos
from cadastre_finder.search.dpe_match import find_parcel_for_dpe, search_dpe
from cadastre_finder.search.models import ComboMatch, ParcelMatch
from cadastre_finder.search.strict_match import search_strict


def search_orchestrated(
    commune: str,
    surface_m2: float,
    living_surface: Optional[float] = None,
    dpe_label: Optional[str] = None,
    ges_label: Optional[str] = None,
    postal_code: Optional[str] = None,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    db_path: Path = DB_PATH,
) -> list[Union[ParcelMatch, ComboMatch]]:
    """Exécute le workflow de recherche en 4 phases."""
    
    all_results: list[Union[ParcelMatch, ComboMatch]] = []
    seen_ids: set[frozenset[str]] = set()

    def add_results(new_matches: list[Union[ParcelMatch, ComboMatch]], phase_bonus: float):
        for m in new_matches:
            ids = frozenset([m.id_parcelle] if isinstance(m, ParcelMatch) else m.ids)
            if ids not in seen_ids:
                m.score += phase_bonus
                all_results.append(m)
                seen_ids.add(ids)

    # Phase 1 : DPE Match & Strict Match (Anchors)
    logger.info("Phase 1 : DPE et Strict Match")
    
    # 1.1 DPE Match
    if living_surface:
        dpe_records = search_dpe(commune, living_surface, dpe_label, ges_label, db_path=db_path)
        dpe_matches = []
        for rec in dpe_records:
            match = find_parcel_for_dpe(rec, db_path=db_path)
            if match:
                match.score = 100.0 # Base score pour DPE
                dpe_matches.append(match)
        add_results(dpe_matches, phase_bonus=50.0) # Bonus Phase 1 DPE

    # 1.2 Strict Match (uniquement sur parcelles ancres)
    strict_matches = search_strict(commune, surface_m2, postal_code, tolerance_pct, db_path=db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD spatial;")
        anchor_strict = filter_anchors(strict_matches, con, MIN_ANCHOR_BUILT_M2)
        add_results(anchor_strict, phase_bonus=30.0)
    finally:
        con.close()

    if len(all_results) >= 3:
        logger.info(f"Phase 1 suffisante : {len(all_results)} résultats.")
        return _sort_and_limit(all_results, surface_m2)

    # Phase 2 : Local Combo Match (Anchors only)
    logger.info("Phase 2 : Local Combo Match")
    local_combos = search_combos(
        commune, surface_m2, postal_code, tolerance_pct,
        include_rank2=False, anchors_only=True, db_path=db_path
    )
    add_results(local_combos, phase_bonus=10.0)

    if len(all_results) >= 5:
        return _sort_and_limit(all_results, surface_m2)

    # Phase 3 : Neighboring Communes (Anchors only)
    logger.info("Phase 3 : Neighbors Combo Match")
    neighbor_combos = search_combos(
        commune, surface_m2, postal_code, tolerance_pct,
        include_rank2=True, anchors_only=True, db_path=db_path
    )
    add_results(neighbor_combos, phase_bonus=0.0)

    return _sort_and_limit(all_results, surface_m2)


def _sort_and_limit(results: list[Union[ParcelMatch, ComboMatch]], target_m2: float) -> list[Union[ParcelMatch, ComboMatch]]:
    """Trie les résultats par score décroissant et limite au top N."""
    # Phase 4 : Système de Scoring Final (déjà partiellement appliqué par phase_bonus)
    # On peut ajouter des ajustements ici
    for r in results:
        surf = r.contenance if isinstance(r, ParcelMatch) else r.total_contenance
        ecart_rel = abs(surf - target_m2) / target_m2
        # +30 points si la surface est à ± 5%
        if ecart_rel <= 0.05:
            r.score += 30.0
        
        # -20 points si l'emprise au sol est anormalement petite
        # On définit "anormalement petite" par rapport à MIN_ANCHOR_BUILT_M2
        barea = r.built_area if isinstance(r, ParcelMatch) else sum(p.built_area or 0 for p in r.parts)
        if barea and barea < MIN_ANCHOR_BUILT_M2:
            r.score -= 20.0

    results.sort(key=lambda r: (-r.score, abs((r.contenance if isinstance(r, ParcelMatch) else r.total_contenance) - target_m2)))
    
    # Mise à jour du rang final
    for i, r in enumerate(results):
        r.rank = i + 1
        
    return results[:DEFAULT_TOP_N]


def search_from_text(text: str, commune_hint: Optional[str] = None, db_path: Path = DB_PATH) -> list[Union[ParcelMatch, ComboMatch]]:
    """Point d'entrée principal : prend le texte de l'annonce et orchestre la recherche."""
    criteria = parse_ad_text(text)
    
    commune = commune_hint or criteria.commune
    if not commune:
        # On essaie d'extraire la commune du texte si non fournie
        # (Simplifié : on pourrait utiliser une liste de communes ou NLP)
        logger.warning("Aucune commune identifiée dans l'annonce ou fournie.")
        return []

    if not criteria.terrain_surface:
        logger.warning("Aucune surface de terrain identifiée dans l'annonce.")
        return []

    return search_orchestrated(
        commune=commune,
        surface_m2=criteria.terrain_surface,
        living_surface=criteria.living_surface,
        dpe_label=criteria.dpe_label,
        ges_label=criteria.ges_label,
        db_path=db_path
    )
