"""Module d'extraction des critères de recherche depuis le texte d'une annonce."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class SearchCriteria:
    """Critères extraits d'une annonce."""
    terrain_surface: Optional[float] = None
    living_surface: Optional[float] = None
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    commune: Optional[str] = None


def parse_ad_text(text: str) -> SearchCriteria:
    """Extrait les surfaces et labels DPE/GES d'un texte brut."""
    criteria = SearchCriteria()

    # Nettoyage minimal (espaces insécables, etc.)
    text = text.replace('\xa0', ' ')

    # 1. Surface Terrain (ex: "terrain de 2500 m2", "parcelle de 3 000m²")
    # On cherche un chiffre après "terrain" ou "parcelle"
    terrain_match = re.search(r"(?i)(?:terrain|parcelle|foncier|jardin|parc|clos).*?(\d[\d\s]*)\s*m[2²]", text)
    if terrain_match:
        val = terrain_match.group(1).replace(" ", "")
        criteria.terrain_surface = float(val)

    # 2. Surface Habitable (ex: "maison de 130 m2", "habitable 150m²")
    living_match = re.search(r"(?i)(?:maison|habitable|surface|villa|pavillon|propriété|longère).*?(\d[\d\s]*)\s*m[2²]", text)
    if living_match:
        val = living_match.group(1).replace(" ", "")
        criteria.living_surface = float(val)

    # 3. DPE Label (A-G)
    # Cherche "DPE : C" ou "Classe énergie D"
    dpe_match = re.search(r"(?i)(?:DPE|classe énergie|performance énergétique)\s*[:\s]*([A-G])(?:\s|$|\W)", text)
    if dpe_match:
        criteria.dpe_label = dpe_match.group(1).upper()

    # 4. GES Label (A-G)
    # Cherche "GES : B" ou "Classe climat A"
    ges_match = re.search(r"(?i)(?:GES|classe climat)\s*[:\s]*([A-G])(?:\s|$|\W)", text)
    if ges_match:
        criteria.ges_label = ges_match.group(1).upper()

    return criteria
