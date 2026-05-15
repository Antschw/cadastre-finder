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
    dpe_date: Optional[str] = None
    commune: Optional[str] = None
    postal_code: Optional[str] = None


def _parse_surface(s: str) -> float:
    return float(s.replace("\xa0", "").replace(" ", "").replace(" ", ""))


def parse_ad_text(text: str) -> SearchCriteria:
    """Extrait les surfaces, labels DPE/GES, commune et code postal d'un texte brut."""
    criteria = SearchCriteria()

    # Nettoyage (espaces insécables, etc.)
    text = text.replace("\xa0", " ").replace(" ", " ")

    # ── 1. Surface Terrain ────────────────────────────────────────────────────
    # Patterns : "terrain de 2500 m²", "parcelle de 3 000m²",
    #            "3 104 m² de terrain", "3 104 m² terrain"
    terrain_patterns = [
        # keyword AVANT le nombre
        r"(?i)(?:terrain|parcelle|foncier|jardin|parc|clos)\s+(?:de\s+)?(\d[\d\s ]*)\s*m[2²]",
        # nombre AVANT le keyword (ex: "3 104 m² de terrain")
        r"(?i)(\d[\d\s ]*)\s*m[2²]\s+(?:de\s+)?(?:terrain|parcelle|foncier)",
    ]
    for pat in terrain_patterns:
        m = re.search(pat, text)
        if m:
            try:
                criteria.terrain_surface = _parse_surface(m.group(1))
                break
            except ValueError:
                pass

    # ── 2. Surface Habitable ─────────────────────────────────────────────────
    # Priorité : "m² habitables" > keyword avant le nombre
    living_patterns = [
        # "200 m² habitables" ou "150 m² de surface habitable"
        r"(?i)(\d[\d\s ]*)\s*m[2²]\s+(?:de\s+)?(?:surface\s+)?habitable",
        # keyword AVANT le nombre
        r"(?i)(?:maison|villa|pavillon|propriété|longère)\s+(?:de\s+)?(\d[\d\s ]*)\s*m[2²]",
        # fallback générique "surface" ou "habitable" suivi d'un nombre
        r"(?i)(?:surface|habitable).*?(\d[\d\s ]*)\s*m[2²]",
    ]
    for pat in living_patterns:
        m = re.search(pat, text)
        if m:
            try:
                criteria.living_surface = _parse_surface(m.group(1))
                break
            except ValueError:
                pass

    # ── 3. DPE Label (A-G) ────────────────────────────────────────────────────
    dpe_m = re.search(
        r"(?i)(?:DPE|classe\s+énergie|performance\s+énergétique)\s*[:\s]*([A-G])(?:\s|$|\W)",
        text,
    )
    if dpe_m:
        criteria.dpe_label = dpe_m.group(1).upper()

    # ── 4. GES Label (A-G) ────────────────────────────────────────────────────
    ges_m = re.search(
        r"(?i)(?:GES|classe\s+climat)\s*[:\s]*([A-G])(?:\s|$|\W)",
        text,
    )
    if ges_m:
        criteria.ges_label = ges_m.group(1).upper()

    # ── 5. Date DPE ───────────────────────────────────────────────────────────
    date_m = re.search(
        r"(?i)date\s+du\s+dpe\s*[:\s]*(\d{2})/(\d{2})/(\d{4})",
        text,
    )
    if date_m:
        d, mo, y = date_m.groups()
        criteria.dpe_date = f"{y}-{mo}-{d}"

    # ── 6. Code postal + Commune ─────────────────────────────────────────────
    # Pattern le plus fiable : "Ville (CP)" ou "Ville CP" en fin de ligne
    city_cp_m = re.search(
        r"([A-ZÀ-ÿ][A-Za-zÀ-ÿ\s\-\'\.]{2,40}?)\s*\((\d{5})\)",
        text,
    )
    if city_cp_m:
        criteria.commune = city_cp_m.group(1).strip()
        criteria.postal_code = city_cp_m.group(2)
    else:
        # Fallback : cherche "à Ville" ou "commune de Ville" suivi optionnellement d'un CP
        city_m = re.search(
            r"(?i)(?:à|situé\s+à|commune\s+de|ville\s+de)\s+"
            r"([A-ZÀ-ÿ][A-Za-zÀ-ÿ\s\-\'\.]{2,40}?)(?:\s*[\(\,\.\n]|$)",
            text,
        )
        if city_m:
            criteria.commune = city_m.group(1).strip()

        # Code postal seul (5 chiffres, commence par [0-9][0-9])
        cp_m = re.search(r"\b([0-9]{5})\b", text)
        if cp_m:
            criteria.postal_code = cp_m.group(1)

    return criteria
