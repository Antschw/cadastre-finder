"""Types partagés entre les modules de recherche."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ParcelMatch:
    """Résultat d'une recherche de parcelle cadastrale (parcelle unique)."""
    id_parcelle: str
    code_insee: str
    nom_commune: str
    contenance: int
    centroid_lat: float
    centroid_lon: float
    geometry_geojson: str
    score: float = 0.0
    rank: int = 0

    @property
    def street_view_url(self) -> str:
        return (
            f"https://www.google.com/maps/@?api=1&map_action=pano"
            f"&viewpoint={self.centroid_lat},{self.centroid_lon}"
        )

    @property
    def geoportail_url(self) -> str:
        return (
            f"https://www.geoportail.gouv.fr/carte?"
            f"c={self.centroid_lon},{self.centroid_lat}&z=17"
            f"&l0=GEOGRAPHICALGRIDSYSTEMS.MAPS"
        )


@dataclass
class ComboMatch:
    """Résultat d'une recherche : combinaison de parcelles adjacentes."""
    parts: list[ParcelMatch]
    total_contenance: int
    centroid_lat: float
    centroid_lon: float
    combined_geojson: str
    score: float = 0.0
    rank: int = 0
    compactness: float = 0.0   # Polsby-Popper [0, 1]

    @property
    def nb_parcelles(self) -> int:
        return len(self.parts)

    @property
    def ids(self) -> list[str]:
        return [p.id_parcelle for p in self.parts]

    @property
    def label(self) -> str:
        return " + ".join(self.ids)

    @property
    def nom_commune(self) -> str:
        return self.parts[0].nom_commune if self.parts else ""

    @property
    def code_insee(self) -> str:
        return self.parts[0].code_insee if self.parts else ""

    @property
    def geoportail_url(self) -> str:
        return (
            f"https://www.geoportail.gouv.fr/carte?"
            f"c={self.centroid_lon},{self.centroid_lat}&z=17"
            f"&l0=GEOGRAPHICALGRIDSYSTEMS.MAPS"
        )
