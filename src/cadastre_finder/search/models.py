"""Types partagés entre les modules de recherche."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class NeighborMode(str, Enum):
    """Périmètre d'extension géographique de la recherche."""
    NONE = "none"      # commune principale uniquement
    RANK1 = "rank1"    # principale + voisines directes
    RANK2 = "rank2"    # principale + rang 1 + rang 2
    RANK3 = "rank3"    # principale + rang 1 + rang 2 + rang 3


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
    built_area: Optional[float] = None
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None

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

    @property
    def google_maps_url(self) -> str:
        return (
            f"https://www.google.com/maps/@{self.centroid_lat},{self.centroid_lon}"
            f",17z/data=!3m1!1e3"
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
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None

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

    @property
    def google_maps_url(self) -> str:
        return (
            f"https://www.google.com/maps/@{self.centroid_lat},{self.centroid_lon}"
            f",17z/data=!3m1!1e3"
        )


@dataclass
class DPEPositionMatch:
    """Résultat d'une recherche en mode Positions DPE : position GPS depuis la base ADEME."""
    address: str
    postcode: str
    city: str
    code_insee: str
    surface_habitable: float
    centroid_lat: float
    centroid_lon: float
    score: float = 0.0
    rank: int = 0
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    date: Optional[str] = None

    @property
    def geoportail_url(self) -> str:
        return (
            f"https://www.geoportail.gouv.fr/carte?"
            f"c={self.centroid_lon},{self.centroid_lat}&z=17"
            f"&l0=GEOGRAPHICALGRIDSYSTEMS.MAPS"
        )

    @property
    def google_maps_url(self) -> str:
        return (
            f"https://www.google.com/maps/@{self.centroid_lat},{self.centroid_lon}"
            f",17z/data=!3m1!1e3"
        )
