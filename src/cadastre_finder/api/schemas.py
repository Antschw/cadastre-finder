"""Schémas Pydantic pour la sérialisation JSON de l'API FastAPI."""
from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field

from cadastre_finder.search.models import ComboMatch, DPEPositionMatch, ParcelMatch


# ---------------------------------------------------------------------------
# Schémas de réponse
# ---------------------------------------------------------------------------

class ComboPartSchema(BaseModel):
    id_parcelle: str
    contenance: int
    built_area: Optional[float] = None


class ParcelMatchSchema(BaseModel):
    type: Literal["parcel"] = "parcel"
    id_parcelle: str
    code_insee: str
    nom_commune: str
    contenance: int
    centroid_lat: float
    centroid_lon: float
    geometry_geojson: str
    score: float
    rank: int
    built_area: Optional[float] = None
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    street_view_url: str
    geoportail_url: str
    google_maps_url: str


class ComboMatchSchema(BaseModel):
    type: Literal["combo"] = "combo"
    parts: list[ComboPartSchema]
    total_contenance: int
    centroid_lat: float
    centroid_lon: float
    combined_geojson: str
    score: float
    rank: int
    compactness: float
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    nb_parcelles: int
    label: str
    nom_commune: str
    code_insee: str
    geoportail_url: str
    google_maps_url: str


class DPEPositionMatchSchema(BaseModel):
    type: Literal["dpe_position"] = "dpe_position"
    address: str
    postcode: str
    city: str
    code_insee: str
    surface_habitable: float
    centroid_lat: float
    centroid_lon: float
    score: float
    rank: int
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    date: Optional[str] = None
    geoportail_url: str
    google_maps_url: str


SearchResult = Annotated[
    Union[ParcelMatchSchema, ComboMatchSchema, DPEPositionMatchSchema],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Schémas de requête
# ---------------------------------------------------------------------------

class SearchParcelRequest(BaseModel):
    commune: str
    surface_m2: float
    living_surface: Optional[float] = None
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    postal_code: Optional[str] = None
    tolerance_pct: float = 5.0
    neighbor_mode: str = "none"


class SearchDPERequest(BaseModel):
    commune: str
    living_surface: float
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    dpe_date: Optional[str] = None
    conso_ep: Optional[float] = None
    ges_ep: Optional[float] = None
    postal_code: Optional[str] = None
    tolerance_pct: float = 10.0
    neighbor_mode: str = "none"


class ParseAdRequest(BaseModel):
    text: str


class ParseAdResponse(BaseModel):
    terrain_surface: Optional[float] = None
    living_surface: Optional[float] = None
    dpe_label: Optional[str] = None
    ges_label: Optional[str] = None
    dpe_date: Optional[str] = None
    commune: Optional[str] = None


class CommuneItem(BaseModel):
    label: str
    nom: str
    code_dept: str


# ---------------------------------------------------------------------------
# Fonctions de conversion dataclass → schéma Pydantic
# Les @property ne sont pas dans __dataclass_fields__ : conversion explicite.
# ---------------------------------------------------------------------------

def parcel_to_schema(m: ParcelMatch) -> ParcelMatchSchema:
    return ParcelMatchSchema(
        id_parcelle=m.id_parcelle,
        code_insee=m.code_insee,
        nom_commune=m.nom_commune,
        contenance=m.contenance,
        centroid_lat=m.centroid_lat,
        centroid_lon=m.centroid_lon,
        geometry_geojson=m.geometry_geojson,
        score=m.score,
        rank=m.rank,
        built_area=m.built_area,
        dpe_label=m.dpe_label,
        ges_label=m.ges_label,
        street_view_url=m.street_view_url,
        geoportail_url=m.geoportail_url,
        google_maps_url=m.google_maps_url,
    )


def combo_to_schema(m: ComboMatch) -> ComboMatchSchema:
    return ComboMatchSchema(
        parts=[ComboPartSchema(id_parcelle=p.id_parcelle, contenance=p.contenance, built_area=p.built_area) for p in m.parts],
        total_contenance=m.total_contenance,
        centroid_lat=m.centroid_lat,
        centroid_lon=m.centroid_lon,
        combined_geojson=m.combined_geojson,
        score=m.score,
        rank=m.rank,
        compactness=m.compactness,
        dpe_label=m.dpe_label,
        ges_label=m.ges_label,
        nb_parcelles=m.nb_parcelles,
        label=m.label,
        nom_commune=m.nom_commune,
        code_insee=m.code_insee,
        geoportail_url=m.geoportail_url,
        google_maps_url=m.google_maps_url,
    )


def dpe_position_to_schema(m: DPEPositionMatch) -> DPEPositionMatchSchema:
    return DPEPositionMatchSchema(
        address=m.address,
        postcode=str(m.postcode) if m.postcode is not None else "",
        city=m.city,
        code_insee=m.code_insee,
        surface_habitable=m.surface_habitable,
        centroid_lat=m.centroid_lat,
        centroid_lon=m.centroid_lon,
        score=m.score,
        rank=m.rank,
        dpe_label=m.dpe_label,
        ges_label=m.ges_label,
        date=m.date,
        geoportail_url=m.geoportail_url,
        google_maps_url=m.google_maps_url,
    )


def result_to_schema(m) -> SearchResult:
    if isinstance(m, ParcelMatch):
        return parcel_to_schema(m)
    if isinstance(m, ComboMatch):
        return combo_to_schema(m)
    return dpe_position_to_schema(m)
