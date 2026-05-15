export type NeighborMode = 'none' | 'rank1' | 'rank2' | 'rank3' | 'dept';
export type DpeLabel = 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G';

export interface ComboPartDto {
  id_parcelle: string;
  contenance: number;
  built_area: number | null;
}

export interface ParcelMatchDto {
  type: 'parcel';
  id_parcelle: string;
  code_insee: string;
  nom_commune: string;
  contenance: number;
  centroid_lat: number;
  centroid_lon: number;
  geometry_geojson: string;
  score: number;
  rank: number;
  built_area: number | null;
  dpe_label: DpeLabel | null;
  ges_label: DpeLabel | null;
  street_view_url: string;
  geoportail_url: string;
  google_maps_url: string;
}

export interface ComboMatchDto {
  type: 'combo';
  parts: ComboPartDto[];
  total_contenance: number;
  centroid_lat: number;
  centroid_lon: number;
  combined_geojson: string;
  score: number;
  rank: number;
  compactness: number;
  dpe_label: DpeLabel | null;
  ges_label: DpeLabel | null;
  nb_parcelles: number;
  label: string;
  nom_commune: string;
  code_insee: string;
  geoportail_url: string;
  google_maps_url: string;
}

export interface DPEPositionMatchDto {
  type: 'dpe_position';
  address: string;
  postcode: string;
  city: string;
  code_insee: string;
  surface_habitable: number;
  centroid_lat: number;
  centroid_lon: number;
  score: number;
  rank: number;
  dpe_label: DpeLabel | null;
  ges_label: DpeLabel | null;
  date: string | null;
  geoportail_url: string;
  google_maps_url: string;
}

export type SearchResultDto = ParcelMatchDto | ComboMatchDto | DPEPositionMatchDto;

export interface SearchParcelRequest {
  commune: string;
  surface_m2: number;
  living_surface?: number;
  dpe_label?: DpeLabel;
  ges_label?: DpeLabel;
  postal_code?: string;
  tolerance_pct: number;
  neighbor_mode: NeighborMode;
}

export interface SearchDPERequest {
  commune: string;
  living_surface: number;
  dpe_label?: DpeLabel;
  ges_label?: DpeLabel;
  dpe_date?: string;
  conso_ep?: number;
  ges_ep?: number;
  postal_code?: string;
  tolerance_pct: number;
  neighbor_mode: NeighborMode;
}

export interface ParseAdResponse {
  terrain_surface: number | null;
  living_surface: number | null;
  dpe_label: DpeLabel | null;
  ges_label: DpeLabel | null;
  dpe_date: string | null;
  commune: string | null;
  postal_code: string | null;
}

export interface CommuneItem {
  label: string;
  nom: string;
  code_dept: string;
}
