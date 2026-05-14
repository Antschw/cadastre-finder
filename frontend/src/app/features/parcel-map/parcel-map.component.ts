import {
  AfterViewInit,
  Component,
  Input,
  NgZone,
  OnChanges,
  OnDestroy,
  SimpleChanges,
} from '@angular/core';
import * as L from 'leaflet';
import { SearchResultDto } from '../../core/models/search.models';

const IGN_WMTS_URL =
  'https://data.geopf.fr/wmts?' +
  'SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0' +
  '&LAYER=ORTHOIMAGERY.ORTHOPHOTOS' +
  '&STYLE=normal&TILEMATRIXSET=PM' +
  '&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}' +
  '&FORMAT=image/jpeg';

@Component({
  selector: 'app-parcel-map',
  standalone: true,
  template: `<div [id]="mapId" class="map-container"></div>`,
  styles: [`
    .map-container { height: 900px; width: 100%; border-radius: 8px; overflow: hidden; }
    @media (max-width: 1536px) { .map-container { height: 500px; } }
  `],
})
export class ParcelMapComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() result!: SearchResultDto;

  readonly mapId = `map-${Math.random().toString(36).slice(2)}`;
  private map?: L.Map;
  private dataLayer?: L.Layer;

  constructor(private ngZone: NgZone) {}

  ngAfterViewInit(): void {
    this.ngZone.runOutsideAngular(() => {
      this.map = L.map(this.mapId).setView(
        [this.result.centroid_lat, this.result.centroid_lon],
        17,
      );
      L.tileLayer(IGN_WMTS_URL, {
        attribution: '© IGN Géoplateforme',
        maxZoom: 19,
      }).addTo(this.map);
      L.tileLayer(
        'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        { attribution: '© OpenStreetMap', maxZoom: 19, opacity: 0 }
      );
      this.renderResult();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['result'] && this.map) {
      this.ngZone.runOutsideAngular(() => this.renderResult());
    }
  }

  private renderResult(): void {
    if (!this.map) return;
    if (this.dataLayer) {
      this.map.removeLayer(this.dataLayer);
      this.dataLayer = undefined;
    }

    const r = this.result;
    this.map.setView([r.centroid_lat, r.centroid_lon], 17);

    if (r.type === 'dpe_position') {
      this.dataLayer = L.marker([r.centroid_lat, r.centroid_lon], {
        icon: L.icon({
          iconUrl: 'https://cdn.jsdelivr.net/gh/pointhi/leaflet-color-markers@master/img/marker-icon-2x-blue.png',
          shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
          iconSize: [25, 41],
          iconAnchor: [12, 41],
          shadowSize: [41, 41],
        }),
      }).addTo(this.map);
    } else if (r.type === 'combo') {
      try {
        const geojson = JSON.parse(r.combined_geojson);
        this.dataLayer = L.geoJSON(geojson, {
          style: {
            fillColor: '#7b1fa2',
            color: '#4a148c',
            weight: 2.5,
            fillOpacity: 0.45,
            dashArray: '6 3',
          },
        }).addTo(this.map);
      } catch {
        this.dataLayer = L.marker([r.centroid_lat, r.centroid_lon]).addTo(this.map);
      }
    } else {
      const color = this.scoreColor(r.score);
      try {
        const geojson = JSON.parse(r.geometry_geojson);
        this.dataLayer = L.geoJSON(geojson, {
          style: {
            fillColor: color,
            color: '#333',
            weight: 2,
            fillOpacity: 0.5,
          },
        }).addTo(this.map);
      } catch {
        this.dataLayer = L.marker([r.centroid_lat, r.centroid_lon]).addTo(this.map);
      }
    }
  }

  private scoreColor(score: number): string {
    if (score >= 100) return '#2e7d32';
    if (score >= 80) return '#558b2f';
    if (score >= 60) return '#f9a825';
    if (score >= 40) return '#e65100';
    return '#c62828';
  }

  ngOnDestroy(): void {
    this.ngZone.runOutsideAngular(() => this.map?.remove());
  }
}
