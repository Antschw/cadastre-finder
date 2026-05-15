import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import { ScoreBadgeComponent } from '../../../shared/score-badge/score-badge.component';
import { ParcelMapComponent } from '../../parcel-map/parcel-map.component';
import { ComboMatchDto, DPEPositionMatchDto, ParcelMatchDto, SearchResultDto } from '../../../core/models/search.models';

@Component({
  selector: 'app-result-detail',
  standalone: true,
  imports: [
    CommonModule,
    MatButtonModule,
    MatIconModule,
    MatProgressBarModule,
    MatCardModule,
    MatChipsModule,
    ScoreBadgeComponent,
    ParcelMapComponent,
  ],
  template: `
    <!-- Navigation -->
    <div class="nav-bar">
      <button mat-icon-button (click)="prevResult.emit()" [disabled]="currentIndex === 0">
        <mat-icon>chevron_left</mat-icon>
      </button>
      <span class="nav-counter">Résultat <strong>{{ currentIndex + 1 }}</strong> / {{ total }}</span>
      <button mat-icon-button (click)="nextResult.emit()" [disabled]="currentIndex === total - 1">
        <mat-icon>chevron_right</mat-icon>
      </button>
    </div>

    <!-- Contenu en 2 colonnes -->
    <div class="detail-layout">
      <!-- Colonne info -->
      <div class="info-column">
        <app-score-badge [score]="result.score" [rank]="result.rank" />

        <mat-progress-bar
          mode="determinate"
          [value]="progressValue"
          class="score-bar"
        />

        <!-- ParcelMatch -->
        @if (isParcel(result)) {
          <div class="metrics-grid">
            <div class="metric"><span class="m-label">Surface</span><span class="m-value">{{ asParcel(result).contenance | number }} m²</span></div>
            @if (asParcel(result).built_area) {
              <div class="metric"><span class="m-label">Bâti</span><span class="m-value">{{ asParcel(result).built_area | number:'1.0-0' }} m²</span></div>
            }
            <div class="metric"><span class="m-label">Commune</span><span class="m-value">{{ asParcel(result).nom_commune }}</span></div>
            <div class="metric full-width"><span class="m-label">Identifiant</span><span class="m-value mono">{{ asParcel(result).id_parcelle }}</span></div>
          </div>
          <ng-container *ngTemplateOutlet="dpeBadges; context: { dpe: asParcel(result).dpe_label, ges: asParcel(result).ges_label }" />
          <div class="link-row">
            <a mat-stroked-button [href]="asParcel(result).geoportail_url" target="_blank">Géoportail</a>
            <a mat-stroked-button [href]="asParcel(result).google_maps_url" target="_blank">Google Maps</a>
            <a mat-stroked-button [href]="asParcel(result).street_view_url" target="_blank">Street View</a>
          </div>
        }

        <!-- ComboMatch -->
        @if (isCombo(result)) {
          <div class="metrics-grid">
            <div class="metric"><span class="m-label">Surface totale</span><span class="m-value">{{ asCombo(result).total_contenance | number }} m²</span></div>
            <div class="metric"><span class="m-label">Parcelles</span><span class="m-value">{{ asCombo(result).nb_parcelles }}</span></div>
            <div class="metric"><span class="m-label">Commune</span><span class="m-value">{{ asCombo(result).nom_commune }}</span></div>
            <div class="metric">
              <span class="m-label">Compacité</span>
              <span class="m-value" [style.color]="compacityColor(asCombo(result).compactness)">
                {{ asCombo(result).compactness | number:'1.2-2' }}
              </span>
            </div>
          </div>
          <ng-container *ngTemplateOutlet="dpeBadges; context: { dpe: asCombo(result).dpe_label, ges: asCombo(result).ges_label }" />
          <div class="combo-ids">
            @for (p of asCombo(result).parts; track p.id_parcelle) {
              <span class="id-tag">{{ p.id_parcelle }} ({{ p.contenance | number }} m²)</span>
            }
          </div>
          <div class="link-row">
            <a mat-stroked-button [href]="asCombo(result).geoportail_url" target="_blank">Géoportail</a>
            <a mat-stroked-button [href]="asCombo(result).google_maps_url" target="_blank">Google Maps</a>
          </div>
        }

        <!-- DPEPositionMatch -->
        @if (isDpe(result)) {
          <div class="metrics-grid">
            <div class="metric full-width"><span class="m-label">Adresse</span><span class="m-value">{{ asDpe(result).address }}</span></div>
            <div class="metric full-width"><span class="m-label">Commune</span><span class="m-value">{{ asDpe(result).city }} ({{ asDpe(result).postcode }})</span></div>
            <div class="metric full-width"><span class="m-label">Surface hab.</span><span class="m-value">{{ asDpe(result).surface_habitable | number:'1.0-0' }} m²</span></div>
            @if (asDpe(result).date) {
              <div class="metric full-width"><span class="m-label">Date DPE</span><span class="m-value">{{ asDpe(result).date }}</span></div>
            }
          </div>
          <ng-container *ngTemplateOutlet="dpeBadges; context: { dpe: asDpe(result).dpe_label, ges: asDpe(result).ges_label }" />
          <div class="link-row">
            <a mat-stroked-button [href]="asDpe(result).geoportail_url" target="_blank" class="logo-btn" aria-label="Géoportail">
              <span class="btn-content"><img src="logos/geoportail.svg" class="logo-geoportail" alt="Géoportail"></span>
            </a>
            <a mat-stroked-button [href]="asDpe(result).google_maps_url" target="_blank" class="logo-btn">
              <span class="btn-content"><img src="logos/google-maps.svg" class="icon-gmap" alt="">Google Maps</span>
            </a>
          </div>
        }
      </div>

      <!-- Colonne carte -->
      <div class="map-column">
        <app-parcel-map [result]="result" />
      </div>
    </div>

    <!-- Template badges DPE/GES partagé -->
    <ng-template #dpeBadges let-dpe="dpe" let-ges="ges">
      @if (dpe || ges) {
        <div class="dpe-row">
          @if (dpe) {
            <div class="dpe-badge-group">
              <span class="dpe-row-label">DPE</span>
              <span class="dpe-badge"
                [style.background]="dpeColor(dpe)"
                [style.color]="dpeLabelColor(dpe)">{{ dpe }}</span>
            </div>
          }
          @if (ges) {
            <div class="dpe-badge-group">
              <span class="dpe-row-label">GES</span>
              <span class="dpe-badge"
                [style.background]="gesColor(ges)"
                [style.color]="gesLabelColor(ges)">{{ ges }}</span>
            </div>
          }
        </div>
      }
    </ng-template>
  `,
  styles: [`
    .nav-bar {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 16px;
    }
    .nav-counter { font-size: 0.9rem; color: #555; flex: 1; text-align: center; }
    .detail-layout {
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 28px;
      align-items: start;
    }
    .info-column { min-width: 0; max-width: 340px; }
    .map-column { min-height: 640px; }
    .score-bar { margin-bottom: 20px; height: 8px; border-radius: 4px; }
    .metrics-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 12px;
    }
    .metric { display: flex; flex-direction: column; padding: 6px 0; border-bottom: 1px solid #f0f0f0; }
    .metric.full-width { grid-column: 1 / -1; }
    .m-label { font-size: 0.72rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }
    .m-value { font-size: 1rem; font-weight: 500; color: #202124; line-height: 1.4; }
    .m-value.mono { font-family: monospace; font-size: 0.85rem; word-break: break-all; }
    .link-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 24px; align-items: center; }
    .link-row a { font-size: 0.82rem; }
    .logo-btn { min-width: 140px; }
    .btn-content { display: inline-flex; align-items: center; justify-content: center; gap: 6px; white-space: nowrap; width: 100%; }
    .logo-geoportail { height: 20px; width: auto; }
    .icon-gmap { height: 18px; width: auto; }
    .combo-ids { display: flex; flex-direction: column; gap: 5px; margin: 10px 0; }
    .id-tag { font-family: monospace; font-size: 0.78rem; color: #555; }

    /* DPE/GES row */
    .dpe-row {
      display: flex;
      gap: 20px;
      align-items: center;
      margin: 12px 0 16px;
      padding: 10px 14px;
      background: #f8f9fa;
      border-radius: 6px;
      border: 1px solid #e8eaed;
    }
    .dpe-badge-group { display: flex; align-items: center; gap: 8px; }
    .dpe-row-label {
      font-size: 0.7rem;
      color: #888;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 500;
    }
    .dpe-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 32px;
      height: 32px;
      border-radius: 4px;
      font-weight: 700;
      font-size: 1.05rem;
      line-height: 1;
    }

    /* En dessous de 60% d'un écran QHD : carte en premier, info dessous */
    @media (max-width: 1536px) {
      .detail-layout { grid-template-columns: 1fr; }
      .map-column { order: -1; min-height: 500px; }
      .info-column { max-width: 100%; }
    }
  `],
})
export class ResultDetailComponent {
  @Input() result!: SearchResultDto;
  @Input() currentIndex = 0;
  @Input() total = 0;
  @Output() prevResult = new EventEmitter<void>();
  @Output() nextResult = new EventEmitter<void>();

  private readonly DPE_COLORS: Record<string, [string, string]> = {
    A: ['#009a44', '#fff'],
    B: ['#55a820', '#fff'],
    C: ['#cacc03', '#000'],
    D: ['#f1b516', '#000'],
    E: ['#e57416', '#fff'],
    F: ['#d7421c', '#fff'],
    G: ['#ce1516', '#fff'],
  };

  private readonly GES_COLORS: Record<string, [string, string]> = {
    A: ['#e8d0e8', '#000'],
    B: ['#c9a0c9', '#000'],
    C: ['#a870a8', '#fff'],
    D: ['#884088', '#fff'],
    E: ['#682068', '#fff'],
    F: ['#501050', '#fff'],
    G: ['#380038', '#fff'],
  };

  get progressValue(): number {
    return Math.min(Math.max((this.result.score / 118) * 100, 0), 100);
  }

  isParcel(r: SearchResultDto): r is ParcelMatchDto { return r.type === 'parcel'; }
  isCombo(r: SearchResultDto): r is ComboMatchDto { return r.type === 'combo'; }
  isDpe(r: SearchResultDto): r is DPEPositionMatchDto { return r.type === 'dpe_position'; }
  asParcel(r: SearchResultDto): ParcelMatchDto { return r as ParcelMatchDto; }
  asCombo(r: SearchResultDto): ComboMatchDto { return r as ComboMatchDto; }
  asDpe(r: SearchResultDto): DPEPositionMatchDto { return r as DPEPositionMatchDto; }

  compacityColor(c: number): string {
    if (c >= 0.5) return '#2e7d32';
    if (c >= 0.2) return '#e65100';
    return '#c62828';
  }

  dpeColor(l: string): string { return this.DPE_COLORS[l]?.[0] ?? '#888'; }
  dpeLabelColor(l: string): string { return this.DPE_COLORS[l]?.[1] ?? '#fff'; }
  gesColor(l: string): string { return this.GES_COLORS[l]?.[0] ?? '#888'; }
  gesLabelColor(l: string): string { return this.GES_COLORS[l]?.[1] ?? '#fff'; }
}
