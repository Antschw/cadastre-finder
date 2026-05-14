import {
  Component,
  EventEmitter,
  OnInit,
  Output,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import {
  FormBuilder,
  FormGroup,
  ReactiveFormsModule,
  Validators,
} from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { MatSliderModule } from '@angular/material/slider';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatTabsModule } from '@angular/material/tabs';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatExpansionModule } from '@angular/material/expansion';
import {
  debounceTime,
  distinctUntilChanged,
  filter,
  map,
  Observable,
  startWith,
  switchMap,
} from 'rxjs';
import { CommuneItem, DpeLabel, NeighborMode, SearchDPERequest, SearchParcelRequest } from '../../core/models/search.models';
import { CommunesService } from '../../core/services/communes.service';
import { ParseAdService } from '../../core/services/parse-ad.service';
import { SearchService } from '../../core/services/search.service';

export type SearchMode = 'parcelles' | 'dpe';

@Component({
  selector: 'app-search-form',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatIconModule,
    MatSelectModule,
    MatSliderModule,
    MatButtonToggleModule,
    MatAutocompleteModule,
    MatTabsModule,
    MatProgressSpinnerModule,
    MatExpansionModule,
  ],
  template: `
    <div class="sidebar-header">
      <mat-icon class="logo-icon">map</mat-icon>
      <span class="app-title">Cadastre Finder</span>
    </div>

    <!-- Mode tabs -->
    <mat-tab-group (selectedTabChange)="onModeChange($event.index)" animationDuration="200ms">
      <mat-tab label="Positions DPE"></mat-tab>
      <mat-tab label="Parcelles"></mat-tab>
    </mat-tab-group>

    <div class="form-scroll">
      <form [formGroup]="form">

        <!-- Annonce brute -->
        <mat-expansion-panel class="ad-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>Analyser une annonce</mat-panel-title>
          </mat-expansion-panel-header>
          <mat-form-field appearance="outline" class="full-width">
            <mat-label>Texte de l'annonce</mat-label>
            <textarea matInput formControlName="adText" rows="4"
              placeholder="Collez l'annonce ici…"></textarea>
          </mat-form-field>
          <button mat-stroked-button type="button" (click)="parseAd()" [disabled]="!form.get('adText')?.value">
            <mat-icon>auto_awesome</mat-icon> Extraire les critères
          </button>
        </mat-expansion-panel>

        <!-- Commune -->
        <mat-form-field appearance="outline" class="full-width form-field-gap">
          <mat-label>Commune</mat-label>
          <input matInput formControlName="commune"
            [matAutocomplete]="communeAuto"
            placeholder="ex : Neuvy-le-Roi" />
          <mat-autocomplete #communeAuto [displayWith]="communeDisplay">
            @for (c of filteredCommunes$ | async; track c.nom) {
              <mat-option [value]="c">{{ c.label }}</mat-option>
            }
          </mat-autocomplete>
        </mat-form-field>

        <mat-form-field appearance="outline" class="full-width">
          <mat-label>Code postal (optionnel)</mat-label>
          <input matInput formControlName="postalCode" placeholder="ex : 37370" maxlength="5" />
        </mat-form-field>

        <!-- Surfaces selon mode -->
        @if (mode === 'parcelles') {
          <mat-form-field appearance="outline" class="full-width form-field-gap">
            <mat-label>Surface Terrain (m²)</mat-label>
            <input matInput type="number" formControlName="surfaceTerrain" min="100" max="100000" />
            <mat-hint>Min 100 m²</mat-hint>
          </mat-form-field>
          <mat-form-field appearance="outline" class="full-width">
            <mat-label>Surface Habitable (m²)</mat-label>
            <input matInput type="number" formControlName="surfaceHabitable" min="10" max="2000" />
          </mat-form-field>
        } @else {
          <mat-form-field appearance="outline" class="full-width form-field-gap">
            <mat-label>Surface Habitable (m²)</mat-label>
            <input matInput type="number" formControlName="surfaceHabitable" min="10" max="2000" />
          </mat-form-field>
        }

        <!-- Filtres DPE / GES -->
        <div class="two-cols form-field-gap">
          <mat-form-field appearance="outline">
            <mat-label>DPE</mat-label>
            <mat-select formControlName="dpeLabel">
              <mat-option [value]="null">—</mat-option>
              @for (l of dpeLabels; track l) {
                <mat-option [value]="l">{{ l }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
          <mat-form-field appearance="outline">
            <mat-label>GES</mat-label>
            <mat-select formControlName="gesLabel">
              <mat-option [value]="null">—</mat-option>
              @for (l of dpeLabels; track l) {
                <mat-option [value]="l">{{ l }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
        </div>

        <!-- Champs spécifiques mode DPE -->
        @if (mode === 'dpe') {
          <mat-form-field appearance="outline" class="full-width">
            <mat-label>Date DPE (AAAA-MM-JJ)</mat-label>
            <input matInput formControlName="dpeDate" placeholder="2023-06-15" />
          </mat-form-field>
          <div class="two-cols form-field-gap">
            <mat-form-field appearance="outline">
              <mat-label>Conso (kWh/m²/an)</mat-label>
              <input matInput type="number" formControlName="consoEp" min="0" max="600" step="5" />
            </mat-form-field>
            <mat-form-field appearance="outline">
              <mat-label>GES émis (kg CO₂)</mat-label>
              <input matInput type="number" formControlName="gesEp" min="0" max="200" step="1" />
            </mat-form-field>
          </div>
        }

        <!-- Tolérance -->
        <div class="form-field-gap">
          <label class="slider-label">
            Tolérance surface : <strong>{{ toleranceDisplay }}</strong>
          </label>
          <mat-slider [min]="toleranceMin" [max]="toleranceMax" [step]="toleranceStep" class="full-slider">
            <input matSliderThumb formControlName="tolerance" />
          </mat-slider>
        </div>

        <!-- Voisinage -->
        <div class="form-field-gap">
          <label class="slider-label">Extension géographique</label>
          <mat-button-toggle-group formControlName="neighborMode" class="neighbor-toggle">
            <mat-button-toggle value="none">Aucun</mat-button-toggle>
            <mat-button-toggle value="rank1">Rang 1</mat-button-toggle>
            <mat-button-toggle value="rank2">Rang 2</mat-button-toggle>
            <mat-button-toggle value="rank3">Rang 3</mat-button-toggle>
          </mat-button-toggle-group>
        </div>

        <!-- Bouton recherche -->
        <button
          mat-flat-button
          color="primary"
          class="search-btn"
          type="button"
          (click)="onSearch()"
          [disabled]="loading || !canSearch"
        >
          <span class="btn-inner">
            @if (loading) {
              <mat-spinner diameter="18" class="btn-spinner"></mat-spinner>
            } @else {
              <mat-icon>search</mat-icon>
            }
            <span>Lancer la recherche</span>
          </span>
        </button>

      </form>
    </div>
  `,
  styles: [`
    :host { display: flex; flex-direction: column; height: 100%; }
    .sidebar-header {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 16px 16px 8px;
      border-bottom: 1px solid #e0e0e0;
    }
    .logo-icon { color: #1a73e8; font-size: 28px; width: 28px; height: 28px; }
    .app-title { font-size: 1.1rem; font-weight: 600; color: #202124; }

    mat-tab-group { padding: 0 8px; }

    .form-scroll {
      flex: 1;
      overflow-y: auto;
      overflow-x: hidden;
      padding: 16px;
    }
    .full-width { width: 100%; }
    .form-field-gap { margin-top: 12px; }
    .two-cols {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .ad-panel { margin-bottom: 12px; box-shadow: none !important; border: 1px solid #e0e0e0; }
    .slider-label { font-size: 0.82rem; color: #555; display: block; margin-bottom: 4px; }
    .full-slider { width: 100%; }
    .neighbor-toggle { width: 100%; }
    .neighbor-toggle mat-button-toggle { flex: 1; font-size: 0.78rem; }
    .search-btn {
      width: 100%;
      margin-top: 20px;
      height: 44px;
      font-size: 0.95rem;
    }
    .btn-inner {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      width: 100%;
    }
    .btn-spinner { display: flex; }
  `],
})
export class SearchFormComponent implements OnInit {
  @Output() searchResults = new EventEmitter<any[]>();

  mode: SearchMode = 'dpe';
  loading = false;
  dpeLabels: DpeLabel[] = ['A', 'B', 'C', 'D', 'E', 'F', 'G'];

  form!: FormGroup;
  filteredCommunes$!: Observable<CommuneItem[]>;
  private allCommunes: CommuneItem[] = [];

  constructor(
    private fb: FormBuilder,
    private communesService: CommunesService,
    private parseAdService: ParseAdService,
    private searchService: SearchService,
  ) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      adText: [''],
      commune: [null, Validators.required],
      postalCode: [''],
      surfaceTerrain: [5000, [Validators.min(100), Validators.max(100000)]],
      surfaceHabitable: [100, [Validators.min(10), Validators.max(2000)]],
      dpeLabel: [null],
      gesLabel: [null],
      dpeDate: [''],
      consoEp: [0],
      gesEp: [0],
      tolerance: [10],
      neighborMode: ['none'],
    });

    // Charger les communes
    this.communesService.getCommunes().subscribe(list => {
      this.allCommunes = list;
    });

    // Autocomplétion
    this.filteredCommunes$ = this.form.get('commune')!.valueChanges.pipe(
      startWith(''),
      debounceTime(150),
      map(value => {
        const query = typeof value === 'string' ? value : (value as CommuneItem)?.nom ?? '';
        if (query.length < 2) return [];
        const normalized = query.toLowerCase();
        return this.allCommunes
          .filter(c => c.nom.toLowerCase().includes(normalized))
          .slice(0, 30);
      }),
    );
  }

  communeDisplay(c: CommuneItem | string | null): string {
    if (!c) return '';
    return typeof c === 'string' ? c : c.nom;
  }

  onModeChange(index: number): void {
    this.mode = index === 0 ? 'dpe' : 'parcelles';
    this.form.patchValue({ tolerance: this.mode === 'dpe' ? 10 : 100 });
  }

  get toleranceMin(): number { return this.mode === 'dpe' ? 1 : 0; }
  get toleranceMax(): number { return this.mode === 'dpe' ? 30 : 5000; }
  get toleranceStep(): number { return this.mode === 'dpe' ? 1 : 50; }
  get toleranceDisplay(): string {
    const v = this.form.get('tolerance')?.value ?? 0;
    return this.mode === 'dpe' ? `±${v} %` : `±${v} m²`;
  }

  get canSearch(): boolean {
    const c = this.form.get('commune')?.value;
    const hasCommune = c && (typeof c === 'object' || (typeof c === 'string' && c.trim().length > 0));
    if (this.mode === 'parcelles') {
      return hasCommune && (this.form.get('surfaceTerrain')?.value > 0);
    }
    return hasCommune && (this.form.get('surfaceHabitable')?.value > 0);
  }

  parseAd(): void {
    const text = this.form.get('adText')?.value;
    if (!text) return;
    this.parseAdService.parseAd(text).subscribe(criteria => {
      const patch: Record<string, unknown> = {};
      if (criteria.terrain_surface) patch['surfaceTerrain'] = criteria.terrain_surface;
      if (criteria.living_surface) patch['surfaceHabitable'] = criteria.living_surface;
      if (criteria.dpe_label) patch['dpeLabel'] = criteria.dpe_label;
      if (criteria.ges_label) patch['gesLabel'] = criteria.ges_label;
      if (criteria.dpe_date) patch['dpeDate'] = criteria.dpe_date;
      if (criteria.commune) patch['commune'] = criteria.commune;
      this.form.patchValue(patch);
    });
  }

  onSearch(): void {
    if (!this.canSearch || this.loading) return;
    this.loading = true;

    const v = this.form.value;
    const communeVal = v.commune;
    const communeName = typeof communeVal === 'object' && communeVal?.nom
      ? communeVal.nom
      : String(communeVal ?? '');
    const postal = v.postalCode || undefined;
    const neighborMode: NeighborMode = v.neighborMode;
    const tolerance = v.tolerance;

    if (this.mode === 'dpe') {
      const req: SearchDPERequest = {
        commune: communeName,
        living_surface: v.surfaceHabitable,
        dpe_label: v.dpeLabel || undefined,
        ges_label: v.gesLabel || undefined,
        dpe_date: v.dpeDate || undefined,
        conso_ep: v.consoEp || undefined,
        ges_ep: v.gesEp || undefined,
        postal_code: postal,
        tolerance_pct: tolerance,
        neighbor_mode: neighborMode,
      };
      this.searchService.searchDPEPositions(req).subscribe({
        next: results => { this.loading = false; this.searchResults.emit(results); },
        error: () => { this.loading = false; },
      });
    } else {
      const surf = v.surfaceTerrain;
      const tolPct = surf > 0 ? (tolerance / surf) * 100 : 5;
      const req: SearchParcelRequest = {
        commune: communeName,
        surface_m2: surf,
        living_surface: v.surfaceHabitable || undefined,
        dpe_label: v.dpeLabel || undefined,
        ges_label: v.gesLabel || undefined,
        postal_code: postal,
        tolerance_pct: tolPct,
        neighbor_mode: neighborMode,
      };
      this.searchService.searchParcelles(req).subscribe({
        next: results => { this.loading = false; this.searchResults.emit(results); },
        error: () => { this.loading = false; },
      });
    }
  }
}
