import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { SearchFormComponent } from './features/search-form/search-form.component';
import { ResultDetailComponent } from './features/results/result-detail/result-detail.component';
import { SearchResultDto } from './core/models/search.models';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    MatSidenavModule,
    MatIconModule,
    MatSnackBarModule,
    SearchFormComponent,
    ResultDetailComponent,
  ],
  template: `
    <mat-sidenav-container class="sidenav-container">
      <!-- Sidebar formulaire -->
      <mat-sidenav mode="side" opened class="sidenav">
        <app-search-form (searchResults)="onResults($event)" />
      </mat-sidenav>

      <!-- Zone principale résultats -->
      <mat-sidenav-content class="main-content">
        @if (results().length === 0 && !hasSearched()) {
          <div class="empty-state">
            <mat-icon class="welcome-icon">travel_explore</mat-icon>
            <p class="empty-title">Cadastre Finder</p>
            <p class="empty-hint">Renseignez vos critères dans le formulaire pour identifier des parcelles cadastrales.</p>
          </div>
        } @else if (results().length === 0 && hasSearched()) {
          <div class="empty-state">
            <mat-icon class="no-result-icon">search_off</mat-icon>
            <p class="empty-title">Aucun résultat trouvé</p>
            <p class="empty-hint">Essayez d'élargir la tolérance, de choisir un voisinage de rang supérieur, ou de relâcher les filtres DPE/GES.</p>
          </div>
        } @else {
          <div class="results-wrapper">
            <app-result-detail
              [result]="results()[currentIndex()]"
              [currentIndex]="currentIndex()"
              [total]="results().length"
              (prevResult)="currentIndex.update(i => i - 1)"
              (nextResult)="currentIndex.update(i => i + 1)"
            />
          </div>
        }
      </mat-sidenav-content>
    </mat-sidenav-container>
  `,
  styles: [`
    .sidenav-container { height: 100vh; }
    .sidenav {
      width: 360px;
      background: #fff;
      border-right: 1px solid #e0e0e0;
      display: flex;
      flex-direction: column;
    }
    .main-content {
      background: #f8f9fa;
      padding: 16px 20px;
      overflow-y: auto;
      height: 100%;
    }
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 70%;
      gap: 10px;
    }
    .welcome-icon { font-size: 72px; width: 72px; height: 72px; color: #dadce0; }
    .no-result-icon { font-size: 72px; width: 72px; height: 72px; color: #e8a87c; }
    .empty-title { font-size: 1.1rem; font-weight: 500; color: #5f6368; margin: 0; }
    .empty-hint { color: #80868b; font-size: 0.9rem; max-width: 320px; text-align: center; margin: 0; }
    .results-wrapper { width: 100%; }
  `],
})
export class App {
  results = signal<SearchResultDto[]>([]);
  currentIndex = signal(0);
  hasSearched = signal(false);

  onResults(data: SearchResultDto[]): void {
    this.results.set(data);
    this.currentIndex.set(0);
    this.hasSearched.set(true);
  }
}
