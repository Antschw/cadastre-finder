import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SearchResultDto } from '../../../core/models/search.models';

@Component({
  selector: 'app-results-overview',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="overview-grid">
      @for (r of results.slice(0, 12); track $index) {
        <div
          class="overview-cell"
          [style.background-color]="scoreColor(r.score)"
          [class.selected]="$index === currentIndex"
          (click)="selectResult.emit($index)"
          [title]="'Score ' + r.score.toFixed(1)"
        >
          <span class="type-label">{{ typeLabel(r) }}</span>
          <strong>{{ r.score | number: '1.0-0' }}</strong>
        </div>
      }
    </div>
    @if (results.length > 12) {
      <p class="overflow-caption">… et {{ results.length - 12 }} autres résultats</p>
    }
  `,
  styles: [`
    .overview-grid {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 8px;
    }
    .overview-cell {
      width: 64px;
      height: 64px;
      border-radius: 6px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      color: #fff;
      font-size: 0.88rem;
      cursor: pointer;
      border: 2px solid transparent;
      transition: transform 0.1s, border-color 0.15s;
    }
    .overview-cell:hover { transform: scale(1.06); }
    .overview-cell.selected { border-color: #1a73e8; }
    .type-label { font-size: 0.72rem; opacity: 0.85; }
    .overflow-caption { font-size: 0.8rem; color: #888; margin: 4px 0 0; }
  `],
})
export class ResultsOverviewComponent {
  @Input() results: SearchResultDto[] = [];
  @Input() currentIndex = 0;
  @Output() selectResult = new EventEmitter<number>();

  scoreColor(score: number): string {
    if (score >= 100) return '#2e7d32';
    if (score >= 80) return '#558b2f';
    if (score >= 60) return '#f9a825';
    if (score >= 40) return '#e65100';
    return '#c62828';
  }

  typeLabel(r: SearchResultDto): string {
    if (r.type === 'combo') return 'C';
    if (r.type === 'dpe_position') return 'D';
    return 'P';
  }
}
