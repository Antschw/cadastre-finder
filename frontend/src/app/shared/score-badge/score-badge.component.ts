import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-score-badge',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="score-badge-wrapper">
      <span class="score-pill" [style.background-color]="color">
        Score {{ score | number: '1.1-1' }}
      </span>
      <span class="rank-label">{{ rankLabel }}</span>
    </div>
  `,
  styles: [`
    .score-badge-wrapper {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 12px;
    }
    .score-pill {
      display: inline-block;
      padding: 4px 14px;
      border-radius: 20px;
      font-size: 1rem;
      font-weight: 600;
      color: #fff;
      letter-spacing: 0.02em;
    }
    .rank-label {
      color: #666;
      font-size: 0.85rem;
    }
  `],
})
export class ScoreBadgeComponent {
  @Input() score = 0;
  @Input() rank = 0;

  get color(): string {
    if (this.score >= 100) return '#2e7d32';
    if (this.score >= 80) return '#558b2f';
    if (this.score >= 60) return '#f9a825';
    if (this.score >= 40) return '#e65100';
    return '#c62828';
  }

  get rankLabel(): string {
    const labels: Record<number, string> = {
      0: 'Commune annoncée',
      1: 'Voisine rang 1',
      2: 'Voisine rang 2',
      3: 'Voisine rang 3',
    };
    return labels[this.rank] ?? `Rang ${this.rank}`;
  }
}
