import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, shareReplay } from 'rxjs';
import { CommuneItem } from '../models/search.models';

@Injectable({ providedIn: 'root' })
export class CommunesService {
  private communes$: Observable<CommuneItem[]>;

  constructor(private http: HttpClient) {
    this.communes$ = this.http
      .get<CommuneItem[]>('/api/communes')
      .pipe(shareReplay(1));
  }

  getCommunes(): Observable<CommuneItem[]> {
    return this.communes$;
  }
}
