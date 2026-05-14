import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { ParseAdResponse } from '../models/search.models';

@Injectable({ providedIn: 'root' })
export class ParseAdService {
  constructor(private http: HttpClient) {}

  parseAd(text: string): Observable<ParseAdResponse> {
    return this.http.post<ParseAdResponse>('/api/parse-ad', { text });
  }
}
