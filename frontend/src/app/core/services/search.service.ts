import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import {
  DPEPositionMatchDto,
  SearchDPERequest,
  SearchParcelRequest,
  SearchResultDto,
} from '../models/search.models';

@Injectable({ providedIn: 'root' })
export class SearchService {
  constructor(private http: HttpClient) {}

  searchParcelles(req: SearchParcelRequest): Observable<SearchResultDto[]> {
    return this.http.post<SearchResultDto[]>('/api/search/parcelles', req);
  }

  searchDPEPositions(req: SearchDPERequest): Observable<DPEPositionMatchDto[]> {
    return this.http.post<DPEPositionMatchDto[]>('/api/search/dpe-positions', req);
  }
}
