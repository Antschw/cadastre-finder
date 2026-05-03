# NEUVY

## Description

Maison à Neuvy-le-Roi composé de 6 parcelles qui forme un terrain de 5415m2.

## Adresse

34 Rue de la Fontaine, 37370 Neuvy-le-Roi

### Critères de recherche :
- Commune : Neuvy-le-Roi
- Surface : 5415
- Surface habitable : 250
- DPE : D
- GES : C
- Tolérance : 0
- Communes voisines de rang 2 : false

---

## Parcelles

Parcelle cadastrale
N° parcelle : 1436

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 3135

---

Parcelle cadastrale
N° parcelle : 1290

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 787

---

Parcelle cadastrale
N° parcelle : 1435

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 1363

---

Parcelle cadastrale
N° parcelle : 1437

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 15

---

Parcelle cadastrale
N° parcelle : 1434

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 92

---

Parcelle cadastrale
N° parcelle : 1289

Feuille : 4

Section : 0D

N° INSEE commune : 37170

Contenance : 23

2026-05-03 11:17:46.464 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:43 - Phase 1 : DPE et Strict Match
2026-05-03 11:17:46.532 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'NEUVY LE ROI' → 1 résultat(s) local/locaux
2026-05-03 11:17:46.743 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'NEUVY LE ROI' → 1 résultat(s) local/locaux
2026-05-03 11:17:46.743 | INFO     | cadastre_finder.search.strict_match:search_strict:62 - [strict_match] Recherche 'NEUVY LE ROI' (37170), surface [5415, 5415] m², terrain >= 2500 m²
2026-05-03 11:17:46.867 | INFO     | cadastre_finder.search.strict_match:search_strict:112 - [strict_match] 0 parcelle(s) trouvée(s).
2026-05-03 11:17:46.976 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:67 - Phase 2 : Local Combo Match
2026-05-03 11:17:47.044 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'NEUVY LE ROI' → 1 résultat(s) local/locaux
2026-05-03 11:17:47.146 | INFO     | cadastre_finder.search.combo_match:search_combos:387 - [combo_match] Recherche combos sur 7 commune(s), cible 5415 m² ±0.0%, max 6 parcelles, candidats [10–5365] m²
2026-05-03 11:17:47.269 | INFO     | cadastre_finder.search.combo_match:search_combos:394 - [combo_match] 18798 parcelles candidates
2026-05-03 11:17:47.782 | DEBUG    | cadastre_finder.search.combo_match:_get_adjacency:147 - [combo_match] Adjacence chargée depuis la table pré-calculée.
2026-05-03 11:17:47.783 | INFO     | cadastre_finder.search.combo_match:search_combos:400 - [combo_match] 30573 paires adjacentes
2026-05-03 11:17:49.182 | INFO     | cadastre_finder.search.combo_match:search_combos:424 - [combo_match] 5124 ancres (>= 65m² bâti) + 4871 voisins non-ancres
2026-05-03 11:17:50.622 | WARNING  | cadastre_finder.search.combo_match:_find_combos_dfs:295 - [combo_match] Plafond DFS atteint (500,000 nœuds). Résultats partiels. Réduisez la tolérance ou installez la table d'adjacence pré-calculée.
2026-05-03 11:17:51.382 | INFO     | cadastre_finder.search.building_filter:filter_built_combos:143 - [building_filter] 5 combo(s) non bâti(s) exclu(s).
2026-05-03 11:17:52.227 | INFO     | cadastre_finder.search.combo_match:search_combos:439 - [combo_match] 20 combo(s) trouvé(s).
2026-05-03 11:17:52.364 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 63% > 45% (3418 m² bâti sur 5415 m²)
2026-05-03 11:17:52.364 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 47% > 45% (2566 m² bâti sur 5415 m²)

Trouvé en 1er résultat.
