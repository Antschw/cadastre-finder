# HARQUENCY

## Description

Maison à Harquency composé de 5 parcelles qui forme un terrain de 6024m2.

# Adresse

4 Rte des Templiers, 27700 Harquency

### Critères de recherche :
- Commune : Les Andelys
- Surface : 6024
- Surface habitable : 268
- DPE : F
- GES : F
- Tolérance : 0
- Communes voisines de rang 2 : true

---

## Parcelles

Parcelle cadastrale
N° parcelle : 0172

Feuille : 1

Section : AB

N° INSEE commune : 27315

Contenance : 492

---

Parcelle cadastrale
N° parcelle : 0006

Feuille : 1

Section : AB

N° INSEE commune : 27315

Contenance : 1237

---

Parcelle cadastrale
N° parcelle : 0007

Feuille : 1

Section : AB

N° INSEE commune : 27315

Contenance : 4090

---

Parcelle cadastrale
N° parcelle : 0173

Feuille : 1

Section : AB

N° INSEE commune : 27315

Contenance : 152

---

Parcelle cadastrale
N° parcelle : 0174

Feuille : 1

Section : AB

N° INSEE commune : 27315

Contenance : 53

2026-05-03 11:21:41.713 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:43 - Phase 1 : DPE et Strict Match
2026-05-03 11:21:41.797 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'LES ANDELYS' → 1 résultat(s) local/locaux
2026-05-03 11:21:42.019 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'LES ANDELYS' → 1 résultat(s) local/locaux
2026-05-03 11:21:42.020 | INFO     | cadastre_finder.search.strict_match:search_strict:62 - [strict_match] Recherche 'LES ANDELYS' (27016), surface [6024, 6024] m², terrain >= 2500 m²
2026-05-03 11:21:42.173 | INFO     | cadastre_finder.search.strict_match:search_strict:112 - [strict_match] 0 parcelle(s) trouvée(s).
2026-05-03 11:21:42.289 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:67 - Phase 2 : Local Combo Match
2026-05-03 11:21:42.344 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'LES ANDELYS' → 1 résultat(s) local/locaux
2026-05-03 11:21:42.444 | INFO     | cadastre_finder.search.combo_match:search_combos:387 - [combo_match] Recherche combos sur 10 commune(s), cible 6024 m² ±0.0%, max 6 parcelles, candidats [12–5974] m²
2026-05-03 11:21:42.727 | INFO     | cadastre_finder.search.combo_match:search_combos:394 - [combo_match] 16587 parcelles candidates
2026-05-03 11:21:44.026 | DEBUG    | cadastre_finder.search.combo_match:_get_adjacency:147 - [combo_match] Adjacence chargée depuis la table pré-calculée.
2026-05-03 11:21:44.027 | INFO     | cadastre_finder.search.combo_match:search_combos:400 - [combo_match] 28671 paires adjacentes
2026-05-03 11:21:47.924 | INFO     | cadastre_finder.search.combo_match:search_combos:424 - [combo_match] 6044 ancres (>= 65m² bâti) + 3726 voisins non-ancres
2026-05-03 11:21:48.988 | WARNING  | cadastre_finder.search.combo_match:_find_combos_dfs:295 - [combo_match] Plafond DFS atteint (500,000 nœuds). Résultats partiels. Réduisez la tolérance ou installez la table d'adjacence pré-calculée.
2026-05-03 11:21:51.340 | INFO     | cadastre_finder.search.building_filter:filter_built_combos:143 - [building_filter] 1 combo(s) non bâti(s) exclu(s).
2026-05-03 11:21:52.453 | INFO     | cadastre_finder.search.combo_match:search_combos:439 - [combo_match] 20 combo(s) trouvé(s).
2026-05-03 11:21:52.580 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 53% > 45% (3194 m² bâti sur 6024 m²)
2026-05-03 11:21:52.580 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 60% > 45% (3629 m² bâti sur 6024 m²)
2026-05-03 11:21:52.580 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 67% > 45% (4062 m² bâti sur 6024 m²)

Trouvé en 2ème résultat.