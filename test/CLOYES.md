# CLOYES

## Description

Maison à Cloyes-les-Trois-Rivières composé de 3 parcelles qui forme un terrain de 3255m2.

## Adresse

23 Rue de Châteaudun, 28220 Cloyes-les-Trois-Rivières

### Critères de recherche :

- Commune : Cloyes-les-Trois-Rivières 
- Surface : 3300
- Surface habitable : 250
- DPE : C
- GES : A
- Tolérance : 50
- Communes voisines de rang 2 : false

## Parcelles

Parcelle cadastrale   

  N° parcelle : 0322                                                                                                                                                                                                                 
                                                                                                                                                                                                                                     
  Feuille : 1                                                                                                                                                                                                                        
                                                                                                                                                                                                                                     
  Section : AB                                                                                                                                                                                                                       
                                                                                                                                                                                                                                     
  N° INSEE commune : 28103                                                                                                                                                                                                           
                                                                                                                                                                                                                                     
  Contenance : 442 

---

Parcelle cadastrale

  N° parcelle : 0321                                                                                                                                                                                                                 
                                                                                                                                                                                                                                     
  Feuille : 1                                                                                                                                                                                                                        
                                                                                                                                                                                                                                     
  Section : AB                                                                                                                                                                                                                       
                                                                                                                                                                                                                                     
  N° INSEE commune : 28103                                                                                                                                                                                                           
                                                                                                                                                                                                                                     
  Contenance : 1540 

---

Parcelle cadastrale    

  N° parcelle : 0280                                                                                                                                                                                                                 
                                                                                                                                                                                                                                     
  Feuille : 1                                                                                                                                                                                                                        
                                                                                                                                                                                                                                     
  Section : AB                                                                                                                                                                                                                       
                                                                                                                                                                                                                                     
  N° INSEE commune : 28103                                                                                                                                                                                                           
                                                                                                                                                                                                                                     
  Contenance : 1273 

2026-05-03 11:24:33.873 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:43 - Phase 1 : DPE et Strict Match
2026-05-03 11:24:33.959 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'CLOYES-LES-TROIS-RIVIERES' → 1 résultat(s) local/locaux
2026-05-03 11:24:34.545 | DEBUG    | cadastre_finder.utils.geocoding:geocode_address:174 - [geocoding] Erreur géocodage adresse '23 Rue de Chateaudun, 28220 CLOYES SUR LE LOIR': Client error '400 Bad Request' for url 'https://api-adresse.data.gouv.fr/search/?q=23+Rue+de+Chateaudun%2C+28220+CLOYES+SUR+LE+LOIR&city=CLOYES+SUR+LE+LOIR&postcode=28220'                                                                                                                                                                                                                                                                             
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400                                                                                                                                                                                                                             
2026-05-03 11:24:34.900 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'CLOYES-LES-TROIS-RIVIERES' → 1 résultat(s) local/locaux
2026-05-03 11:24:34.900 | INFO     | cadastre_finder.search.strict_match:search_strict:62 - [strict_match] Recherche 'CLOYES-LES-TROIS-RIVIERES' (28103), surface [3250, 3350] m², terrain >= 2500 m²
2026-05-03 11:24:40.305 | INFO     | cadastre_finder.search.building_filter:filter_built_parcels:61 - [building_filter] 32 parcelle(s) non bâtie(s) exclue(s).
2026-05-03 11:24:41.514 | INFO     | cadastre_finder.search.strict_match:search_strict:112 - [strict_match] 14 parcelle(s) trouvée(s).
2026-05-03 11:24:49.167 | INFO     | cadastre_finder.search.building_filter:filter_anchors:100 - [building_filter] 1 parcelle(s) bâties rejetées (sous le seuil ancre de 65m²).
2026-05-03 11:24:50.545 | INFO     | cadastre_finder.search.orchestrator:search_orchestrated:67 - Phase 2 : Local Combo Match
2026-05-03 11:24:50.623 | DEBUG    | cadastre_finder.utils.geocoding:resolve_commune:198 - [geocoding] 'CLOYES-LES-TROIS-RIVIERES' → 1 résultat(s) local/locaux
2026-05-03 11:24:50.733 | INFO     | cadastre_finder.search.combo_match:search_combos:387 - [combo_match] Recherche combos sur 12 commune(s), cible 3300 m² ±1.5151515151515151%, max 6 parcelles, candidats [10–3300] m²
2026-05-03 11:24:51.360 | INFO     | cadastre_finder.search.combo_match:search_combos:394 - [combo_match] 28280 parcelles candidates
2026-05-03 11:24:58.172 | DEBUG    | cadastre_finder.search.combo_match:_get_adjacency:147 - [combo_match] Adjacence chargée depuis la table pré-calculée.
2026-05-03 11:24:58.173 | INFO     | cadastre_finder.search.combo_match:search_combos:400 - [combo_match] 44622 paires adjacentes
2026-05-03 11:25:09.415 | INFO     | cadastre_finder.search.combo_match:search_combos:424 - [combo_match] 12450 ancres (>= 65m² bâti) + 8747 voisins non-ancres
2026-05-03 11:25:19.188 | WARNING  | cadastre_finder.search.combo_match:_find_combos_dfs:295 - [combo_match] Plafond DFS atteint (500,000 nœuds). Résultats partiels. Réduisez la tolérance ou installez la table d'adjacence pré-calculée.
2026-05-03 11:25:26.166 | INFO     | cadastre_finder.search.building_filter:filter_built_combos:143 - [building_filter] 5 combo(s) non bâti(s) exclu(s).
2026-05-03 11:25:27.924 | INFO     | cadastre_finder.search.combo_match:search_combos:439 - [combo_match] 20 combo(s) trouvé(s).
2026-05-03 11:25:28.061 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 57% > 45% (1875 m² bâti sur 3306 m²)
2026-05-03 11:25:28.061 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 85% > 45% (2832 m² bâti sur 3329 m²)
2026-05-03 11:25:28.061 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 46% > 45% (1510 m² bâti sur 3295 m²)
2026-05-03 11:25:28.061 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 79% > 45% (2616 m² bâti sur 3309 m²)
2026-05-03 11:25:28.061 | DEBUG    | cadastre_finder.search.orchestrator:_sort_and_limit:99 - [scoring] Résultat exclu : ratio bâti 53% > 45% (1747 m² bâti sur 3290 m²)

Non trouvé