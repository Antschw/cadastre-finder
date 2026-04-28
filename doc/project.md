# Cadastre Finder — Localisateur de biens immobiliers

## Contexte et objectif

Outil d'aide à la localisation de maisons vues sur des sites d'annonces immobilières lorsque l'adresse exacte n'est pas communiquée. Le but est d'identifier la parcelle cadastrale exacte (ou un petit nombre de candidates) à partir des indices publics de l'annonce, afin d'éviter les déplacements inutiles vers des biens sur des axes bruyants ou mal situés.

**Périmètre géographique :** Ouest de la France, de Fécamp (Seine-Maritime) au nord jusqu'à Poitiers (Vienne) au sud. Soit environ 20 départements : 76, 27, 14, 50, 61, 28, 72, 53, 35, 22, 29, 56, 44, 49, 85, 79, 86, 37, 41, 45.

**Cible utilisateur :** un seul utilisateur (le propriétaire de l'outil), recherchant des maisons avec **terrain > 2500 m²**.

## Workflow de recherche (logique métier)

L'outil reproduit une stratégie en cascade qui correspond au raisonnement manuel de l'utilisateur :

### Étape 1 — Match strict (ville + surface exacte)

- Filtrer par code INSEE de la commune annoncée.
- Filtrer par `contenance` cadastrale strictement égale (tolérance 0).
- Si 1 à 3 résultats : succès probable, afficher avec ortho IGN + Street View.

### Étape 2 — Élargissement aux communes voisines

- Si étape 1 vide : étendre aux communes adjacentes (1er rang).
- Tolérance de surface ±5 %.
- Optionnel : voisines de 2e rang si toujours vide.
- Typiquement 5 à 15 candidates à examiner visuellement.

### Étape 3 — Recherche par périmètre de proximité

- Si étapes 1 et 2 échouent (cas de parcelles multiples, cadastre obsolète, etc.).
- Abandonner le filtre de surface.
- Construire une zone de recherche par **intersection de contraintes géométriques** issues des indices de l'annonce :
  - Proximité positive (gare, église, rivière, ville) → buffers à intersecter
  - Nuisances (autoroute, voie ferrée, départementale) → zones à exclure
- Retourner les parcelles bâties dans la zone résultante.

## Stack technique

- **Langage :** Python 3.12+
- **Base de données :** DuckDB avec extension `spatial`
- **Géospatial :** GeoPandas, Shapely, PyProj
- **HTTP :** httpx, requests
- **Visualisation :** Folium (MVP) puis MapLibre/Leaflet si UI dédiée
- **Projection métier :** Lambert-93 (EPSG:2154) pour tout calcul de distance/surface
- **Projection d'affichage :** WGS84 (EPSG:4326)

## Sources de données

| Source | Usage | Mode d'accès |
|---|---|---|
| Cadastre Etalab (`cadastre.data.gouv.fr/data/etalab-cadastre`) | Parcelles, communes | Téléchargement par département (GeoJSON) |
| Geofabrik `france-latest.osm.pbf` | POI, routes, rivières | Téléchargement unique |
| IGN WMTS Géoplateforme (`data.geopf.fr/wmts`) | Orthophotos haute résolution | À la volée pour affichage |
| API Adresse (`api-adresse.data.gouv.fr`) | Géocodage commune → code INSEE | API REST en ligne |
| Google Street View | Vérification visuelle | Lien externe par parcelle |

## Pièges connus à anticiper

- **Surface :** filtrer sur l'attribut `contenance` (m² légal), pas sur l'aire géométrique calculée — elles peuvent diverger.
- **Code postal ≠ INSEE :** un code postal peut couvrir plusieurs communes. Toujours travailler avec le code INSEE.
- **Projection :** ne jamais calculer un buffer ou une distance en WGS84 sur la zone d'étude, erreur de 5–10 %.
- **Parcelles multiples :** une maison peut être à cheval sur 2–3 parcelles cadastrales contiguës. Étape 3 conçue pour ce cas.
- **Surfaces arrondies :** les annonces arrondissent souvent à la dizaine ou centaine. Tolérance dynamique au-delà de l'étape 1.

---

## Découpage en tâches pour agents IA

Chaque tâche est conçue pour être **autonome**, avec entrées/sorties claires et critères d'acceptation testables. Les tâches sont à exécuter dans l'ordre indiqué (les dépendances sont notées).

### T01 — Bootstrap projet et structure de base

**Dépendances :** aucune

**Objectif :** initialiser le projet Python avec une structure propre.

**Livrables :**
- Repo Git avec `pyproject.toml` (gestionnaire `uv` ou `poetry`)
- Structure : `src/cadastre_finder/`, `tests/`, `data/raw/`, `data/processed/`, `notebooks/`
- Dépendances installées : `duckdb`, `geopandas`, `shapely`, `pyproj`, `httpx`, `folium`, `pytest`, `tqdm`
- `.gitignore` excluant `data/` et fichiers volumineux
- README minimal décrivant la structure
- Fichier `config.py` ou `settings.toml` listant les 20 codes département du périmètre

**Critères d'acceptation :**
- `pytest` s'exécute sans erreur (même avec 0 test)
- `python -c "import duckdb; duckdb.connect().execute('INSTALL spatial; LOAD spatial;')"` fonctionne

---

### T02 — Module d'ingestion cadastre par département

**Dépendances :** T01

**Objectif :** télécharger et charger les données cadastre Etalab dans DuckDB.

**Livrables :**
- Module `src/cadastre_finder/ingestion/cadastre.py` exposant `download_department(dept_code: str)` et `load_department_to_duckdb(dept_code: str, db_path: Path)`
- Téléchargement depuis `cadastre.data.gouv.fr/data/etalab-cadastre/<latest>/geojson/departements/<dept>/cadastre-<dept>-parcelles.json.gz`
- Téléchargement aussi du fichier communes : `cadastre-<dept>-communes.json.gz`
- Tables DuckDB créées :
  - `parcelles` (id, code_insee, code_dept, prefixe, section, numero, contenance, geometry)
  - `communes` (code_insee, nom, code_dept, geometry)
- Index sur `(code_insee, contenance)` dans `parcelles`
- CLI : `python -m cadastre_finder.ingestion.cadastre --dept 61`
- Logs de progression avec `tqdm`

**Critères d'acceptation :**
- Sur le dept 61 : table `parcelles` contient > 100 000 lignes
- Requête `SELECT COUNT(*) FROM parcelles WHERE contenance > 2500 AND code_insee = '61001'` renvoie un entier cohérent
- Idempotence : relancer ne duplique pas les données

---

### T03 — Ingestion OSM (POI, routes, hydrographie)

**Dépendances :** T01

**Objectif :** filtrer un extrait `france-latest.osm.pbf` et charger les éléments utiles dans DuckDB.

**Livrables :**
- Module `src/cadastre_finder/ingestion/osm.py`
- Utilisation de `osmium` (CLI ou bindings Python `osmium` ou `pyosmium`)
- Filtrage en plusieurs couches :
  - `poi_religious` : `amenity=place_of_worship`, `building=church`, `man_made=tower` avec `tower:type=bell_tower`
  - `poi_transport` : `railway=station`, `railway=halt`, `public_transport=station`
  - `poi_admin` : `amenity=townhall`, `amenity=school`, `amenity=post_office`
  - `roads_major` : `highway` in (`motorway`, `trunk`, `primary`, `secondary`)
  - `railways` : `railway=rail` (lignes actives uniquement)
  - `waterways` : `waterway` in (`river`, `stream`, `canal`)
  - `buildings` : `building=*` (utile pour étape "filtrer parcelles bâties")
- Chaque couche dans une table DuckDB avec géométrie en EPSG:4326
- Index spatial (`CREATE INDEX ... USING RTREE`) sur chaque table
- CLI : `python -m cadastre_finder.ingestion.osm --pbf data/raw/france-latest.osm.pbf`

**Critères d'acceptation :**
- Toutes les tables existent et sont non vides
- Requête de test : nombre d'églises dans le dept 61 > 400
- Une requête `ST_DWithin` sur les routes prend < 1s sur un point donné

---

### T04 — Calcul de la table d'adjacence des communes

**Dépendances :** T02

**Objectif :** précalculer les voisinages de communes pour l'étape 2 du workflow.

**Livrables :**
- Module `src/cadastre_finder/processing/adjacency.py`
- Fonction `build_adjacency_table(db_path: Path, db_name: str = 'communes_adj')`
- Table DuckDB `communes_adjacency` (code_insee_a, code_insee_b, rang) où `rang=1` si elles se touchent (`ST_Touches` ou `ST_Intersects` avec buffer minimal de 1m pour gérer les imprécisions topologiques)
- Optionnel : pré-calcul du rang 2 (voisines des voisines) dans une vue ou table dédiée
- Index sur `code_insee_a`

**Critères d'acceptation :**
- Pour une commune au cœur d'un département : entre 4 et 10 voisines de rang 1
- Symétrie vérifiée : si (A, B) existe alors (B, A) existe
- Les communes en bord de département incluent bien des voisines hors département (à valider après ingestion de plusieurs départements)

---

### T05 — Module de géocodage commune → code INSEE

**Dépendances :** T02

**Objectif :** convertir un nom de commune (potentiellement avec code postal) en code INSEE fiable.

**Livrables :**
- Module `src/cadastre_finder/utils/geocoding.py`
- Fonction `resolve_commune(name: str, postal_code: Optional[str] = None) -> CommuneInfo`
- Stratégie en 2 temps : recherche locale dans la table `communes` chargée, fallback sur l'API Adresse (`api-adresse.data.gouv.fr/search/?q=...&type=municipality`)
- Gestion des homonymes (ex: "Saint-Martin" est très ambigu) : si plusieurs candidats, retourne la liste avec scores
- Cache local des résultats API (simple JSON ou table DuckDB)

**Critères d'acceptation :**
- `resolve_commune("Mortagne-au-Perche")` renvoie `61293`
- `resolve_commune("Saint-Martin", postal_code="61000")` désambiguïse correctement
- Tests unitaires sur 10 communes connues

---

### T06 — Moteur de recherche : étape 1 (match strict)

**Dépendances :** T02, T05

**Objectif :** implémenter le premier filtre de la cascade.

**Livrables :**
- Module `src/cadastre_finder/search/strict_match.py`
- Fonction `search_strict(commune: str, surface_m2: float, postal_code: Optional[str] = None, min_surface: float = 2500) -> list[ParcelMatch]`
- Type `ParcelMatch` (dataclass ou Pydantic) avec : `id_parcelle`, `code_insee`, `nom_commune`, `contenance`, `centroid_lat`, `centroid_lon`, `geometry_geojson`, `score`
- Tolérance par défaut = 0, paramétrable
- Filtre minimal `contenance > min_surface`

**Critères d'acceptation :**
- Sur un cas test connu (parcelle réelle identifiée à la main), la fonction la retourne
- Performance : < 100 ms par requête sur un département chargé

---

### T07 — Moteur de recherche : étape 2 (voisines + tolérance)

**Dépendances :** T04, T06

**Objectif :** étendre la recherche aux communes adjacentes.

**Livrables :**
- Module `src/cadastre_finder/search/neighbor_match.py`
- Fonction `search_with_neighbors(commune: str, surface_m2: float, tolerance_pct: float = 5.0, include_rank2: bool = False, ...) -> list[ParcelMatch]`
- Score : +10 si dans la commune annoncée, +3 si voisine rang 1, +1 si rang 2
- Tri du résultat par score décroissant puis écart de surface croissant
- Limite par défaut : top 20

**Critères d'acceptation :**
- Pour une commune avec 6 voisines, le résultat contient des parcelles des 7 communes
- Le scoring trie correctement (parcelle de la commune annoncée avec surface exacte = score le plus élevé)

---

### T08 — Moteur de recherche : étape 3 (périmètre par proximités)

**Dépendances :** T03, T06

**Objectif :** construire une zone de recherche par intersection de contraintes géométriques.

**Livrables :**
- Module `src/cadastre_finder/search/proximity_match.py`
- API déclarative : l'utilisateur définit une liste de contraintes
  ```python
  constraints = [
      NearPOI(category="church", name="Saint-Pierre", max_distance_m=1500),
      NearPOI(category="station", commune="Mortagne", max_distance_m=3000),
      AwayFromFeature(category="motorway", min_distance_m=300),
      AwayFromFeature(category="railway", min_distance_m=150),
      InCommuneOrNeighbors(commune="Mortagne-au-Perche", rank=2),
  ]
  ```
- Algorithme :
  1. Reprojeter en Lambert-93
  2. Pour chaque contrainte positive : calculer le buffer
  3. `ST_Intersection` successive de tous les buffers positifs
  4. `ST_Difference` des zones d'exclusion
  5. Intersecter avec les parcelles bâties (jointure avec `buildings` OSM)
  6. Retour en EPSG:4326 pour affichage
- Fonction `search_by_proximity(constraints: list[Constraint], min_surface: float = 2500) -> list[ParcelMatch]`

**Critères d'acceptation :**
- Sur un cas synthétique (3 contraintes), le polygone de recherche correspond géométriquement à l'attendu
- Performance : < 5s pour une requête typique sur 1 département

---

### T09 — Sortie cartographique Folium

**Dépendances :** T06

**Objectif :** afficher les résultats de recherche sur une carte HTML interactive.

**Livrables :**
- Module `src/cadastre_finder/output/map.py`
- Fonction `render_results(matches: list[ParcelMatch], output_path: Path, query_info: dict)`
- Pour chaque parcelle :
  - Polygone sur la carte (couleur selon score)
  - Popup avec : id parcelle, commune, contenance, score, lien Google Street View centré sur le centroïde, lien Géoportail
  - Image ortho IGN miniature dans le popup (via WMTS, URL directe)
- Couche de fond : OSM standard + layer toggle vers ortho IGN
- Centrage automatique sur l'enveloppe des résultats

**Critères d'acceptation :**
- Le HTML s'ouvre dans un navigateur et est interactif
- Les liens Street View pointent bien vers le bon endroit
- L'ortho IGN s'affiche correctement (test à la souris)

---

### T10 — CLI principale

**Dépendances :** T06, T07, T08, T09

**Objectif :** point d'entrée unique pour utiliser l'outil.

**Livrables :**
- `src/cadastre_finder/cli.py` exposant les sous-commandes :
  - `cadastre-finder ingest --dept 61` → T02 + T03
  - `cadastre-finder build-adjacency` → T04
  - `cadastre-finder search --commune "Mortagne-au-Perche" --surface 4200` → cascade auto étapes 1→2 (et message si étape 3 nécessaire)
  - `cadastre-finder search-area --config constraints.yaml` → étape 3
- Sortie par défaut : carte HTML dans `output/`, ouverture auto dans le navigateur (option `--no-open`)
- Logs propres avec niveaux configurables

**Critères d'acceptation :**
- Le scénario de bout en bout sur un cas réel produit la carte attendue
- `--help` documente toutes les sous-commandes

---

### T11 — Industrialisation : ingestion de tous les départements

**Dépendances :** T02, T03, T04

**Objectif :** orchestrer le chargement complet du périmètre de 20 départements.

**Livrables :**
- Script `scripts/ingest_all.py` qui itère sur la liste de départements
- Reprise sur erreur (skip département déjà chargé)
- Estimation du temps total et de la taille disque attendue
- Documentation README mise à jour avec les étapes de mise en route

**Critères d'acceptation :**
- Lancement à blanc (sans téléchargement réel) montre la liste correcte
- Une exécution réelle complète tous les départements sans intervention

---

### T12 — Tests d'intégration sur cas réels

**Dépendances :** T10, T11

**Objectif :** valider la qualité du matching sur des annonces déjà résolues à la main.

**Livrables :**
- Dossier `tests/fixtures/real_cases/` avec 10–20 cas réels (anonymisés) :
  - Description : commune annoncée, surface, indices de proximité
  - Vérité terrain : id de parcelle attendu
- Suite `tests/test_real_cases.py` qui exécute la cascade et vérifie que la bonne parcelle est dans le top 5
- Rapport de précision : % de cas résolus à l'étape 1, étape 2, étape 3, non résolus

**Critères d'acceptation :**
- > 70 % des cas résolus à l'étape 1 ou 2
- 100 % des cas avec parcelle unique et surface annoncée exacte résolus à l'étape 1

---

## Notes pour les agents

- Toujours filtrer `contenance > 2500` au plus tôt dans les requêtes : c'est le filtre le plus discriminant.
- Utiliser systématiquement Lambert-93 pour les calculs géométriques, WGS84 pour les I/O.
- Privilégier DuckDB sur GeoPandas dès qu'il y a un join sur > 100k lignes.
- Pas d'API en runtime sauf API Adresse (cache obligatoire) et IGN WMTS (tuiles à l'affichage).
- Logs : `loguru` ou `logging` standard, pas de `print`.
- Tests : `pytest`, fixtures réutilisables, pas de fixtures qui dépendent du réseau.