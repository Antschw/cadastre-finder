# Cadastre Finder

Outil de localisation de biens immobiliers par parcelle cadastrale, pour l'Ouest de la France.

## Structure

```
src/cadastre_finder/
├── config.py               # Configuration (départements, chemins, projections)
├── cli.py                  # Point d'entrée CLI
├── ingestion/
│   ├── cadastre.py         # Téléchargement et chargement cadastre Etalab → DuckDB
│   └── osm.py              # Filtrage OSM (POI, routes, rivières) → DuckDB
├── processing/
│   └── adjacency.py        # Table d'adjacence des communes
├── search/
│   ├── models.py           # Type ParcelMatch
│   ├── strict_match.py     # Étape 1 : match strict commune + surface
│   ├── neighbor_match.py   # Étape 2 : élargissement aux voisines
│   └── proximity_match.py  # Étape 3 : intersection de contraintes géométriques
├── output/
│   └── map.py              # Rendu cartographique Folium
└── utils/
    └── geocoding.py        # Géocodage commune → code INSEE
```

## Mise en route

```bash
# 1. Installer les dépendances
uv pip install -e ".[dev]"

# 2. Ingérer un département (ex: Orne)
cadastre-finder ingest --dept 61

# 3. Ingérer les données OSM
cadastre-finder ingest-osm --pbf data/raw/france-latest.osm.pbf

# 4. Calculer les adjacences
cadastre-finder build-adjacency

# 5. Rechercher
cadastre-finder search --commune "Mortagne-au-Perche" --surface 4200
```

## Tests

```bash
pytest
```

## Périmètre géographique

20 départements : 76, 27, 14, 50, 61, 28, 72, 53, 35, 22, 29, 56, 44, 49, 85, 79, 86, 37, 41, 45
