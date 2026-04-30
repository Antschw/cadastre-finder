# Cadastre Finder

Outil de localisation de biens immobiliers par parcelle cadastrale, optimisé pour l'Ouest de la France. Il permet de retrouver des propriétés à partir d'annonces immobilières en combinant les données du cadastre, d'OpenStreetMap (OSM) et les diagnostics de performance énergétique (DPE).

## Fonctionnalités

- **Recherche orchestrée en 4 phases** :
    1. **Identification par DPE** : Localisation directe via la base ADEME et géo-codage.
    2. **Recherche de parcelles individuelles** : Identification de parcelles correspondant à la surface recherchée.
    3. **Agrégation (Combos)** : Reconstruction de propriétés composées de plusieurs parcelles adjacentes (ex: maison, jardin et prairie).
    4. **Extension géographique** : Recherche élargie aux communes limitrophes (rang 1 et 2).
- **Analyse d'annonces** : Extraction automatique de la commune, des surfaces et des labels DPE/GES à partir d'un texte brut.
- **Logique de "Parcelle Ancre"** : Priorisation des parcelles contenant des bâtiments (> 65m²) pour limiter les faux positifs.
- **Interface de visualisation** : Cartographie interactive avec Folium et calcul de scores de compacité.
- **Critères spatiaux** : Filtrage par proximité de points d'intérêt (POI) ou éloignement de nuisances.

---

## Installation

Le projet utilise `uv` pour la gestion des dépendances.

```bash
# 1. Cloner le dépôt
git clone <url-du-repo>
cd house-finder

# 2. Installer les dépendances
uv pip install -e ".[dev]"

# 3. Installer osmium-tool (requis pour l'ingestion OSM)
sudo dnf install osmium-tool  # Fedora
# ou
sudo apt install osmium-tool # Ubuntu/Debian
```

---

## Alimentation de la base de données

### Méthode recommandée : commande unique

`build-database` enchaîne automatiquement, en une seule commande, toutes les étapes
nécessaires à la construction d'une base complète et cohérente :

1. Téléchargement parallèle des fichiers cadastre Etalab pour les 20 départements.
2. Téléchargement automatique des PBF régionaux Geofabrik (Bretagne, Normandie,
   Pays de la Loire, Centre-Val de Loire, Nouvelle-Aquitaine), fusion via
   `osmium merge` puis extraction sur la bounding box du périmètre.
3. Ingestion des couches OSM (bâtiments, POI, routes, voies ferrées, hydrographie).
4. Ingestion des données DPE de l'ADEME.
5. Pré-calcul des adjacences (communes rang 1 et 2, puis adjacence parcellaire).

```bash
# Construction complète, autonome et idempotente
python3 -m cadastre_finder.cli build-database
```

Caractéristiques :

- **Heartbeat** : un log au moins toutes les 60 secondes pendant les phases longues
  (téléchargements, fusion PBF, calcul d'adjacence) — le silence ne dépasse jamais
  cinq minutes.
- **Idempotence** : la commande peut être interrompue et relancée. Les fichiers
  déjà téléchargés ne sont pas re-téléchargés, les départements déjà chargés ne
  sont pas réinsérés, les communes déjà traitées pour l'adjacence sont conservées.
- **Optimisations DuckDB** : `threads = nombre de cœurs logiques`,
  `memory_limit = 24GB`, `temp_directory` placé sur le volume de la base
  (NVMe rapide). Tous ces réglages sont surchargeables via les options.
- **Téléchargements concurrents** : 8 connexions en parallèle pour le cadastre,
  4 pour les PBF Geofabrik.
- **Reprise sur erreur** : un échec d'OSM ou de DPE ne bloque pas les autres
  étapes. Un bilan final résume le statut (OK / SKIP / ERREUR) et la durée de
  chaque étape.

Options principales (toutes optionnelles) :

```bash
python3 -m cadastre_finder.cli build-database \
    --threads 32 \
    --memory-limit 24GB \
    --download-workers 8 \
    --dept 27 28 37            # ne traiter qu'un sous-ensemble de départements
```

Drapeaux `--skip-cadastre`, `--skip-osm`, `--skip-dpe`, `--skip-adjacency`,
`--skip-parcel-adjacency` pour relancer une étape précise sans tout reconstruire.
`--keep-intermediate-pbf` conserve les PBF régionaux après extraction (utile pour
des traitements OSM annexes).

### Méthode granulaire (étape par étape)

Pour un contrôle fin, les sous-commandes individuelles restent disponibles :

```bash
# 1. Cadastre Etalab
python3 -m cadastre_finder.cli ingest                # tous les départements
python3 -m cadastre_finder.cli ingest --dept 28      # un seul département

# 2. OSM (PBF déjà disponible localement)
python3 -m cadastre_finder.cli ingest-osm --pbf data/raw/ouest-france.osm.pbf

# 3. DPE ADEME
python3 -m cadastre_finder.cli ingest-dpe

# 4. Pré-calculs d'adjacence
python3 -m cadastre_finder.cli build-adjacency
python3 -m cadastre_finder.cli build-parcel-adjacency --dept 28

# Raccourci : toutes les adjacences d'un coup
python3 scripts/precompute_all.py
```

---

## Utilisation

### Interface Web
```bash
python3 -m cadastre_finder.cli ui
```

### Ligne de commande (CLI)
**Recherche depuis une annonce :**
```bash
python3 -m cadastre_finder.cli search --text "A vendre maison de 130m2 à Harquency, terrain de 6024m2. DPE: C"
```

**Recherche par paramètres :**
```bash
python3 -m cadastre_finder.cli search --commune "Neuvy-le-Roi" --surface 5415 --tolerance 10
```

---

## Développement et tests

Exécution de la suite de tests :
```bash
pytest tests/test_integration_*.py
```

## Périmètre géographique (20 départements)
76, 27, 14, 50, 61, 28, 72, 53, 35, 22, 29, 56, 44, 49, 85, 79, 86, 37, 41, 45.
