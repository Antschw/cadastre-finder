# Tâches à faire

## Performance

- [x] **Pré-calculer la table d'adjacence parcellaire** *(fait)*
  Table `parcelles_adjacency(id_a, id_b)` pré-calculée commune par commune.
  `search_combos` l'utilise automatiquement si disponible, sinon fallback spatial.
  Lancer : `cadastre-finder build-parcel-adjacency [--dept 61]`

## Bugs connus (non bloquants)

- [x] **`build-parcel-adjacency` saturait la RAM et plantait** *(corrigé)*
  L'ancienne approche (jointure spatiale `ST_Intersects` dans DuckDB) construisait un R-tree
  en RAM pour chaque commune avant de pouvoir commencer, causant un SIGKILL même avec tiling.
  Correction finale : remplacement complet par **Python + Shapely STRtree** (`parcel_adjacency.py`).
  - Shapely charge une commune à la fois, calcule les intersections en O(n log n) via STRtree
  - Libération mémoire explicite (`del geoms; gc.collect()`) après chaque commune
  - DuckDB limité à 600 MB (uniquement pour la lecture/écriture, pas le calcul spatial)
  - Insertion par batches de 20 000 paires avec `executemany`
  - Checkpoint toutes les 30 communes

## À faire — Interface

- [x] **Refonte UI** *(fait)*
  CSS minimal, `st.metric` / `st.progress` / `st.link_button` natifs, layout 1/3 info + 2/3
  carte, score affiché comme jauge colorée, suppression des badges HTML inline et des emojis.

- [x] **Autocomplétion du champ commune** *(fait)*
  `st.selectbox` alimenté par `@st.cache_resource` sur la table `communes` (~6 000 entrées),
  filtré dynamiquement à la frappe. Format : `"Nom (dept)"`. Fallback `text_input` si la
  table est vide.
