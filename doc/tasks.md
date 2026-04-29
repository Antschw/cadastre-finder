# Tâches à faire

## Performance

- [x] **Pré-calculer la table d'adjacence parcellaire** *(fait)*
  Table `parcelles_adjacency(id_a, id_b)` pré-calculée commune par commune.
  `search_combos` l'utilise automatiquement si disponible, sinon fallback spatial.
  Lancer : `cadastre-finder build-parcel-adjacency [--dept 61]`

## Bugs connus (non bloquants)

- [x] **`build-parcel-adjacency` saturait la RAM et plantait** *(corrigé)*
  La commande chargeait tout en mémoire sans limite, causant un OOM kill.
  Corrections appliquées (`parcel_adjacency.py`) :
  - Limite mémoire DuckDB à 1200 MB (`SET memory_limit`) avec répertoire temporaire pour
    déverser sur disque (`SET temp_directory`).
  - Checkpoint périodique toutes les 50 communes pour vider le WAL.
  - Grandes communes (> 3 000 parcelles) découpées en 4 tuiles spatiales pour limiter la
    taille de chaque jointure.
  - Fallback : si la table est vide pour les communes demandées, `search_combos` bascule
    automatiquement sur la jointure spatiale à la volée (`combo_match.py`).
