J'ai réorganisé l'arborescence des fichiers dans le dossier `data/raw`, prend en compte mes modifications.

Je pense qu'il y a eu une erreur dans le fichier osm pbf car ils utilisent les noms des anciennes régions de France
donc j'ai rajouté à la main dans `data/raw/osm` les fichiers suivants :
  - picardie-latest.osm.pbf
  - poitou-charentes-latest.osm.pbf
  - centre-latest.osm.pbf
  - pays-de-la-loire-latest.osm.pbf
  - basse-normandie-latest.osm.pbf
  - haute-normandie-latest.osm.pbf

J'ai mis la Picardie uniquement pour l'Oise (60) qui fait partie des départements de recherche.

J'ai rajouté à la main `cadastre-60-communes.json.gz` dans `data/raw/cadastre/communes` et
`cadastre-60-parcelles.json.gz` dans `data/raw/cadastre/parcelles`.

Ensuite, pour l'adjacence de communes, ça a été hyper rapide par-contre, j'ai l'impression que tu n'as pas fait
l'optimisation pour les adjacences de parcelles. Peux-tu aussi optimiser l'adjacence des parcelles et supprimer les
heartbeats de la console pour ne garder que de vrais logs qui indiquent une progression.

Pour le DPE, j'ai téléchargé un dump de la base de données globale `data/raw/ademe/dump_dpev2_prod_fdld.sql.gz` et j'ai
aussi téléchargé aussi la version csv `data/raw/ademe/dpe03existant.csv`. Les fichiers font respectivement 67Go et 28Go.

Et vu que tout est déjà téléchargé, il n'y a plus besoin de refaire les étapes de téléchargement dans le script.
