"""Configuration centrale du projet cadastre-finder."""
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

# Répertoires
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_PROCESSED = ROOT_DIR / "data" / "processed"
OUTPUT_DIR = ROOT_DIR / "out"

# Sous-arborescence data/raw
RAW_OSM_DIR = DATA_RAW / "osm"
RAW_CADASTRE_DIR = DATA_RAW / "cadastre"
RAW_CADASTRE_COMMUNES_DIR = RAW_CADASTRE_DIR / "communes"
RAW_CADASTRE_PARCELLES_DIR = RAW_CADASTRE_DIR / "parcelles"
RAW_ADEME_DIR = DATA_RAW / "ademe"

# Base DuckDB par défaut
DB_PATH = DATA_PROCESSED / "cadastre.duckdb"

# 21 départements du périmètre : Ouest de la France + Oise
DEPARTMENTS = [
    "76",  # Seine-Maritime
    "27",  # Eure
    "14",  # Calvados
    "50",  # Manche
    "61",  # Orne
    "28",  # Eure-et-Loir
    "72",  # Sarthe
    "53",  # Mayenne
    "35",  # Ille-et-Vilaine
    "22",  # Côtes-d'Armor
    "29",  # Finistère
    "56",  # Morbihan
    "44",  # Loire-Atlantique
    "49",  # Maine-et-Loire
    "85",  # Vendée
    "79",  # Deux-Sèvres
    "86",  # Vienne
    "37",  # Indre-et-Loire
    "41",  # Loir-et-Cher
    "45",  # Loiret
    "60",  # Oise
]

# URLs sources
CADASTRE_BASE_URL = "https://cadastre.data.gouv.fr/data/etalab-cadastre"
GEOPF_API_URL = "https://data.geopf.fr/geocodage"
# Dataset national "DPE Logements existants depuis juillet 2021" (meg-83tjwtg8dyz4vv7h1dqe)
ADEME_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe"
IGN_APICARTO_URL = "https://apicarto.ign.fr/api/cadastre"

# Projections
CRS_LAMBERT93 = "EPSG:2154"
CRS_WGS84 = "EPSG:4326"

# Seuils métier
MIN_TERRAIN_M2 = 2500
MIN_ANCHOR_BUILT_M2 = 65
MAX_BUILT_RATIO = 0.50          # exclusion : terrain dont le bâti dépasse 50%
MIN_COMPACTNESS = 0.10          # exclusion : compacité Polsby-Popper < 0.1
DEFAULT_TOLERANCE_PCT = 5.0
DEFAULT_TOP_N = 20

# Poids du scoring continu (search_orchestrated)
SCORE_W_DISTANCE = 20.0
SCORE_W_SURFACE = 40.0
SCORE_W_OCCUPATION = 15.0
SCORE_W_COMPACT = 10.0
SCORE_BONUS_DPE_PARCEL = 500.0
SCORE_BONUS_DPE_LABEL = 100.0

# DPE settings
DPE_TABLE = "dpe"
# ADEME DPE Open Data — Logements existants depuis juillet 2021
# Dataset : https://data.ademe.fr/datasets/dpe03existant
DPE_URL = "https://data.ademe.fr/streamsaver/data.ademe.fr/645298/dpe03existant.csv"
DPE_CSV_PATH = RAW_ADEME_DIR / "dpe03existant.csv"
