"""Configuration centrale du projet cadastre-finder."""
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

# Répertoires
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_PROCESSED = ROOT_DIR / "data" / "processed"
OUTPUT_DIR = ROOT_DIR / "out"

# Base DuckDB par défaut
DB_PATH = DATA_PROCESSED / "cadastre.duckdb"

# 20 départements du périmètre : Ouest de la France
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
]

# URLs sources
CADASTRE_BASE_URL = "https://cadastre.data.gouv.fr/data/etalab-cadastre"
API_ADRESSE_URL = "https://api-adresse.data.gouv.fr"

# Projections
CRS_LAMBERT93 = "EPSG:2154"
CRS_WGS84 = "EPSG:4326"

# Seuils métier
MIN_TERRAIN_M2 = 2500
DEFAULT_TOLERANCE_PCT = 5.0
DEFAULT_TOP_N = 20
