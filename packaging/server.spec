# -*- mode: python ; coding: utf-8 -*-
#
# PyInstaller spec pour le backend FastAPI de cadastre-finder.
# Produit un bundle one-folder dans dist/backend/.
#
# Usage (depuis la racine du projet) :
#   pyinstaller packaging/server.spec
#
import sys
from pathlib import Path

ROOT = Path(SPECPATH).parent
FRONTEND_DIST = ROOT / "frontend" / "dist" / "frontend" / "browser"

# ── Extension spatiale DuckDB ──────────────────────────────────────────────
# Bundler l'extension depuis la machine de build pour éviter le téléchargement
# réseau sur les machines cibles (INSTALL spatial échouerait sans internet).
import duckdb as _ddb

_ddb_version = _ddb.__version__
_ddb_arch = "windows_amd64"
_ddb_ext_src = (
    Path.home() / ".duckdb" / "extensions" / _ddb_version / _ddb_arch
)
_ddb_ext_file = _ddb_ext_src / "spatial.duckdb_extension"
_ddb_ext_dest = f"duckdb_ext/{_ddb_version}/{_ddb_arch}"

if not _ddb_ext_file.exists():
    print(
        f"AVERTISSEMENT : {_ddb_ext_file} introuvable.\n"
        "Lancez 'python -c \"import duckdb; duckdb.execute(\'INSTALL spatial\')\"' "
        "sur la machine de build pour pré-installer l'extension."
    )
    _ddb_datas = []
else:
    _ddb_datas = [(str(_ddb_ext_file), _ddb_ext_dest)]

a = Analysis(
    [str(ROOT / "packaging" / "server_entry.py")],
    pathex=[str(ROOT / "src")],
    binaries=[],
    datas=[
        # Angular static files embarqués dans le bundle
        (str(FRONTEND_DIST), "frontend_static"),
        # Settings TOML du projet
        (str(ROOT / "src" / "cadastre_finder" / "settings.toml"), "cadastre_finder"),
        # Extension spatiale DuckDB (évite téléchargement sur machine cible)
        *_ddb_datas,
    ],
    hiddenimports=[
        # FastAPI / uvicorn
        "uvicorn.logging",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.lifespan",
        "uvicorn.lifespan.on",
        "fastapi",
        "starlette",
        # DuckDB
        "duckdb",
        # Géométrie
        "geopandas",
        "shapely",
        "pyproj",
        "pyproj.transformer",
        # Modules du projet
        "cadastre_finder",
        "cadastre_finder.api",
        "cadastre_finder.api.main",
        "cadastre_finder.api.routes.communes",
        "cadastre_finder.api.routes.search",
        "cadastre_finder.api.routes.parse",
        "cadastre_finder.api.schemas",
        "cadastre_finder.search",
        "cadastre_finder.search.orchestrator",
        "cadastre_finder.search.models",
        "cadastre_finder.search.strict_match",
        "cadastre_finder.search.combo_match",
        "cadastre_finder.search.neighbor_match",
        "cadastre_finder.search.dpe_match",
        "cadastre_finder.search.external_search",
        "cadastre_finder.search.proximity_match",
        "cadastre_finder.search.ad_parser",
        "cadastre_finder.processing.adjacency",
        "cadastre_finder.utils.geocoding",
        "cadastre_finder.config",
    ],
    excludes=[
        # Ingestion non nécessaire en production
        "osmium",
        "cadastre_finder.ingestion",
        "cadastre_finder.cli",
        # UI Streamlit non nécessaire
        "streamlit",
        "cadastre_finder.ui",
        # Folium (cartes HTML serveur) remplacé par Leaflet côté client
        "folium",
        # Tests
        "pytest",
        "pytest_cov",
    ],
    noarchive=False,
    optimize=1,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="server",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,  # console=False masque la fenêtre CMD (optionnel)
    disable_windowed_traceback=False,
    argv_emulation=False,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="backend",
)
