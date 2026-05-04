"""Module d'ingestion OSM (POI, routes, hydrographie) dans DuckDB.

Stratégie :
1. osmium tags-filter → filtre le PBF source → .osm.pbf intermédiaire
2. pyosmium SimpleHandler → lit le PBF filtré, extrait les géométries en WKT
3. pandas DataFrame → INSERT via DuckDB

Usage CLI : python -m cadastre_finder.ingestion.osm --pbf data/raw/france-latest.osm.pbf
"""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import duckdb
import osmium
import osmium.geom
import pandas as pd
from loguru import logger
from tqdm import tqdm

from cadastre_finder.config import DATA_RAW, DB_PATH

# ---------------------------------------------------------------------------
# Définition des couches OSM à extraire
# ---------------------------------------------------------------------------
OSM_LAYERS: dict[str, dict] = {
    "poi_religious": {
        "osmium_filter": [
            "n/amenity=place_of_worship",
            "w/amenity=place_of_worship",
            "n/building=church",
            "w/building=church",
            "n/man_made=tower",
            "w/man_made=tower",
        ],
        "description": "Lieux de culte, églises, clochers",
    },
    "poi_transport": {
        "osmium_filter": [
            "n/railway=station",
            "n/railway=halt",
            "n/public_transport=station",
            "w/railway=station",
        ],
        "description": "Gares et haltes ferroviaires",
    },
    "poi_admin": {
        "osmium_filter": [
            "n/amenity=townhall",
            "w/amenity=townhall",
            "n/amenity=school",
            "w/amenity=school",
            "n/amenity=post_office",
        ],
        "description": "Mairies, écoles, bureaux de poste",
    },
    "roads_major": {
        "osmium_filter": [
            "w/highway=motorway",
            "w/highway=trunk",
            "w/highway=primary",
            "w/highway=secondary",
        ],
        "description": "Routes principales (autoroute, nationale, départementale)",
    },
    "railways": {
        "osmium_filter": [
            "w/railway=rail",
        ],
        "description": "Lignes ferroviaires actives",
    },
    "waterways": {
        "osmium_filter": [
            "w/waterway=river",
            "w/waterway=stream",
            "w/waterway=canal",
        ],
        "description": "Cours d'eau",
    },
    "buildings": {
        "osmium_filter": [
            "w/building",
            "r/building",
        ],
        "description": "Bâtiments (pour filtrer parcelles bâties)",
    },
}


# ---------------------------------------------------------------------------
# Extraction osmium-tool → .osm.pbf
# ---------------------------------------------------------------------------

def _check_osmium() -> str:
    for cmd in ("osmium", "osmium-tool"):
        try:
            result = subprocess.run([cmd, "version"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                return cmd
        except FileNotFoundError:
            continue
    raise RuntimeError(
        "osmium-tool n'est pas installé. "
        "Installez-le avec : sudo dnf install osmium-tool  (ou apt install osmium-tool)"
    )


def _filter_to_pbf(
    pbf_path: Path,
    layer_name: str,
    layer_config: dict,
    osmium_cmd: str,
    tmp_dir: Path,
) -> Path:
    """Filtre le PBF source et produit un PBF intermédiaire (format toujours supporté)."""
    out_pbf = tmp_dir / f"{layer_name}.osm.pbf"
    if out_pbf.exists():
        return out_pbf

    filters = layer_config["osmium_filter"]
    cmd = [
        osmium_cmd, "tags-filter",
        str(pbf_path),
        *filters,
        "-o", str(out_pbf),
        "--overwrite",
    ]
    logger.info(f"[osm] Filtrage {layer_name}...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        raise RuntimeError(f"osmium a échoué pour {layer_name} : {result.stderr}")
    return out_pbf


# ---------------------------------------------------------------------------
# Lecture pyosmium → DuckDB par batches
# ---------------------------------------------------------------------------

BATCH_SIZE = 50_000


class _BatchGeomHandler(osmium.SimpleHandler):
    """Handler pyosmium qui écrit dans DuckDB par batches pour limiter la RAM."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        layer_name: str,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        super().__init__()
        self._con = con
        self._layer = layer_name
        self._batch_size = batch_size
        self._buf: list[dict] = []
        self._wktfab = osmium.geom.WKTFactory()
        self.total = 0

    def _append(self, osm_id: int, osm_type: str, tags: dict, wkt: str) -> None:
        self._buf.append({
            "osm_id": osm_id,
            "osm_type": osm_type,
            "tags": json.dumps(tags, ensure_ascii=False),
            "geometry_wkt": wkt,
        })
        if len(self._buf) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        df = pd.DataFrame(self._buf)
        self._con.register("_staging_osm", df)
        self._con.execute(f"""
            INSERT INTO {self._layer} (osm_id, osm_type, tags, geometry)
            SELECT osm_id, osm_type, tags,
                   CASE WHEN geometry_wkt IS NOT NULL AND geometry_wkt != ''
                        THEN TRY_CAST(ST_GeomFromText(geometry_wkt) AS GEOMETRY)
                   END
            FROM _staging_osm
        """)
        self._con.unregister("_staging_osm")
        self.total += len(self._buf)
        self._buf.clear()

    def node(self, n) -> None:
        if not n.tags:
            return
        try:
            wkt = self._wktfab.create_point(n)
            self._append(n.id, "node", {k: v for k, v in n.tags}, wkt)
        except Exception:
            pass

    def way(self, w) -> None:
        if not w.tags:
            return
        # Pour les bâtiments, on préfère area() qui crée des polygones.
        # Les ways de bâtiments seraient des LINESTRING fermés, moins utiles pour ST_Area.
        if "building" in w.tags:
            return
        try:
            wkt = self._wktfab.create_linestring(w)
            self._append(w.id, "way", {k: v for k, v in w.tags}, wkt)
        except Exception:
            pass

    def area(self, a) -> None:
        try:
            wkt = self._wktfab.create_multipolygon(a)
            self._append(a.id, "area", {k: v for k, v in a.tags}, wkt)
        except Exception:
            pass

    def finalize(self) -> int:
        self._flush()
        return self.total


def _stream_pbf_to_duckdb(
    pbf_path: Path,
    con: duckdb.DuckDBPyConnection,
    layer_name: str,
) -> int:
    """Lit un PBF filtré et écrit dans DuckDB par batches de 50k lignes.

    Utilise sparse_mem_array (RAM) pour la reconstruction des géométries way/area.
    Fonctionne bien sur des extraits régionaux (<500 MB PBF).
    Sur le PBF France entier, préférer un extrait régional préalable avec osmium extract.
    """
    handler = _BatchGeomHandler(con, layer_name)
    handler.apply_file(str(pbf_path), locations=True, idx="sparse_mem_array")
    return handler.finalize()


# ---------------------------------------------------------------------------
# Tables DuckDB
# ---------------------------------------------------------------------------

def _ensure_osm_tables(con: duckdb.DuckDBPyConnection) -> None:
    for layer_name in OSM_LAYERS:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {layer_name} (
                osm_id      BIGINT,
                osm_type    VARCHAR,
                tags        VARCHAR,
                geometry    GEOMETRY
            )
        """)
        try:
            con.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{layer_name}_geom
                ON {layer_name} USING RTREE (geometry)
            """)
        except Exception:
            # RTREE non disponible sur cette version, on continue
            pass


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def load_osm_to_duckdb(
    pbf_path: Path,
    db_path: Path = DB_PATH,
    layers: list[str] | None = None,
    force: bool = False,
) -> None:
    """Filtre le fichier OSM PBF et charge les couches utiles dans DuckDB.

    Args:
        force: si True, vide et recharge les couches déjà présentes.
               Utile pour corriger une ingestion précédente défectueuse.
    """
    if not pbf_path.exists():
        raise FileNotFoundError(f"Fichier OSM introuvable : {pbf_path}")

    osmium_cmd = _check_osmium()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    target_layers = {
        k: v for k, v in OSM_LAYERS.items()
        if layers is None or k in layers
    }

    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        _ensure_osm_tables(con)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            for layer_name, layer_config in tqdm(target_layers.items(), desc="Couches OSM"):
                count = con.execute(f"SELECT COUNT(*) FROM {layer_name}").fetchone()[0]
                if count > 0:
                    if not force:
                        logger.info(f"[osm] {layer_name} déjà chargé ({count:,} entités). Skip. Utilisez --force pour recharger.")
                        continue
                    logger.info(f"[osm] --force : purge de {layer_name} ({count:,} entités)...")
                    con.execute(f"DELETE FROM {layer_name}")

                pbf_filtered = _filter_to_pbf(
                    pbf_path, layer_name, layer_config, osmium_cmd, tmp_dir
                )
                logger.info(f"[osm] Lecture {layer_name} (batches de {BATCH_SIZE:,})...")
                n = _stream_pbf_to_duckdb(pbf_filtered, con, layer_name)
                logger.info(f"[osm] {layer_name} : {n:,} entités chargées.")
    finally:
        con.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingestion OSM → DuckDB")
    parser.add_argument("--pbf", required=True, help="Chemin du fichier .osm.pbf")
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    parser.add_argument(
        "--layers", nargs="*",
        choices=list(OSM_LAYERS.keys()),
        help="Couches à charger (toutes par défaut)",
    )
    args = parser.parse_args()

    load_osm_to_duckdb(
        pbf_path=Path(args.pbf),
        db_path=Path(args.db),
        layers=args.layers,
    )
