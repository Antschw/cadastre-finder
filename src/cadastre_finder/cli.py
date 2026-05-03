"""CLI principale de cadastre-finder.

Sous-commandes :
    cadastre-finder ingest --dept 61
    cadastre-finder build-adjacency
    cadastre-finder search --commune "Mortagne-au-Perche" --surface 4200
    cadastre-finder search-area --config constraints.yaml
"""
from __future__ import annotations

import argparse
import sys
import yaml
from pathlib import Path

from loguru import logger

from cadastre_finder.config import DB_PATH, OUTPUT_DIR


def _configure_logging(verbose: bool) -> None:
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=level, format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")


# ---------------------------------------------------------------------------
# Sous-commandes
# ---------------------------------------------------------------------------

def cmd_ingest(args: argparse.Namespace) -> int:
    from cadastre_finder.ingestion.cadastre import load_department_to_duckdb
    from cadastre_finder.config import DEPARTMENTS

    depts = args.dept if args.dept else DEPARTMENTS
    for dept in depts:
        logger.info(f"=== Ingestion département {dept} ===")
        try:
            load_department_to_duckdb(dept, db_path=Path(args.db))
        except Exception as e:
            logger.error(f"Erreur pour le département {dept} : {e}")
            if args.fail_fast:
                return 1
    return 0


def cmd_ingest_osm(args: argparse.Namespace) -> int:
    from cadastre_finder.ingestion.osm import load_osm_to_duckdb

    load_osm_to_duckdb(
        pbf_path=Path(args.pbf),
        db_path=Path(args.db),
        layers=args.layers or None,
        force=args.force,
    )
    return 0


def cmd_ingest_dpe(args: argparse.Namespace) -> int:
    from cadastre_finder.ingestion.dpe import download_dpe_data, load_dpe_to_duckdb
    
    csv_path = download_dpe_data()
    load_dpe_to_duckdb(csv_path, db_path=Path(args.db))
    return 0


def cmd_build_database(args: argparse.Namespace) -> int:
    """Construit toute la base en une commande : cadastre + OSM + DPE + adjacences."""
    from cadastre_finder.ingestion.build_all import BuildOptions, build_database
    from cadastre_finder.config import DEPARTMENTS, RAW_OSM_DIR

    depts = args.dept if args.dept else list(DEPARTMENTS)
    opts = BuildOptions(
        db_path=Path(args.db),
        osm_dir=Path(args.osm_dir) if args.osm_dir else RAW_OSM_DIR,
        departments=depts,
        skip_cadastre=args.skip_cadastre,
        skip_osm=args.skip_osm,
        skip_dpe=args.skip_dpe,
        skip_adjacency=args.skip_adjacency,
        skip_parcel_adjacency=args.skip_parcel_adjacency,
        keep_intermediate_pbf=args.keep_intermediate_pbf,
        duckdb_threads=args.threads,
        duckdb_memory_limit=args.memory_limit,
        cadastre_download_workers=args.download_workers,
        parcel_adjacency_workers=args.parcel_workers,
    )
    bilan = build_database(opts)
    # Code retour : 1 si une étape critique (cadastre) a échoué
    if bilan.get("cadastre", {}).get("status") == "error":
        return 1
    return 0


def cmd_build_adjacency(args: argparse.Namespace) -> int:
    from cadastre_finder.processing.adjacency import build_adjacency_table

    build_adjacency_table(
        db_path=Path(args.db),
        include_rank2=not args.no_rank2,
        force=args.force,
    )
    return 0


def cmd_build_parcel_adjacency(args: argparse.Namespace) -> int:
    from cadastre_finder.processing.parcel_adjacency import build_parcel_adjacency

    depts = args.dept if args.dept else None
    communes = args.communes if args.communes else None
    build_parcel_adjacency(
        db_path=Path(args.db),
        departments=depts,
        communes=communes,
        force=args.force,
        workers=args.workers,
    )
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    from cadastre_finder.search.orchestrator import search_orchestrated, search_from_text
    from cadastre_finder.search.models import NeighborMode
    from cadastre_finder.out.map import render_results

    db_path = Path(args.db)

    # Résolution du périmètre voisinage
    neighbor_mode = NeighborMode(args.neighbors)
    if getattr(args, "rank2", False) and neighbor_mode is NeighborMode.NONE:
        logger.warning("[cli] --rank2 est déprécié, utilisez --neighbors rank2.")
        neighbor_mode = NeighborMode.RANK2

    if args.text:
        logger.info("=== Recherche orchestrée depuis le texte de l'annonce ===")
        all_results = search_from_text(
            args.text, commune_hint=args.commune,
            neighbor_mode=neighbor_mode, db_path=db_path,
        )
        from cadastre_finder.search.ad_parser import parse_ad_text
        criteria = parse_ad_text(args.text)
        commune = args.commune or criteria.commune or "Inconnue"
        surface = criteria.terrain_surface or 0
    else:
        logger.info("=== Recherche orchestrée depuis paramètres manuels ===")
        commune = args.commune
        surface = args.surface
        all_results = search_orchestrated(
            commune=commune,
            surface_m2=surface,
            living_surface=getattr(args, "living_surface", None),
            dpe_label=getattr(args, "dpe", None),
            ges_label=getattr(args, "ges", None),
            postal_code=getattr(args, "postal", None),
            tolerance_pct=args.tolerance,
            neighbor_mode=neighbor_mode,
            db_path=db_path,
        )

    if not all_results:
        logger.warning(
            "Aucun résultat. Utilisez 'search-area' pour une "
            "recherche par contraintes géométriques."
        )
        return 0

    from cadastre_finder.search.models import ParcelMatch, ComboMatch
    matches = [r for r in all_results if isinstance(r, ParcelMatch)]
    combos = [r for r in all_results if isinstance(r, ComboMatch)]

    output_path = OUTPUT_DIR / f"result_{commune.replace(' ', '_')}.html"
    render_results(
        matches,
        output_path=output_path,
        combos=combos,
        query_info={"commune": commune, "surface_m2": surface, "titre": "Résultats cadastre (Orchestrés)"},
        auto_open=not args.no_open,
    )
    return 0


def cmd_ui(args: argparse.Namespace) -> int:
    import subprocess
    import sys
    from pathlib import Path

    app_path = Path(__file__).parent / "ui" / "app.py"
    port = str(args.port)
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", port]
    logger.info(f"[ui] Démarrage de l'interface sur http://localhost:{port}")
    result = subprocess.run(cmd)
    return result.returncode


def cmd_search_area(args: argparse.Namespace) -> int:
    from cadastre_finder.search.proximity_match import (
        search_by_proximity, NearPOI, AwayFromFeature, InCommuneOrNeighbors
    )
    from cadastre_finder.out.map import render_results

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Fichier de config introuvable : {config_path}")
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    constraints = []
    for c in cfg.get("constraints", []):
        ctype = c.get("type")
        if ctype == "near_poi":
            constraints.append(NearPOI(
                category=c["category"],
                max_distance_m=c["max_distance_m"],
                name=c.get("name"),
                commune=c.get("commune"),
            ))
        elif ctype == "away_from":
            constraints.append(AwayFromFeature(
                category=c["category"],
                min_distance_m=c["min_distance_m"],
            ))
        elif ctype == "in_commune":
            constraints.append(InCommuneOrNeighbors(
                commune=c["commune"],
                rank=c.get("rank", 1),
                postal_code=c.get("postal_code"),
            ))
        else:
            logger.warning(f"Type de contrainte inconnu : {ctype}")

    min_surface = cfg.get("min_surface_m2", 2500)
    matches = search_by_proximity(
        constraints, min_surface=min_surface, db_path=Path(args.db)
    )

    output_path = OUTPUT_DIR / "result_search_area.html"
    render_results(
        matches,
        output_path=output_path,
        query_info={"titre": "Recherche par zone"},
        auto_open=not args.no_open,
    )
    return 0


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cadastre-finder",
        description="Localisateur de biens immobiliers par parcelle cadastrale",
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Chemin DuckDB")
    parser.add_argument("-v", "--verbose", action="store_true", help="Logs détaillés")
    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    p_ingest = sub.add_parser("ingest", help="Ingestion cadastre par département")
    p_ingest.add_argument(
        "--dept", nargs="+", metavar="DEPT",
        help="Code(s) département (ex: 61 72). Tous les 20 si omis."
    )
    p_ingest.add_argument("--fail-fast", action="store_true", help="Arrêter à la première erreur")
    p_ingest.set_defaults(func=cmd_ingest)

    # ingest-osm
    p_osm = sub.add_parser("ingest-osm", help="Ingestion OSM (POI, routes, rivières)")
    p_osm.add_argument("--pbf", required=True, help="Chemin du fichier .osm.pbf")
    p_osm.add_argument("--layers", nargs="*", help="Couches à charger (toutes par défaut)")
    p_osm.add_argument("--force", action="store_true", help="Vider et recharger les couches déjà présentes")
    p_osm.set_defaults(func=cmd_ingest_osm)

    # ingest-dpe
    p_dpe = sub.add_parser("ingest-dpe", help="Ingestion DPE ADEME (existants)")
    p_dpe.set_defaults(func=cmd_ingest_dpe)

    # build-database (orchestrateur unique)
    p_build = sub.add_parser(
        "build-database",
        help="Construit toute la base en une commande (cadastre + OSM + DPE + adjacences)",
        description=(
            "Construction autonome et idempotente de l'intégralité de la base : "
            "vérification des PBF Geofabrik, ingestion cadastre Etalab des 21 "
            "départements, OSM (POI/routes/bâtiments), DPE ADEME, et "
            "pré-calculs d'adjacence (communes + parcelles, parallélisé)."
        ),
    )
    p_build.add_argument(
        "--dept", nargs="+", metavar="DEPT",
        help="Sous-ensemble de départements (par défaut : les 21 du périmètre)",
    )
    p_build.add_argument("--osm-dir", help="Répertoire des PBF OSM (défaut : data/raw/osm)")
    p_build.add_argument("--skip-cadastre", action="store_true",
                         help="Ne pas (ré)ingérer les parcelles cadastre")
    p_build.add_argument("--skip-osm", action="store_true",
                         help="Ne pas (ré)ingérer les couches OSM")
    p_build.add_argument("--skip-dpe", action="store_true",
                         help="Ne pas (ré)ingérer les données DPE")
    p_build.add_argument("--skip-adjacency", action="store_true",
                         help="Ne pas calculer l'adjacence des communes")
    p_build.add_argument("--skip-parcel-adjacency", action="store_true",
                         help="Ne pas calculer l'adjacence des parcelles (étape la plus longue)")
    p_build.add_argument("--keep-intermediate-pbf", action="store_true", default=True,
                         help="Conserver le PBF merged après extraction (défaut : True)")
    p_build.add_argument("--threads", type=int, default=None,
                         help="Threads DuckDB (défaut : nombre de cœurs logiques)")
    p_build.add_argument("--memory-limit", default="24GB",
                         help="Limite mémoire DuckDB (défaut : 24GB)")
    p_build.add_argument("--download-workers", type=int, default=8,
                         help="Téléchargements cadastre concurrents (défaut : 8)")
    p_build.add_argument("--parcel-workers", type=int, default=None,
                         help="Workers pour adjacence parcellaire (défaut : cpu-1)")
    p_build.set_defaults(func=cmd_build_database)

    # build-adjacency
    p_adj = sub.add_parser("build-adjacency", help="Calcul table d'adjacence communes")
    p_adj.add_argument("--no-rank2", action="store_true", help="Ne pas calculer le rang 2")
    p_adj.add_argument("--force", action="store_true", help="Supprimer et reconstruire même si déjà présent")
    p_adj.set_defaults(func=cmd_build_adjacency)

    # build-parcel-adjacency
    p_padj = sub.add_parser("build-parcel-adjacency", help="Pré-calcul table d'adjacence parcellaire (accélère search --combos)")
    p_padj.add_argument("--dept", nargs="+", metavar="DEPT", help="Limiter à ces départements")
    p_padj.add_argument("--communes", nargs="+", metavar="INSEE", help="Reconstruire uniquement ces codes INSEE (ex: 28103)")
    p_padj.add_argument("--force", action="store_true", help="Effacer et recalculer les communes déjà traitées")
    p_padj.add_argument("--workers", type=int, default=None, help="Nb processus parallèles (défaut : cpu-1)")
    p_padj.set_defaults(func=cmd_build_parcel_adjacency)

    # search
    p_search = sub.add_parser("search", help="Recherche orchestrée (DPE-led ou combo)")
    p_search.add_argument("--commune", help="Nom de la commune annoncée")
    p_search.add_argument("--surface", type=float, help="Surface terrain en m²")
    p_search.add_argument("--living-surface", type=float, dest="living_surface",
                          help="Surface habitable en m² (utilisée par la recherche DPE)")
    p_search.add_argument("--dpe", choices=["A", "B", "C", "D", "E", "F", "G"],
                          help="Étiquette DPE (déclenche la recherche pilotée par l'ADEME)")
    p_search.add_argument("--ges", choices=["A", "B", "C", "D", "E", "F", "G"],
                          help="Étiquette GES (déclenche la recherche pilotée par l'ADEME)")
    p_search.add_argument("--text", help="Texte de l'annonce immobilière (extraction auto)")
    p_search.add_argument("--postal", help="Code postal (optionnel, pour désambiguïser)")
    p_search.add_argument("--tolerance", type=float, default=5.0, help="Tolérance surface en pourcent")
    p_search.add_argument(
        "--neighbors", choices=["none", "rank1", "rank2"], default="none",
        help="Étendre aux communes voisines (rang 1 ou 2). Défaut : commune principale uniquement.",
    )
    p_search.add_argument("--rank2", action="store_true",
                          help="(Déprécié) alias de --neighbors rank2")
    p_search.add_argument("--no-combo", action="store_true", help="Désactiver la recherche de combos")
    p_search.add_argument("--max-parts", type=int, default=6, choices=[2, 3, 4, 5, 6], help="Taille max des combos (défaut : 6)")
    p_search.add_argument("--include-agricultural", action="store_true", help="Inclure les parcelles sans bâtiment (agricoles)")
    p_search.add_argument("--no-open", action="store_true", help="Ne pas ouvrir le navigateur")
    p_search.set_defaults(func=cmd_search)

    # ui
    p_ui = sub.add_parser("ui", help="Lancer l'interface graphique Streamlit")
    p_ui.add_argument("--port", type=int, default=8501, help="Port HTTP (défaut : 8501)")
    p_ui.set_defaults(func=cmd_ui)

    # search-area
    p_area = sub.add_parser("search-area", help="Recherche par contraintes géométriques (étape 3)")
    p_area.add_argument("--config", required=True, help="Fichier YAML de contraintes")
    p_area.add_argument("--no-open", action="store_true", help="Ne pas ouvrir le navigateur")
    p_area.set_defaults(func=cmd_search_area)

    args = parser.parse_args()
    _configure_logging(args.verbose)

    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
