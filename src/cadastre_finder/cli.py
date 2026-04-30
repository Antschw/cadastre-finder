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


def cmd_build_adjacency(args: argparse.Namespace) -> int:
    from cadastre_finder.processing.adjacency import build_adjacency_table

    build_adjacency_table(
        db_path=Path(args.db),
        include_rank2=not args.no_rank2,
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
    )
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    from cadastre_finder.search.strict_match import search_strict
    from cadastre_finder.search.neighbor_match import search_with_neighbors
    from cadastre_finder.search.combo_match import search_combos
    from cadastre_finder.out.map import render_results

    db_path = Path(args.db)
    commune = args.commune
    surface = args.surface
    postal = getattr(args, "postal", None)

    built_only = not args.include_agricultural

    # Étape 1 : match strict parcelles uniques
    logger.info("=== Étape 1 : match strict ===")
    matches = search_strict(commune, surface, postal_code=postal, built_only=built_only, db_path=db_path)

    if 1 <= len(matches) <= 3:
        logger.info(f"Succès étape 1 : {len(matches)} parcelle(s) trouvée(s).")
    else:
        if not matches:
            logger.info("Étape 1 vide → passage à l'étape 2.")
        else:
            logger.info(f"Étape 1 : {len(matches)} résultats (> 3). Élargissement étape 2.")

        logger.info("=== Étape 2 : communes voisines ===")
        matches = search_with_neighbors(
            commune, surface,
            postal_code=postal,
            tolerance_pct=args.tolerance,
            include_rank2=args.rank2,
            built_only=built_only,
            db_path=db_path,
        )

    # Étape combo : combinaisons de parcelles adjacentes (toujours)
    combos = []
    if not args.no_combo:
        logger.info("=== Combos : parcelles adjacentes ===")
        combos = search_combos(
            commune, surface,
            postal_code=postal,
            tolerance_pct=args.tolerance,
            include_rank2=args.rank2,
            max_parts=args.max_parts,
            built_only=built_only,
            db_path=db_path,
        )

    if not matches and not combos:
        logger.warning(
            "Aucun résultat. Utilisez 'search-area' (étape 3) pour une "
            "recherche par contraintes géométriques."
        )
        return 0

    output_path = OUTPUT_DIR / f"result_{commune.replace(' ', '_')}.html"
    render_results(
        matches,
        output_path=output_path,
        combos=combos,
        query_info={"commune": commune, "surface_m2": surface, "titre": "Résultats cadastre"},
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

    # build-adjacency
    p_adj = sub.add_parser("build-adjacency", help="Calcul table d'adjacence communes")
    p_adj.add_argument("--no-rank2", action="store_true", help="Ne pas calculer le rang 2")
    p_adj.set_defaults(func=cmd_build_adjacency)

    # build-parcel-adjacency
    p_padj = sub.add_parser("build-parcel-adjacency", help="Pré-calcul table d'adjacence parcellaire (accélère search --combos)")
    p_padj.add_argument("--dept", nargs="+", metavar="DEPT", help="Limiter à ces départements")
    p_padj.add_argument("--communes", nargs="+", metavar="INSEE", help="Reconstruire uniquement ces codes INSEE (ex: 28103)")
    p_padj.add_argument("--force", action="store_true", help="Effacer et recalculer les communes déjà traitées")
    p_padj.set_defaults(func=cmd_build_parcel_adjacency)

    # search
    p_search = sub.add_parser("search", help="Recherche par commune + surface (étapes 1 et 2)")
    p_search.add_argument("--commune", required=True, help="Nom de la commune annoncée")
    p_search.add_argument("--surface", required=True, type=float, help="Surface en m²")
    p_search.add_argument("--postal", help="Code postal (optionnel, pour désambiguïser)")
    p_search.add_argument("--tolerance", type=float, default=5.0, help="Tolérance surface en pourcent")
    p_search.add_argument("--rank2", action="store_true", help="Inclure voisines rang 2")
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
