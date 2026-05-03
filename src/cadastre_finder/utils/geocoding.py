"""Géocodage via la Géoplateforme IGN.

L'API `api-adresse.data.gouv.fr` (BAN) a été décommissionnée fin janvier 2026 ;
toutes les requêtes passent désormais par `data.geopf.fr/geocodage`.

Stratégies :
1. Résolution commune → INSEE : recherche locale dans la table `communes`,
   fallback sur `/search?index=poi&type=municipality`.
2. Géocodage adresse complète → (lat, lon) via `/search?index=address`.
3. Reverse parcelle → id cadastral via `/reverse?index=parcel`.

Cache JSON local pour les résolutions de commune.
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb
import httpx
from loguru import logger

from cadastre_finder.config import GEOPF_API_URL, DATA_PROCESSED, DB_PATH

CACHE_PATH = DATA_PROCESSED / "geocoding_cache.json"


@dataclass
class CommuneInfo:
    code_insee: str
    nom: str
    code_dept: str
    score: float = 1.0


@dataclass
class ResolveResult:
    """Résultat de la résolution d'une commune."""
    candidates: list[CommuneInfo] = field(default_factory=list)

    @property
    def unique(self) -> CommuneInfo | None:
        """Retourne le candidat unique si non ambigu, None sinon."""
        return self.candidates[0] if len(self.candidates) == 1 else None

    @property
    def best(self) -> CommuneInfo | None:
        """Retourne le meilleur candidat (score le plus élevé)."""
        return self.candidates[0] if self.candidates else None


def _normalize(text: str) -> str:
    """Normalise une chaîne pour la comparaison (minuscules, sans accents)."""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _search_local(
    name: str,
    postal_code: Optional[str],
    db_path: Path,
) -> list[CommuneInfo]:
    """Recherche dans la table communes locale."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return []

    try:
        name_norm = _normalize(name)
        rows = con.execute(
            "SELECT code_insee, nom, code_dept FROM communes"
        ).fetchall()

        results = []
        for code_insee, nom, code_dept in rows:
            if nom is None:
                continue
            if _normalize(nom) == name_norm:
                # Filtrage par code postal si fourni (les 2 premiers chiffres = dept)
                if postal_code and not postal_code.startswith(code_dept):
                    continue
                results.append(CommuneInfo(
                    code_insee=code_insee, nom=nom, code_dept=code_dept, score=1.0
                ))

        return results
    finally:
        con.close()


def _search_api(
    name: str,
    postal_code: Optional[str],
) -> list[CommuneInfo]:
    """Interroge l'API Géoplateforme pour résoudre une commune."""
    cache = _load_cache()
    cache_key = f"{_normalize(name)}|{postal_code or ''}"

    if cache_key in cache:
        logger.debug(f"[geocoding] Cache hit pour '{name}'")
        return [CommuneInfo(**c) for c in cache[cache_key]]

    params: dict[str, str] = {
        "q": name,
        "index": "poi",
        "type": "municipality",
        "limit": "10",
    }
    if postal_code:
        params["postcode"] = postal_code

    try:
        resp = httpx.get(
            f"{GEOPF_API_URL}/search",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[geocoding] Géoplateforme indisponible pour '{name}' : {e}")
        return []

    results = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        code_insee = props.get("citycode", "")
        if isinstance(code_insee, list) and code_insee:
            code_insee = code_insee[0]

        nom = props.get("city", "")
        if isinstance(nom, list) and nom:
            nom = nom[0]

        score = props.get("score", 0.0)
        code_dept = code_insee[:2] if len(code_insee) >= 2 else ""

        if code_insee:
            results.append(CommuneInfo(
                code_insee=code_insee, nom=nom, code_dept=code_dept, score=score
            ))

    # Mise en cache
    cache[cache_key] = [
        {"code_insee": c.code_insee, "nom": c.nom, "code_dept": c.code_dept, "score": c.score}
        for c in results
    ]
    _save_cache(cache)

    return results


def geocode_address(
    address: str,
    city: Optional[str] = None,
    postcode: Optional[str] = None,
    citycode: Optional[str] = None,
) -> Optional[tuple[float, float]]:
    """Géo-code une adresse complète via la Géoplateforme. Retourne (lat, lon) ou None."""
    params = {
        "q": address,
        "index": "address",
        "limit": 1,
    }
    if citycode:
        params["citycode"] = citycode
    elif city:
        params["city"] = city
    if postcode:
        params["postcode"] = postcode

    try:
        resp = httpx.get(f"{GEOPF_API_URL}/search", params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("features"):
            coords = data["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0]  # lat, lon
    except Exception as e:
        logger.debug(f"[geocoding] Erreur géocodage adresse '{address}' : {e}")

    return None


def reverse_geocode_parcel(lat: float, lon: float) -> Optional[str]:
    """Trouve l'ID de la parcelle cadastrale aux coordonnées données via l'API Géoplateforme."""
    params = {
        "lat": lat,
        "lon": lon,
        "index": "parcel",
        "limit": 1,
    }
    try:
        resp = httpx.get(f"{GEOPF_API_URL}/reverse", params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("features"):
            return data["features"][0]["properties"].get("id")
    except Exception as e:
        logger.debug(f"[geocoding] Erreur reverse geocoding parcelle ({lat}, {lon}) : {e}")
    return None


def resolve_commune(
    name: str,
    postal_code: Optional[str] = None,
    db_path: Path = DB_PATH,
) -> ResolveResult:
    """Résout un nom de commune en code(s) INSEE.

    1. Recherche locale dans la table communes DuckDB
    2. Sinon, fallback sur la Géoplateforme
    3. Retourne un ResolveResult avec la liste des candidats triés par score

    Exemples :
        >>> r = resolve_commune("Mortagne-au-Perche")
        >>> r.unique.code_insee
        '61293'
    """
    local = _search_local(name, postal_code, db_path)
    if local:
        logger.debug(f"[geocoding] '{name}' → {len(local)} résultat(s) local/locaux")
        return ResolveResult(candidates=local)

    logger.info(f"[geocoding] '{name}' non trouvé localement, interrogation Géoplateforme...")
    api_results = _search_api(name, postal_code)

    if not api_results:
        logger.warning(f"[geocoding] Aucun résultat pour '{name}'")
        return ResolveResult()

    return ResolveResult(candidates=api_results)
