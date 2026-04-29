"""Rendu cartographique Folium des résultats de recherche."""
from __future__ import annotations

import json
import math
import webbrowser
from pathlib import Path

import folium
from loguru import logger

from cadastre_finder.search.models import ComboMatch, ParcelMatch

IGN_WMTS_URL = (
    "https://data.geopf.fr/wmts?"
    "SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=ORTHOIMAGERY.ORTHOPHOTOS"
    "&STYLE=normal&TILEMATRIXSET=PM"
    "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}"
    "&FORMAT=image/jpeg"
)
IGN_PREVIEW_URL = (
    "https://data.geopf.fr/wmts?"
    "SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=ORTHOIMAGERY.ORTHOPHOTOS"
    "&STYLE=normal&TILEMATRIXSET=PM"
    "&TILEMATRIX=18&TILEROW={row}&TILECOL={col}"
    "&FORMAT=image/jpeg"
)


def _score_to_color(score: float) -> str:
    if score >= 100:
        return "#1a9641"
    elif score >= 80:
        return "#a6d96a"
    elif score >= 60:
        return "#ffffbf"
    elif score >= 40:
        return "#fdae61"
    else:
        return "#d7191c"


def _lat_lon_to_tile(lat: float, lon: float, zoom: int = 18) -> tuple[int, int]:
    n = 2 ** zoom
    col = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    row = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return row, col


def _rang_label(rank: int) -> str:
    return {0: "Commune annoncée", 1: "Voisine rang 1", 2: "Voisine rang 2"}.get(rank, f"Rang {rank}")


def _popup_single(match: ParcelMatch) -> str:
    row, col = _lat_lon_to_tile(match.centroid_lat, match.centroid_lon)
    img_url = IGN_PREVIEW_URL.format(row=row, col=col)
    color = _score_to_color(match.score)
    return f"""
    <div style="font-family:sans-serif;font-size:13px;min-width:220px;">
        <b style="font-size:14px;">{match.id_parcelle}</b><br>
        <span style="color:#555;">{match.nom_commune} ({match.code_insee})</span><br>
        <hr style="margin:4px 0;">
        Surface : <b>{match.contenance:,} m²</b><br>
        Score : <b style="color:{color};">{match.score:.1f}</b><br>
        Position : {_rang_label(match.rank)}<br>
        <hr style="margin:4px 0;">
        <a href="{match.street_view_url}" target="_blank">Street View</a> &nbsp;
        <a href="{match.geoportail_url}" target="_blank">Géoportail</a><br>
        <hr style="margin:4px 0;">
        <img src="{img_url}" width="200" height="200"
             style="display:block;margin-top:4px;"
             alt="Ortho IGN" onerror="this.style.display='none'">
    </div>"""


def _popup_combo(combo: ComboMatch) -> str:
    row, col = _lat_lon_to_tile(combo.centroid_lat, combo.centroid_lon)
    img_url = IGN_PREVIEW_URL.format(row=row, col=col)
    color = _score_to_color(combo.score)

    parts_html = "".join(
        f"<li>{p.id_parcelle} — {p.contenance:,} m²</li>"
        for p in combo.parts
    )
    # Couleur de l'indicateur de compacité
    pp = combo.compactness
    if pp >= 0.5:
        pp_color, pp_label = "#1a9641", "bonne"
    elif pp >= 0.2:
        pp_color, pp_label = "#fdae61", "moyenne"
    else:
        pp_color, pp_label = "#d7191c", "faible"

    return f"""
    <div style="font-family:sans-serif;font-size:13px;min-width:240px;">
        <b style="font-size:14px;">Combinaison {combo.nb_parcelles} parcelles</b><br>
        <span style="color:#555;">{combo.nom_commune} ({combo.code_insee})</span><br>
        <hr style="margin:4px 0;">
        Surface totale : <b>{combo.total_contenance:,} m²</b><br>
        Score : <b style="color:{color};">{combo.score:.1f}</b><br>
        Compacité : <b style="color:{pp_color};">{pp:.2f}</b> ({pp_label})<br>
        Position : {_rang_label(combo.rank)}<br>
        <hr style="margin:4px 0;">
        <b>Parcelles :</b>
        <ul style="margin:4px 0 4px 16px;padding:0;">{parts_html}</ul>
        <a href="{combo.geoportail_url}" target="_blank">Géoportail</a><br>
        <hr style="margin:4px 0;">
        <img src="{img_url}" width="200" height="200"
             style="display:block;margin-top:4px;"
             alt="Ortho IGN" onerror="this.style.display='none'">
    </div>"""


def _add_single(fg: folium.FeatureGroup, match: ParcelMatch) -> None:
    color = _score_to_color(match.score)
    popup_html = _popup_single(match)
    try:
        geom = json.loads(match.geometry_geojson)
        folium.GeoJson(
            geom,
            style_function=lambda _, c=color: {
                "fillColor": c, "color": "#333333", "weight": 2, "fillOpacity": 0.5,
            },
            tooltip=f"{match.id_parcelle} — {match.contenance:,} m²",
            popup=folium.Popup(popup_html, max_width=280),
        ).add_to(fg)
    except Exception:
        folium.Marker(
            location=[match.centroid_lat, match.centroid_lon],
            popup=folium.Popup(popup_html, max_width=280),
            icon=folium.Icon(color="blue", icon="home"),
        ).add_to(fg)


def _add_combo(fg: folium.FeatureGroup, combo: ComboMatch) -> None:
    popup_html = _popup_combo(combo)
    ids_short = " + ".join(p.id_parcelle[-4:] for p in combo.parts)
    try:
        geom = json.loads(combo.combined_geojson)
        folium.GeoJson(
            geom,
            style_function=lambda _: {
                "fillColor": "#8B008B",   # violet = combo
                "color": "#4B0082",
                "weight": 3,
                "fillOpacity": 0.45,
                "dashArray": "6 3",
            },
            tooltip=f"Combo {combo.nb_parcelles}p [{ids_short}] — {combo.total_contenance:,} m²",
            popup=folium.Popup(popup_html, max_width=300),
        ).add_to(fg)
    except Exception:
        folium.Marker(
            location=[combo.centroid_lat, combo.centroid_lon],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color="purple", icon="object-group", prefix="fa"),
        ).add_to(fg)


def render_results(
    matches: list[ParcelMatch],
    output_path: Path,
    combos: list[ComboMatch] | None = None,
    query_info: dict | None = None,
    auto_open: bool = True,
) -> Path:
    """Génère une carte HTML Folium avec parcelles individuelles et combinaisons."""
    combos = combos or []
    all_empty = not matches and not combos
    if all_empty:
        logger.warning("[map] Aucun résultat à afficher.")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    lats = [m.centroid_lat for m in matches] + [c.centroid_lat for c in combos]
    lons = [m.centroid_lon for m in matches] + [c.centroid_lon for c in combos]
    center_lat = (min(lats) + max(lats)) / 2
    center_lon = (min(lons) + max(lons)) / 2

    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="OpenStreetMap")
    folium.TileLayer(
        tiles=IGN_WMTS_URL, attr="© IGN Géoplateforme",
        name="Ortho IGN", overlay=False, control=True, max_zoom=19,
    ).add_to(fmap)

    if matches:
        fg_single = folium.FeatureGroup(name=f"Parcelles uniques ({len(matches)})")
        for match in matches:
            _add_single(fg_single, match)
        fg_single.add_to(fmap)

    if combos:
        fg_combo = folium.FeatureGroup(name=f"Combinaisons ({len(combos)})")
        for combo in combos:
            _add_combo(fg_combo, combo)
        fg_combo.add_to(fmap)

    # Légende
    nb_single = len(matches)
    nb_combo = len(combos)
    total = nb_single + nb_combo
    if query_info:
        titre = query_info.get("titre", "Résultats cadastre")
        commune = query_info.get("commune", "")
        surface = query_info.get("surface_m2", "")
        title_html = f"""
        <div style="position:fixed;top:10px;left:50%;transform:translateX(-50%);
                    z-index:1000;background:white;padding:8px 16px;
                    border-radius:6px;box-shadow:2px 2px 6px rgba(0,0,0,0.3);
                    font-family:sans-serif;font-size:13px;">
            <b>{titre}</b>
            {"— " + commune if commune else ""}
            {"— " + str(surface) + " m²" if surface else ""}
            &nbsp;|&nbsp; {total} résultat(s)
            {f"&nbsp;({nb_combo} combo)" if nb_combo else ""}
        </div>"""
        fmap.get_root().html.add_child(folium.Element(title_html))

    # Légende couleurs combos
    if combos:
        legend_html = """
        <div style="position:fixed;bottom:30px;right:10px;z-index:1000;
                    background:white;padding:8px 12px;border-radius:6px;
                    box-shadow:2px 2px 6px rgba(0,0,0,0.3);font-family:sans-serif;font-size:12px;">
            <div><span style="display:inline-block;width:14px;height:14px;
                 background:#1a9641;margin-right:6px;vertical-align:middle;"></span>Parcelle unique exacte</div>
            <div><span style="display:inline-block;width:14px;height:14px;
                 background:#a6d96a;margin-right:6px;vertical-align:middle;"></span>Parcelle unique proche</div>
            <div><span style="display:inline-block;width:14px;height:14px;
                 background:#8B008B;border:2px dashed #4B0082;margin-right:6px;
                 vertical-align:middle;"></span>Combinaison de parcelles</div>
        </div>"""
        fmap.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(fmap)
    fmap.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]])

    fmap.save(str(output_path))
    logger.info(f"[map] Carte sauvegardée → {output_path}")

    if auto_open:
        webbrowser.open(output_path.as_uri())

    return output_path
