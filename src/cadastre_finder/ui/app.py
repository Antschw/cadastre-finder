"""Interface Streamlit de cadastre-finder.

Lancement : cadastre-finder ui
         ou : streamlit run src/cadastre_finder/ui/app.py
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Union

import folium
import streamlit as st
import streamlit.components.v1 as components

from cadastre_finder.config import DB_PATH, DEFAULT_TOLERANCE_PCT, DEFAULT_TOP_N
from cadastre_finder.search.models import ComboMatch, ParcelMatch

# ---------------------------------------------------------------------------
# Config page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Cadastre Finder",
    page_icon="🏡",
    layout="wide",
    initial_sidebar_state="expanded",
)

_CSS = """
<style>
.result-card {
    background: #f8f9fa;
    border: 1px solid #dee2e6;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 8px;
}
.score-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-weight: bold;
    font-size: 1rem;
    color: white;
}
.nav-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 12px;
}
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Helpers visuels
# ---------------------------------------------------------------------------

def _score_color(score: float) -> str:
    if score >= 100:
        return "#1a9641"
    elif score >= 80:
        return "#a6d96a"
    elif score >= 60:
        return "#e8c200"
    elif score >= 40:
        return "#fdae61"
    else:
        return "#d7191c"


def _rang_label(rank: int) -> str:
    return {0: "Commune annoncée", 1: "Voisine rang 1", 2: "Voisine rang 2"}.get(rank, f"Rang {rank}")


def _make_mini_map(result: Union[ParcelMatch, ComboMatch], height: int = 420) -> str:
    """Génère une carte Folium pour un seul résultat et renvoie le HTML."""
    if isinstance(result, ComboMatch):
        lat, lon = result.centroid_lat, result.centroid_lon
    else:
        lat, lon = result.centroid_lat, result.centroid_lon

    fmap = folium.Map(location=[lat, lon], zoom_start=17, tiles="OpenStreetMap")
    folium.TileLayer(
        tiles=(
            "https://data.geopf.fr/wmts?"
            "SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
            "&LAYER=ORTHOIMAGERY.ORTHOPHOTOS"
            "&STYLE=normal&TILEMATRIXSET=PM"
            "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}"
            "&FORMAT=image/jpeg"
        ),
        attr="© IGN Géoplateforme",
        name="Ortho IGN",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(fmap)

    if isinstance(result, ComboMatch):
        color, border = "#8B008B", "#4B0082"
        try:
            geom = json.loads(result.combined_geojson)
            folium.GeoJson(
                geom,
                style_function=lambda _: {
                    "fillColor": color,
                    "color": border,
                    "weight": 3,
                    "fillOpacity": 0.5,
                    "dashArray": "6 3",
                },
            ).add_to(fmap)
        except Exception:
            folium.Marker(location=[lat, lon], icon=folium.Icon(color="purple")).add_to(fmap)
    else:
        color = _score_color(result.score)
        try:
            geom = json.loads(result.geometry_geojson)
            folium.GeoJson(
                geom,
                style_function=lambda _, c=color: {
                    "fillColor": c, "color": "#333", "weight": 2, "fillOpacity": 0.55,
                },
            ).add_to(fmap)
        except Exception:
            folium.Marker(location=[lat, lon], icon=folium.Icon(color="blue")).add_to(fmap)

    folium.LayerControl(collapsed=True).add_to(fmap)
    return fmap._repr_html_()


# ---------------------------------------------------------------------------
# Affichage d'un résultat
# ---------------------------------------------------------------------------

def _display_result(result: Union[ParcelMatch, ComboMatch], idx: int, total: int) -> None:
    score = result.score
    badge_color = _score_color(score)

    col_nav1, col_nav2, col_nav3 = st.columns([1, 6, 1])
    with col_nav1:
        if st.button("◀ Précédent", disabled=(idx == 0), key="prev"):
            st.session_state.result_idx = max(0, idx - 1)
            st.rerun()
    with col_nav2:
        st.markdown(
            f"<div style='text-align:center;padding-top:6px;font-size:0.95rem;color:#555;'>"
            f"Résultat <b>{idx + 1}</b> / {total}</div>",
            unsafe_allow_html=True,
        )
    with col_nav3:
        if st.button("Suivant ▶", disabled=(idx == total - 1), key="next"):
            st.session_state.result_idx = min(total - 1, idx + 1)
            st.rerun()

    st.markdown(
        f"<span class='score-badge' style='background:{badge_color};'>Score : {score:.1f}</span> &nbsp;"
        f"<span style='color:#555;font-size:0.9rem;'>{_rang_label(result.rank)}</span>",
        unsafe_allow_html=True,
    )
    st.write("")

    col_info, col_map = st.columns([1, 2])

    with col_info:
        if isinstance(result, ComboMatch):
            st.markdown(f"### 🔗 Combinaison de {result.nb_parcelles} parcelles")
            st.markdown(f"**Commune :** {result.nom_commune} `{result.code_insee}`")
            st.markdown(f"**Surface totale :** {result.total_contenance:,} m²")

            pp = result.compactness
            if pp >= 0.5:
                pp_label, pp_color = "bonne", "green"
            elif pp >= 0.2:
                pp_label, pp_color = "moyenne", "orange"
            else:
                pp_label, pp_color = "faible", "red"
            st.markdown(
                f"**Compacité :** :{pp_color}[{pp:.2f}] ({pp_label})"
            )

            st.markdown("**Parcelles :**")
            for p in result.parts:
                st.markdown(f"- `{p.id_parcelle}` — {p.contenance:,} m²")

            st.markdown(f"[Ouvrir sur Géoportail]({result.geoportail_url})")

        else:
            st.markdown(f"### 🏡 Parcelle `{result.id_parcelle}`")
            st.markdown(f"**Commune :** {result.nom_commune} `{result.code_insee}`")
            st.markdown(f"**Surface :** {result.contenance:,} m²")
            st.markdown(
                f"[Street View]({result.street_view_url}) &nbsp;|&nbsp; "
                f"[Géoportail]({result.geoportail_url})"
            )

    with col_map:
        html = _make_mini_map(result)
        components.html(html, height=420, scrolling=False)


# ---------------------------------------------------------------------------
# Sidebar : formulaire de recherche
# ---------------------------------------------------------------------------

def _sidebar() -> dict | None:
    st.sidebar.title("🔍 Recherche cadastrale")

    commune = st.sidebar.text_input("Commune annoncée *", placeholder="ex : Neuvy-le-Roi")
    surface = st.sidebar.number_input("Surface en m² *", min_value=100, max_value=100_000, value=5000, step=50)
    postal = st.sidebar.text_input("Code postal (optionnel)", placeholder="ex : 37370")

    st.sidebar.markdown("---")
    tolerance = st.sidebar.slider("Tolérance surface (%)", min_value=1, max_value=30, value=5)
    max_parts = st.sidebar.slider("Taille max des combos", min_value=2, max_value=6, value=6)
    rank2 = st.sidebar.checkbox("Inclure communes voisines rang 2", value=False)
    no_combo = st.sidebar.checkbox("Désactiver la recherche de combos", value=False)
    include_agri = st.sidebar.checkbox("Inclure parcelles sans bâtiment", value=False)

    st.sidebar.markdown("---")
    launched = st.sidebar.button("🚀 Lancer la recherche", type="primary", use_container_width=True)

    if not launched:
        return None
    if not commune or not commune.strip():
        st.sidebar.error("La commune est obligatoire.")
        return None

    return {
        "commune": commune.strip(),
        "surface": float(surface),
        "postal": postal.strip() or None,
        "tolerance": float(tolerance),
        "max_parts": int(max_parts),
        "rank2": rank2,
        "no_combo": no_combo,
        "include_agri": include_agri,
    }


# ---------------------------------------------------------------------------
# Barre de résultats (synthèse)
# ---------------------------------------------------------------------------

def _results_overview(
    all_results: list[Union[ParcelMatch, ComboMatch]],
    current_idx: int,
) -> None:
    st.markdown(f"**{len(all_results)} résultat(s) trouvé(s)**, triés par score décroissant.")
    cols = st.columns(min(len(all_results), 10))
    for i, (col, r) in enumerate(zip(cols, all_results[:10])):
        with col:
            color = _score_color(r.score)
            label = "C" if isinstance(r, ComboMatch) else "P"
            style = "border: 3px solid #1976D2;" if i == current_idx else ""
            st.markdown(
                f"<div style='text-align:center;background:{color};color:white;"
                f"border-radius:6px;padding:4px;cursor:pointer;{style}'>"
                f"<b>{label}</b><br>{r.score:.0f}</div>",
                unsafe_allow_html=True,
            )


# ---------------------------------------------------------------------------
# Logique de recherche
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner="Recherche en cours…", ttl=300)
def _run_search(
    commune: str,
    surface: float,
    postal: str | None,
    tolerance: float,
    max_parts: int,
    rank2: bool,
    no_combo: bool,
    include_agri: bool,
    db_path_str: str,
) -> tuple[list[ParcelMatch], list[ComboMatch]]:
    from cadastre_finder.search.strict_match import search_strict
    from cadastre_finder.search.neighbor_match import search_with_neighbors
    from cadastre_finder.search.combo_match import search_combos

    db_path = Path(db_path_str)
    built_only = not include_agri

    matches = search_strict(commune, surface, postal_code=postal, built_only=built_only, db_path=db_path)

    if not matches or len(matches) > 3:
        matches = search_with_neighbors(
            commune, surface,
            postal_code=postal,
            tolerance_pct=tolerance,
            include_rank2=rank2,
            built_only=built_only,
            db_path=db_path,
        )

    combos: list[ComboMatch] = []
    if not no_combo:
        combos = search_combos(
            commune, surface,
            postal_code=postal,
            tolerance_pct=tolerance,
            include_rank2=rank2,
            max_parts=max_parts,
            built_only=built_only,
            db_path=db_path,
        )

    return matches, combos


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def main() -> None:
    params = _sidebar()

    if params is not None:
        st.session_state.search_params = params
        st.session_state.result_idx = 0
        st.session_state.search_done = False

    if "search_params" not in st.session_state:
        st.markdown("## 🏡 Cadastre Finder")
        st.markdown(
            "Renseignez la **commune** et la **surface** dans le panneau de gauche, "
            "puis cliquez sur **Lancer la recherche**."
        )
        return

    p = st.session_state.search_params

    with st.spinner("Recherche en cours…"):
        try:
            matches, combos = _run_search(
                commune=p["commune"],
                surface=p["surface"],
                postal=p["postal"],
                tolerance=p["tolerance"],
                max_parts=p["max_parts"],
                rank2=p["rank2"],
                no_combo=p["no_combo"],
                include_agri=p["include_agri"],
                db_path_str=str(DB_PATH),
            )
        except Exception as exc:
            st.error(f"Erreur lors de la recherche : {exc}")
            return

    # Fusionner et trier par score décroissant
    all_results: list[Union[ParcelMatch, ComboMatch]] = sorted(
        list(matches) + list(combos),
        key=lambda r: -r.score,
    )

    if not all_results:
        st.warning(
            "Aucun résultat trouvé. Essayez d'augmenter la tolérance surface "
            "ou d'activer les communes voisines rang 2."
        )
        return

    idx = st.session_state.get("result_idx", 0)
    idx = max(0, min(idx, len(all_results) - 1))
    st.session_state.result_idx = idx

    st.markdown(f"## Résultats — {p['commune']} · {p['surface']:,.0f} m²")
    _results_overview(all_results, idx)
    st.markdown("---")
    _display_result(all_results[idx], idx, len(all_results))


if __name__ == "__main__":
    main()
