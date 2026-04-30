"""Interface Streamlit de cadastre-finder.

Lancement : cadastre-finder ui
         ou : streamlit run src/cadastre_finder/ui/app.py
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Union

import duckdb
import folium
import streamlit as st
import streamlit.components.v1 as components

from cadastre_finder.config import DB_PATH
from cadastre_finder.search.models import ComboMatch, ParcelMatch

# ---------------------------------------------------------------------------
# Configuration de la page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Cadastre Finder",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  /* Retire le padding top excessif de Streamlit */
  .block-container { padding-top: 1.5rem; }

  /* Carte info résultat */
  .info-card {
      background: #ffffff;
      border: 1px solid #e0e0e0;
      border-radius: 6px;
      padding: 1rem 1.2rem;
  }

  /* Indicateur de score coloré */
  .score-pill {
      display: inline-block;
      padding: 3px 12px;
      border-radius: 20px;
      font-size: 1.05rem;
      font-weight: 600;
      color: #fff;
      letter-spacing: 0.02em;
  }

  /* Barre de navigation résultats */
  div[data-testid="stHorizontalBlock"] > div:first-child button,
  div[data-testid="stHorizontalBlock"] > div:last-child button {
      width: 100%;
  }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _score_color(score: float) -> str:
    if score >= 100:
        return "#2e7d32"
    elif score >= 80:
        return "#558b2f"
    elif score >= 60:
        return "#f9a825"
    elif score >= 40:
        return "#e65100"
    else:
        return "#c62828"


def _rang_label(rank: int) -> str:
    return {0: "Commune annoncée", 1: "Voisine rang 1", 2: "Voisine rang 2"}.get(rank, f"Rang {rank}")


def _score_progress(score: float) -> float:
    """Normalise le score pour st.progress (0–1). Les scores peuvent dépasser 100."""
    return min(max(score / 118.0, 0.0), 1.0)


# ---------------------------------------------------------------------------
# Chargement des communes pour l'autocomplétion
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def _load_communes(db_path_str: str) -> list[str]:
    """Charge la liste des communes depuis la base. Retourne ["Nom (dept)", ...]."""
    try:
        con = duckdb.connect(db_path_str, read_only=True)
        rows = con.execute(
            "SELECT nom, code_dept FROM communes ORDER BY nom"
        ).fetchall()
        con.close()
        return [f"{nom} ({dept})" for nom, dept in rows]
    except Exception:
        return []


def _extract_commune_name(label: str) -> str:
    """Extrait le nom de commune depuis le label 'Nom (dept)'."""
    return label.rsplit(" (", 1)[0] if " (" in label else label


# ---------------------------------------------------------------------------
# Carte Folium pour un seul résultat
# ---------------------------------------------------------------------------

def _make_mini_map(result: Union[ParcelMatch, ComboMatch]) -> str:
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
        try:
            folium.GeoJson(
                json.loads(result.combined_geojson),
                style_function=lambda _: {
                    "fillColor": "#7b1fa2",
                    "color": "#4a148c",
                    "weight": 2.5,
                    "fillOpacity": 0.45,
                    "dashArray": "6 3",
                },
            ).add_to(fmap)
        except Exception:
            folium.Marker(location=[lat, lon]).add_to(fmap)
    else:
        color = _score_color(result.score)
        try:
            folium.GeoJson(
                json.loads(result.geometry_geojson),
                style_function=lambda _, c=color: {
                    "fillColor": c, "color": "#333", "weight": 2, "fillOpacity": 0.5,
                },
            ).add_to(fmap)
        except Exception:
            folium.Marker(location=[lat, lon]).add_to(fmap)

    folium.LayerControl(collapsed=True).add_to(fmap)
    return fmap._repr_html_()


# ---------------------------------------------------------------------------
# Affichage d'un résultat
# ---------------------------------------------------------------------------

def _display_result(result: Union[ParcelMatch, ComboMatch], idx: int, total: int) -> None:
    # Navigation
    c_prev, c_counter, c_next = st.columns([1, 4, 1])
    with c_prev:
        if st.button("← Précédent", disabled=(idx == 0), use_container_width=True, key="btn_prev"):
            st.session_state.result_idx = idx - 1
            st.rerun()
    with c_counter:
        st.markdown(
            f"<p style='text-align:center;margin:0;padding-top:6px;color:#555;'>"
            f"Résultat <strong>{idx + 1}</strong> sur {total}</p>",
            unsafe_allow_html=True,
        )
    with c_next:
        if st.button("Suivant →", disabled=(idx == total - 1), use_container_width=True, key="btn_next"):
            st.session_state.result_idx = idx + 1
            st.rerun()

    st.write("")

    col_info, col_map = st.columns([1, 2], gap="medium")

    with col_info:
        score = result.score
        color = _score_color(score)

        # Score
        st.markdown(
            f"<span class='score-pill' style='background:{color};'>Score {score:.1f}</span>"
            f"&nbsp; <span style='color:#666;font-size:0.9rem;'>{_rang_label(result.rank)}</span>",
            unsafe_allow_html=True,
        )
        st.progress(_score_progress(score))
        st.write("")

        if isinstance(result, ComboMatch):
            st.markdown(f"**Combinaison** de {result.nb_parcelles} parcelles")
            st.metric("Surface totale", f"{result.total_contenance:,} m²")
            
            # Surface bâtie cumulée
            barea = sum(p.built_area or 0 for p in result.parts)
            if barea > 0:
                st.metric("Emprise bâtie estimée", f"{barea:.1f} m²")

            st.metric("Commune", f"{result.nom_commune} ({result.code_insee})")

            # DPE / GES
            if result.dpe_label or result.ges_label:
                c1, c2 = st.columns(2)
                if result.dpe_label:
                    c1.metric("DPE", result.dpe_label)
                if result.ges_label:
                    c2.metric("GES", result.ges_label)

            pp = result.compactness
            pp_color = "#2e7d32" if pp >= 0.5 else ("#e65100" if pp < 0.2 else "#f9a825")
            st.markdown(
                f"**Compacité** : "
                f"<span style='color:{pp_color};font-weight:600;'>{pp:.2f}</span>",
                unsafe_allow_html=True,
            )

            st.write("")
            st.markdown("**Parcelles**")
            for p in result.parts:
                st.markdown(
                    f"<div style='font-size:0.85rem;padding:2px 0;"
                    f"border-bottom:1px solid #f0f0f0;'>"
                    f"<code>{p.id_parcelle}</code> &nbsp; {p.contenance:,} m²</div>",
                    unsafe_allow_html=True,
                )

            st.write("")
            st.link_button("Ouvrir sur Géoportail", result.geoportail_url)

        else:
            st.markdown(f"**Parcelle** individuelle")
            st.metric("Surface", f"{result.contenance:,} m²")
            
            if result.built_area and result.built_area > 0:
                st.metric("Emprise bâtie", f"{result.built_area:.1f} m²")

            st.metric("Commune", f"{result.nom_commune} ({result.code_insee})")
            
            # DPE / GES
            if result.dpe_label or result.ges_label:
                c1, c2 = st.columns(2)
                if result.dpe_label:
                    c1.metric("DPE", result.dpe_label)
                if result.ges_label:
                    c2.metric("GES", result.ges_label)

            st.metric("Identifiant", result.id_parcelle)

            st.write("")
            c1, c2 = st.columns(2)
            with c1:
                st.link_button("Géoportail", result.geoportail_url)
            with c2:
                st.link_button("Street View", result.street_view_url)

    with col_map:
        html = _make_mini_map(result)
        b64_html = base64.b64encode(html.encode()).decode()
        st.iframe(src=f"data:text/html;base64,{b64_html}", height=480)


# ---------------------------------------------------------------------------
# Barre de résultats (vue d'ensemble)
# ---------------------------------------------------------------------------

def _results_overview(
    all_results: list[Union[ParcelMatch, ComboMatch]],
    current_idx: int,
) -> None:
    n = len(all_results)
    visible = min(n, 12)
    cols = st.columns(visible)
    for i, (col, r) in enumerate(zip(cols, all_results[:visible])):
        with col:
            color = _score_color(r.score)
            label = "C" if isinstance(r, ComboMatch) else "P"
            border = "border:2px solid #1565c0;" if i == current_idx else "border:2px solid transparent;"
            st.markdown(
                f"<div style='text-align:center;background:{color};color:#fff;"
                f"border-radius:4px;padding:5px 2px;font-size:0.78rem;{border}'>"
                f"{label}<br><strong>{r.score:.0f}</strong></div>",
                unsafe_allow_html=True,
            )
    if n > visible:
        st.caption(f"… et {n - visible} autres résultats")


# ---------------------------------------------------------------------------
# Recherche
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=300)
def _run_search(
    commune: str,
    surface: float,
    living_surface: float | None,
    dpe: str | None,
    ges: str | None,
    postal: str | None,
    tolerance_m2: float,
    rank2: bool,
    db_path_str: str,
) -> list[Union[ParcelMatch, ComboMatch]]:
    from cadastre_finder.search.orchestrator import search_orchestrated

    db_path = Path(db_path_str)
    tolerance_pct = (tolerance_m2 / surface * 100.0) if surface > 0 else 0.0

    return search_orchestrated(
        commune,
        surface,
        living_surface=living_surface,
        dpe_label=dpe,
        ges_label=ges,
        postal_code=postal,
        tolerance_pct=tolerance_pct,
        db_path=db_path,
    )


# ---------------------------------------------------------------------------
# Formulaire sidebar
# ---------------------------------------------------------------------------

def _sidebar() -> dict | None:
    st.sidebar.title("Cadastre Finder")
    st.sidebar.caption("Recherche de parcelles par commune et surface")
    st.sidebar.write("")

    # --- Analyse d'annonce ---
    ad_text = st.sidebar.text_area(
        "Annonce brute (optionnel)", 
        placeholder="Collez l'annonce ici pour extraire les critères...",
        help="Extrait automatiquement les surfaces et labels DPE/GES."
    )
    
    extracted = None
    if ad_text:
        from cadastre_finder.search.ad_parser import parse_ad_text
        extracted = parse_ad_text(ad_text)
        st.sidebar.success("Critères extraits de l'annonce !")

    # Autocomplétion commune
    communes_list = _load_communes(str(DB_PATH))
    if communes_list:
        selected_label = st.sidebar.selectbox(
            "Commune *",
            options=communes_list,
            index=None,
            placeholder="Tapez pour filtrer…",
        )
        commune = _extract_commune_name(selected_label) if selected_label else ""
    else:
        commune = st.sidebar.text_input("Commune *", placeholder="ex : Neuvy-le-Roi")

    # Champs de recherche avec valeurs par défaut extraites
    def_surface = extracted.terrain_surface if extracted and extracted.terrain_surface else 5000.0
    surface = st.sidebar.number_input(
        "Surface Terrain (m²) *", min_value=100.0, max_value=100_000.0, value=float(def_surface), step=50.0
    )
    
    def_living = extracted.living_surface if extracted and extracted.living_surface else None
    living_surface = st.sidebar.number_input(
        "Surface Habitable (m²)", min_value=0.0, max_value=2000.0, value=float(def_living) if def_living else 0.0, step=10.0
    )
    living_surface = living_surface if living_surface > 0 else None

    c1, c2 = st.sidebar.columns(2)
    def_dpe = extracted.dpe_label if extracted and extracted.dpe_label else None
    dpe = c1.selectbox("DPE", options=[None, "A", "B", "C", "D", "E", "F", "G"], index=[None, "A", "B", "C", "D", "E", "F", "G"].index(def_dpe))
    
    def_ges = extracted.ges_label if extracted and extracted.ges_label else None
    ges = c2.selectbox("GES", options=[None, "A", "B", "C", "D", "E", "F", "G"], index=[None, "A", "B", "C", "D", "E", "F", "G"].index(def_ges))

    postal = st.sidebar.text_input("Code postal", placeholder="optionnel")

    st.sidebar.divider()

    tolerance_m2 = st.sidebar.number_input(
        "Tolérance ±m²", min_value=0, max_value=5_000, value=100, step=10
    )
    rank2 = st.sidebar.checkbox("Communes voisines rang 2", value=True)

    st.sidebar.divider()
    launched = st.sidebar.button("Lancer la recherche", type="primary", use_container_width=True)

    if not launched:
        return None
    if not commune:
        st.sidebar.error("La commune est obligatoire.")
        return None

    return {
        "commune": commune,
        "surface": float(surface),
        "living_surface": living_surface,
        "dpe": dpe,
        "ges": ges,
        "postal": postal.strip() or None,
        "tolerance_m2": float(tolerance_m2),
        "rank2": rank2,
    }


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def main() -> None:
    params = _sidebar()

    if params is not None:
        st.session_state.search_params = params
        st.session_state.result_idx = 0

    if "search_params" not in st.session_state:
        st.title("Cadastre Finder")
        st.markdown(
            "Sélectionnez une **commune** et une **surface** dans le panneau de gauche, "
            "puis lancez la recherche."
        )
        return

    p = st.session_state.search_params

    with st.spinner("Recherche en cours…"):
        try:
            all_results = _run_search(
                commune=p["commune"],
                surface=p["surface"],
                living_surface=p.get("living_surface"),
                dpe=p.get("dpe"),
                ges=p.get("ges"),
                postal=p["postal"],
                tolerance_m2=p["tolerance_m2"],
                rank2=p["rank2"],
                db_path_str=str(DB_PATH),
            )
        except Exception as exc:
            st.error(f"Erreur lors de la recherche : {exc}")
            return

    if not all_results:
        st.warning(
            "Aucun résultat. Augmentez la tolérance ou activez les communes voisines rang 2."
        )
        return

    idx = st.session_state.get("result_idx", 0)
    idx = max(0, min(idx, len(all_results) - 1))
    st.session_state.result_idx = idx

    st.subheader(f"{p['commune']} · {p['surface']:,.0f} m²")
    st.caption(f"{len(all_results)} résultat(s) — triés par score")
    _results_overview(all_results, idx)
    st.divider()
    _display_result(all_results[idx], idx, len(all_results))


if __name__ == "__main__":
    main()
