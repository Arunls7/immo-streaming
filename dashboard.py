"""
src/dashboard.py
================
Dashboard Streamlit — Visualisation dynamique du graphe immobilier.

Lit les fichiers Parquet produits par le pipeline Spark et rafraîchit
automatiquement l'affichage. Le dashboard est volontairement découplé du
pipeline (communication via fichiers Parquet) : il peut être lancé, arrêté
ou rechargé sans impacter le traitement de flux.

Lancer (depuis la racine du projet) :
    streamlit run src/dashboard.py
"""

import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from config import settings

# ─── Palette ──────────────────────────────────────────────────────────────────
NODE_COLORS = {"USER": "#4A90D9", "SELLER": "#E67E22", "PRODUCT": "#27AE60"}
NODE_SIZES  = {"USER": 14, "SELLER": 26, "PRODUCT": 20}
EDGE_COLORS = {
    "AIME": "#3498DB", "VOUT": "#F39C12",
    "ACHAT": "#E74C3C", "PROPOSE": "#9B59B6",
}


# ══════════════════════════════════════════════════════════════════════════════
# Chargement des données
# ══════════════════════════════════════════════════════════════════════════════
def load_parquet(path) -> pd.DataFrame | None:
    """
    Charge un dossier Parquet. Retourne None si absent ou en cours d'écriture.

    On encapsule la lecture dans un try/except car le pipeline réécrit ces
    dossiers en continu : une lecture peut tomber pile pendant un overwrite.
    Dans ce cas on renvoie None et le dashboard réessaiera au prochain cycle.
    """
    path = str(path)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        return df if not df.empty else None
    except Exception:
        return None


def last_modified(path) -> str:
    """Horodatage de dernière modification d'un dossier de sortie."""
    path = str(path)
    if not os.path.exists(path):
        return "—"
    try:
        ts = max(
            os.path.getmtime(os.path.join(path, f))
            for f in os.listdir(path)
            if f.endswith(".parquet")
        )
        return datetime.fromtimestamp(ts).strftime("%H:%M:%S")
    except (ValueError, OSError):
        return "—"


# ══════════════════════════════════════════════════════════════════════════════
# Graphe PyVis
# ══════════════════════════════════════════════════════════════════════════════
def build_pyvis_graph(vertices: pd.DataFrame, edges: pd.DataFrame) -> str:
    """Construit un graphe interactif PyVis et renvoie son code HTML."""
    net = Network(height="560px", width="100%", bgcolor="#0e1117",
                  font_color="white", directed=True)
    net.barnes_hut(gravity=-8000, spring_length=110)

    v_sample  = vertices.head(settings.MAX_NODES_DISP)
    valid_ids = set(v_sample["id"].tolist())

    for _, row in v_sample.iterrows():
        ntype = row.get("type", "USER")
        net.add_node(
            row["id"],
            label=str(row.get("label", row["id"]))[:18],
            color=NODE_COLORS.get(ntype, "#888888"),
            size=NODE_SIZES.get(ntype, 14),
            title=f"{ntype} — {row['id']}",
        )

    for _, row in edges.iterrows():
        if row["src"] in valid_ids and row["dst"] in valid_ids:
            rel = row.get("relationship", "")
            net.add_edge(row["src"], row["dst"], title=rel,
                         color=EDGE_COLORS.get(rel, "#AAAAAA"), arrows="to")

    html_path = "/tmp/graph_immo.html"
    net.save_graph(html_path)
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ══════════════════════════════════════════════════════════════════════════════
# Interface
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    st.set_page_config(page_title="Immo Streaming", page_icon="🏠", layout="wide")

    st.title("🏠 Plateforme Immobilière — Streaming Temps Réel")

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Paramètres")
        refresh = st.slider("Rafraîchissement (s)", 2, 30, settings.REFRESH_SEC)
        action_filter = st.multiselect(
            "Filtrer par action",
            ["AIME", "VOUT", "ACHAT"],
            default=["AIME", "VOUT", "ACHAT"],
        )
        st.divider()
        st.markdown("**Nœuds**")
        st.markdown("🔵 Utilisateur 🟠 Vendeur 🟢 Produit")
        st.markdown("**Arêtes**")
        st.markdown("🔵 AIME 🟡 VOUT 🔴 ACHAT 🟣 PROPOSE")
        st.divider()
        st.caption(f"Dernier graphe : {last_modified(settings.VERTICES_PATH)}")
        st.caption(f"Dernières métriques : {last_modified(settings.METRICS_PATH)}")

    # ── Chargement ───────────────────────────────────────────────────────────
    vertices = load_parquet(settings.VERTICES_PATH)
    edges    = load_parquet(settings.EDGES_PATH)
    metrics  = load_parquet(settings.METRICS_PATH)
    data_ok  = vertices is not None and edges is not None

    # ── KPI ──────────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    if data_ok:
        c1.metric("👤 Utilisateurs", int((vertices["type"] == "USER").sum()))
        c2.metric("🏢 Vendeurs",     int((vertices["type"] == "SELLER").sum()))
        c3.metric("🏠 Produits",     int((vertices["type"] == "PRODUCT").sum()))
        c4.metric("🔗 Interactions", len(edges))
    else:
        for c, lbl in zip((c1, c2, c3, c4),
                          ("👤 Utilisateurs", "🏢 Vendeurs", "🏠 Produits", "🔗 Interactions")):
            c.metric(lbl, 0)

    st.divider()

    # ── Graphe ─────────────────────────────────────────────────────────────────
    st.subheader("🕸️ Graphe de Connexions")
    if data_ok:
        edges_f = edges[edges["relationship"].isin(action_filter + ["PROPOSE"])]
        components.html(build_pyvis_graph(vertices, edges_f), height=580)
    else:
        st.info(
            "⏳ En attente des données du pipeline Spark.\n\n"
            "Vérifie que `generator.py` **et** `spark_pipeline.py` tournent. "
            "Le premier graphe apparaît après le premier micro-batch (~5 s)."
        )

    # ── Analytics ────────────────────────────────────────────────────────────
    if data_ok:
        st.divider()
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("📊 Répartition des actions")
            ac = (edges[edges["relationship"].isin(["AIME", "VOUT", "ACHAT"])]
                  ["relationship"].value_counts().reset_index())
            ac.columns = ["Action", "Nombre"]
            if not ac.empty:
                fig = px.pie(ac, names="Action", values="Nombre", color="Action",
                             color_discrete_map=EDGE_COLORS, hole=0.4)
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white")
                st.plotly_chart(fig, use_container_width=True)

        with col_b:
            st.subheader("🏙️ Utilisateurs par ville")
            uc = (vertices[vertices["type"] == "USER"]["label"]
                  .value_counts().head(10).reset_index())
            uc.columns = ["Ville", "Nombre"]
            if not uc.empty:
                fig = px.bar(uc, x="Ville", y="Nombre", color="Nombre",
                             color_continuous_scale="Blues")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white",
                                  showlegend=False, xaxis_tickangle=-30)
                st.plotly_chart(fig, use_container_width=True)

        # ── Métriques de fenêtre (issues de l'agrégation Spark) ───────────────
        if metrics is not None:
            st.divider()
            st.subheader("⏱️ Volume d'actions par fenêtre temporelle")
            metrics_sorted = metrics.sort_values("window_start")
            fig = px.line(
                metrics_sorted,
                x="window_start", y="nb_actions",
                color="action_type",
                color_discrete_map=EDGE_COLORS,
                markers=True,
            )
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("📋 Détail des métriques par fenêtre"):
                st.dataframe(metrics_sorted, use_container_width=True)

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()
