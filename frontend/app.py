"""
frontend/app.py
Dashboard interactivo de monitoreo en Streamlit para CivicMesh.
Consume métricas JSONL y presenta estado por tópico x canal.
"""

import json
import os
from pathlib import Path
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ==========================================
# 1. CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(
    page_title="CivicMesh Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed", # Ocultar barra lateral por defecto
)

# ==========================================
# 2. CSS MINIMALISTA Y PROFESIONAL
# ==========================================
st.markdown("""
<style>
    /* Ocultar elementos nativos de Streamlit (Header, Footer, Menu, Sidebar toggle) */
    header { visibility: hidden !important; }
    footer { visibility: hidden !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    [data-testid="stSidebar"] { display: none !important; }
    
    /* Reducir el padding superior para aprovechar el espacio */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
        max-width: 95% !important;
    }

    /* Diseño Minimalista para Tarjetas de Métricas (Glassmorphism sutil) */
    [data-testid="stMetric"] {
        background-color: rgba(30, 30, 32, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.05);
        padding: 20px 24px;
        border-radius: 12px;
        transition: transform 0.2s ease;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    /* Tipografía de las métricas */
    [data-testid="stMetricLabel"] {
        font-size: 0.95rem !important;
        color: #8E8E93 !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    [data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        color: #0A84FF !important; /* Azul moderno */
        font-weight: 600 !important;
    }
    
    /* Estilos para las pestañas (Tabs) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 3. ENCABEZADO Y CONTROLES (LAYOUT HORIZONTAL)
# ==========================================
title_col, ctrl_col1, ctrl_col2 = st.columns([3, 1, 1])

with title_col:
    st.title("📡 CivicMesh Monitor")
    st.caption("Sistemas Distribuidos y Paralelos | Framework Gossip + Pub/Sub Geográfico")

env_runs = os.environ.get("CIVICMESH_RUNS", "runs")
runs_base = Path(env_runs)
available_runs = [d.name for d in runs_base.iterdir() if d.is_dir()] if runs_base.exists() else []

with ctrl_col1:
    st.write("") # Espaciador para alinear
    selected_run = st.selectbox(
        "Corrida (Run ID)",
        options=available_runs if available_runs else ["(Sin corridas)"],
        label_visibility="collapsed"
    )

with ctrl_col2:
    st.write("") # Espaciador para alinear
    st.write("")
    auto_refresh = st.checkbox("🔄 Auto-refresco (2s)", value=True)

st.markdown("---")

if not available_runs or selected_run == "(Sin corridas)":
    st.info(f"Esperando datos de ejecución en el directorio: `{runs_base.resolve()}`")
    st.stop()

if auto_refresh:
    time.sleep(2)
    st.rerun()

current_run_path = runs_base / selected_run
metrics_dir = current_run_path / "metrics"
hostfile_path = current_run_path / "hostfile.txt"

# ==========================================
# 4. EXTRACCIÓN DE DATOS
# ==========================================
metric_files = list(metrics_dir.glob("*.jsonl")) if metrics_dir.exists() else []
if not metric_files:
    st.warning("Recolectando métricas de la red. Esperando primer volcado de datos...")
    st.stop()

peer_records, publisher_records = [], []
for m_file in metric_files:
    with open(m_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            try:
                data = json.loads(line)
                if "peer_id" in data: peer_records.append(data)
                elif "publisher_id" in data: publisher_records.append(data)
            except: pass

# ==========================================
# 5. KPIS GLOBALES
# ========================================== 
latest_ts = max([r.get("timestamp", 0) for r in peer_records + publisher_records], default=0)

# NUEVO: Contar solo peers vivos (con métricas en los últimos 5 segundos)
active_peers = 0
for p_id in set(r.get("peer_id") for r in peer_records):
    p_last = [r for r in peer_records if r.get("peer_id") == p_id][-1]
    if latest_ts - p_last.get("timestamp", 0) <= 5.0:
        active_peers += 1

total_pubs = len(set(r.get("publisher_id") for r in publisher_records))
all_topics = set()
for r in peer_records: all_topics.update(r.get("topics", {}).keys())

m_col1, m_col2, m_col3, m_col4 = st.columns(4)
m_col1.metric("Peers Activos", active_peers) # Actualizado a la variable real
m_col2.metric("Publicadores", total_pubs)
m_col3.metric("Comunas", len(all_topics))
m_col4.metric("Últ. Actualización", time.strftime("%H:%M:%S", time.localtime(latest_ts)) if latest_ts else "N/A")

# ==========================================
# 6. PROCESAMIENTO DE SNAPSHOT
# ==========================================
snapshot_rows = []
for p_id in set(r.get("peer_id") for r in peer_records):
    p_last = [r for r in peer_records if r.get("peer_id") == p_id][-1]
    for t_name, t_data in p_last.get("topics", {}).items():
        snapshot_rows.append({
            "Peer": p_id,
            "Comuna": t_name,
            "Obj (Real)": round(t_data.get("objective_val", 0), 2),
            "Eventos": t_data.get("objective_events", 0),
            "Subj (Perc)": round(t_data.get("subjective_val", 0), 2),
            "Rumores": round(t_data.get("rumor_aggregate", 0), 2),
            "Memoria EMA": round(t_data.get("ema_memory", 0), 2),
            "Brecha": round(t_data.get("perception_gap", 0), 2),
            "Hops": round(t_data.get("avg_hops_objective", 0), 1),
        })

df_snapshot = pd.DataFrame(snapshot_rows)

# ==========================================
# 7. NAVEGACIÓN PRINCIPAL (TABS)
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["📊 Vista General", "📈 Temporal", "🔄 Convergencia", "🖥️ Topología"])

# --- TAB 1: Vista General ---
with tab1:
    if not df_snapshot.empty:
        st.write("Estado actual de todos los tópicos y canales en la red P2P.")
        styled_df = df_snapshot.style.background_gradient(
            cmap='RdYlBu', subset=['Brecha'], vmin=-20, vmax=20
        ).format(precision=2)
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.caption("Aún no hay datos de snapshot suficientes.")

# --- TAB 2: Análisis Temporal ---
with tab2:
    selected_topic = st.selectbox("Analizar Comuna Específica:", options=sorted(list(all_topics)) if all_topics else ["Santiago"])

    time_series_rows = []
    for r in peer_records:
        t_data = r.get("topics", {}).get(selected_topic)
        if t_data:
            time_series_rows.append({
                "timestamp": r.get("timestamp"),
                "peer_id": r.get("peer_id"),
                "objective": t_data.get("objective_val", 0),
                "subjective": t_data.get("subjective_val", 0),
                "perception_gap": t_data.get("perception_gap", 0),
            })

    if time_series_rows:
        df_ts = pd.DataFrame(time_series_rows).sort_values("timestamp")
        
        # 1. SOLUCIÓN AL EJE X: Convertir el timestamp numérico a Fecha/Hora legible
        df_ts["timestamp"] = pd.to_datetime(df_ts["timestamp"], unit='s')
        
        # OPCIONAL: Si aún es demasiada información, puedes descomentar la siguiente línea 
        # para mostrar solo los últimos 1000 registros y evitar que se congele:
        # df_ts = df_ts.tail(1000)
        
        c1, c2 = st.columns(2)
        with c1:
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=df_ts["timestamp"], y=df_ts["objective"], mode="lines", name="Obj (Real)", line=dict(color="#34C759", width=2)))
            fig1.add_trace(go.Scatter(x=df_ts["timestamp"], y=df_ts["subjective"], mode="lines", name="Subj (Perc)", line=dict(color="#FF3B30", width=2, dash="dash")))
            fig1.update_layout(
                title=f"Evolución: {selected_topic}", 
                margin=dict(l=0, r=0, t=40, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)"
            )
            fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
            fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
            st.plotly_chart(fig1, use_container_width=True)

        with c2:
            # 2. SOLUCIÓN A LA SATURACIÓN: Cambiar px.bar por px.line
            fig2 = px.line(df_ts, x="timestamp", y="perception_gap", color="peer_id", title=f"Brecha (Pc - Gc): {selected_topic}", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig2.update_layout(
                margin=dict(l=0, r=0, t=40, b=0),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1) # Movemos la leyenda arriba
            )
            fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
            fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
            st.plotly_chart(fig2, use_container_width=True)

# --- TAB 3: Convergencia ---
with tab3:
    if not df_snapshot.empty:
        st.write("Valores del Canal Objetivo (Ground Truth) reportados por cada Peer en el último instante.")
        
        # Agregamos un pequeño recuadro informativo
        st.info("💡 Si los puntos están alineados horizontalmente, significa que el protocolo Gossip logró una **convergencia perfecta**.")
        
        # Cambiamos px.box por px.strip y coloreamos por "Peer"
        fig_conv = px.strip(
            df_snapshot, x="Comuna", y="Obj (Real)", color="Peer", 
            stripmode="group",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        # Agrandamos un poco los puntos para que se vean mejor
        fig_conv.update_traces(marker=dict(size=10, opacity=0.8))
        
        fig_conv.update_layout(
            margin=dict(l=0, r=0, t=20, b=0), 
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        fig_conv.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
        st.plotly_chart(fig_conv, use_container_width=True)

# --- TAB 4: Topología y Nodos ---
with tab4:
    st.write("Nodos actualmente registrados en la malla.")
    if hostfile_path.exists():
        with open(hostfile_path, "r", encoding="utf-8") as f:
            nodos = f.readlines()
        
        if nodos:
            for line in nodos:
                st.code(line.strip(), language="bash")
        else:
            st.info("El archivo hostfile.txt está vacío.")
    else:
        st.caption("No se encontró hostfile.txt en esta corrida.")