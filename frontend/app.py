"""
frontend/app.py
Dashboard interactivo de monitoreo en Streamlit para CivicMesh.
Consume métricas JSONL desde $CIVICMESH_RUNS/<run_id>/metrics/ y presenta
estado por tópico x canal, brecha percepción-realidad y convergencia entre peers.
"""

import json
import os
from pathlib import Path
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="CivicMesh Monitor - SDP 1-2026",
    page_icon="📡",
    layout="wide",
)

st.title("📡 CivicMesh: Monitoreo Ciudadano P2P")
st.markdown(
    "**Sistemas Distribuidos y Paralelos (USACH)** — *Framework Gossip + Pub/Sub Geográfico*"
)

# 1. Selector de Corrida (Run ID)
env_runs = os.environ.get("CIVICMESH_RUNS", "runs")
runs_base = Path(env_runs)

available_runs = []
if runs_base.exists():
    available_runs = [d.name for d in runs_base.iterdir() if d.is_dir()]

selected_run = st.sidebar.selectbox(
    "Seleccionar Corrida (Run ID)",
    options=available_runs if available_runs else ["(Sin corridas detectadas)"],
)

auto_refresh = st.sidebar.checkbox("Auto-refresco (2s)", value=True)
if auto_refresh:
    time.sleep(2)
    st.rerun()

if not available_runs or selected_run == "(Sin corridas detectadas)":
    st.info(f"Esperando datos de ejecución en `{runs_base.resolve()}`...")
    st.stop()

current_run_path = runs_base / selected_run
metrics_dir = current_run_path / "metrics"
hostfile_path = current_run_path / "hostfile.txt"

# 2. Cargar Hostfile y Nodos Activos
st.sidebar.subheader("Nodos Registrados")
if hostfile_path.exists():
    with open(hostfile_path, "r", encoding="utf-8") as f:
        hostfile_content = f.readlines()
    for line in hostfile_content:
        st.sidebar.text(f"🟢 {line.strip()}")
else:
    st.sidebar.text("No se encontró hostfile.txt")

# 3. Cargar y Procesar Archivos de Métricas JSONL
metric_files = list(metrics_dir.glob("*.jsonl")) if metrics_dir.exists() else []

if not metric_files:
    st.warning("No hay archivos de métricas disponibles aún en la corrida.")
    st.stop()

peer_records = []
publisher_records = []

for m_file in metric_files:
    with open(m_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if "peer_id" in data:
                    peer_records.append(data)
                elif "publisher_id" in data:
                    publisher_records.append(data)
            except Exception:
                pass

# 4. Resumen General en Métricas KPI
st.subheader("Estado de la Malla")
col1, col2, col3, col4 = st.columns(4)

total_peers = len(set(r.get("peer_id") for r in peer_records))
col1.metric("Peers Activos", total_peers)

total_pubs = len(set(r.get("publisher_id") for r in publisher_records))
col2.metric("Publicadores", total_pubs)

all_topics = set()
for r in peer_records:
    all_topics.update(r.get("topics", {}).keys())
col3.metric("Comunas Monitoreadas", len(all_topics))

latest_ts = max([r.get("timestamp", 0) for r in peer_records + publisher_records], default=0)
col4.metric("Última Actualización", time.strftime("%H:%M:%S", time.localtime(latest_ts)) if latest_ts else "N/A")

# 5. Vista por Tópico x Canal
st.markdown("---")
st.subheader("📊 Estado por Tópico × Canal (Último Snapshot)")

snapshot_rows = []
for p_id in set(r.get("peer_id") for r in peer_records):
    p_last = [r for r in peer_records if r.get("peer_id") == p_id][-1]
    topics_dict = p_last.get("topics", {})
    for t_name, t_data in topics_dict.items():
        snapshot_rows.append({
            "Peer": p_id,
            "Comuna": t_name,
            "Objetivo (Ground Truth)": round(t_data.get("objective_val", 0), 2),
            "Eventos Obj": t_data.get("objective_events", 0),
            "Subjetivo (Percepción)": round(t_data.get("subjective_val", 0), 2),
            "Rumores": round(t_data.get("rumor_aggregate", 0), 2),
            "Memoria EMA": round(t_data.get("ema_memory", 0), 2),
            "Brecha (Subj - Obj)": round(t_data.get("perception_gap", 0), 2),
            "Hops Promedio": round(t_data.get("avg_hops_objective", 0), 1),
        })

if snapshot_rows:
    df_snapshot = pd.DataFrame(snapshot_rows)
    st.dataframe(df_snapshot, use_container_width=True)

# 6. Gráficos de Evolución Temporal y Brecha Percepción - Realidad
st.markdown("---")
st.subheader("📈 Brecha Percepción vs Realidad en el Tiempo")

selected_topic = st.selectbox("Seleccionar Comuna para Análisis Detallado", options=sorted(list(all_topics)) if all_topics else ["Santiago"])

time_series_rows = []
for r in peer_records:
    p_id = r.get("peer_id")
    ts = r.get("timestamp")
    t_data = r.get("topics", {}).get(selected_topic)
    if t_data:
        time_series_rows.append({
            "timestamp": ts,
            "peer_id": p_id,
            "objective": t_data.get("objective_val", 0),
            "subjective": t_data.get("subjective_val", 0),
            "perception_gap": t_data.get("perception_gap", 0),
            "ema_memory": t_data.get("ema_memory", 0),
        })

if time_series_rows:
    df_ts = pd.DataFrame(time_series_rows).sort_values("timestamp")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=df_ts["timestamp"], y=df_ts["objective"], mode="lines+markers", name="Canal Objetivo (Real)", line=dict(color="#2ca02c", width=2)))
        fig1.add_trace(go.Scatter(x=df_ts["timestamp"], y=df_ts["subjective"], mode="lines+markers", name="Canal Subjetivo (Percepción)", line=dict(color="#d62728", width=2, dash="dash")))
        fig1.update_layout(title=f"Evolución Temporal en {selected_topic}", xaxis_title="Timestamp", yaxis_title="Valor")
        st.plotly_chart(fig1, use_container_width=True)

    with col_chart2:
        fig2 = px.bar(df_ts, x="timestamp", y="perception_gap", color="peer_id", title=f"Brecha Percepción - Realidad (Pc - Gc) en {selected_topic}")
        fig2.update_layout(xaxis_title="Timestamp", yaxis_title="Brecha")
        st.plotly_chart(fig2, use_container_width=True)

# 7. Convergencia entre Peers
st.markdown("---")
st.subheader("🔄 Convergencia del Canal Objetivo entre Réplicas (Peers)")
if snapshot_rows:
    fig_conv = px.box(df_snapshot, x="Comuna", y="Objetivo (Ground Truth)", color="Comuna", points="all", title="Dispersión del Canal Objetivo entre Peers")
    st.plotly_chart(fig_conv, use_container_width=True)
