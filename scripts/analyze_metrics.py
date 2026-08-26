"""
scripts/analyze_metrics.py

Genera, a partir de las métricas ya escritas por civicmesh.storage.metrics_logger
(formato $CIVICMESH_RUNS/<run_id>/metrics/<peer_id>.jsonl, con la estructura que
produce PeerLocalState.get_snapshot() en civicmesh/core/state.py), los insumos
para la sección de resultados del informe:

  1. Gráfico de convergencia del canal OBJETIVO entre peers réplica
     (mismo tópico, distinto peer -> ¿convergen al mismo valor?)
  2. Gráfico de divergencia percepción-realidad del canal SUBJETIVO
     (perception_gap = Pc(t) - Gc(t), por tópico, a lo largo del tiempo)
  3. Tabla resumen cuantitativa por tópico (media/desv. estándar del gap,
     hops promedio) -> para la sección de comparación entre dominios
  4. (opcional) Marca visualmente el instante de un experimento de caída de
     peer, si se lo indicas con --fault-time, para el gráfico de robustez

Uso:
    python scripts/analyze_metrics.py --run-id compose-run --domain delitos
    python scripts/analyze_metrics.py --run-dir ./runs/compose-run --fault-time 40

Requiere: pandas, matplotlib (pip install pandas matplotlib)
Salida: ./informe_assets/<run_id>/*.png y resumen_<run_id>.csv
"""

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def load_peer_snapshots(metrics_dir: Path) -> pd.DataFrame:
    """Lee todos los *.jsonl de metrics/ (uno por peer) y los aplana a un DataFrame
    con una fila por (peer_id, topic, timestamp)."""
    rows = []
    for jsonl_file in sorted(metrics_dir.glob("*.jsonl")):
        node_id = jsonl_file.stem
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    snap = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Snapshots de peer (PeerLocalState.get_snapshot): tienen "topics"
                if "topics" in snap:
                    ts = snap.get("timestamp")
                    for topic, tdata in snap["topics"].items():
                        rows.append({
                            "node_id": node_id,
                            "node_type": "peer",
                            "topic": topic,
                            "timestamp": ts,
                            "objective_val": tdata.get("objective_val"),
                            "subjective_val": tdata.get("subjective_val"),
                            "perception_gap": tdata.get("perception_gap"),
                            "rumor_aggregate": tdata.get("rumor_aggregate"),
                            "avg_hops_objective": tdata.get("avg_hops_objective"),
                        })
                # Snapshots de publisher (formato distinto, sin desglose por tópico):
                # se conservan aparte para la tabla de throughput, no entran al
                # gráfico de convergencia por tópico.
                elif "publisher_id" in snap:
                    rows.append({
                        "node_id": node_id,
                        "node_type": "publisher",
                        "topic": None,
                        "timestamp": snap.get("timestamp"),
                        "sim_time": snap.get("sim_time"),
                        "events_generated": snap.get("events_generated"),
                        "published_count": snap.get("published_count"),
                        "active_peers_detected": snap.get("active_peers_detected"),
                    })
    return pd.DataFrame(rows)


def plot_convergence(df: pd.DataFrame, out_dir: Path, fault_time: float | None = None):
    """Un subplot por tópico: valor objetivo reportado por cada peer réplica en el tiempo.
    Si los peers convergen, las líneas deberían quedar cerca entre sí (o seguir el
    mismo patrón con retraso por hop-latency)."""
    peer_df = df[df["node_type"] == "peer"].dropna(subset=["objective_val"])
    if peer_df.empty:
        print("No hay datos de canal objetivo en peers — ¿corriste el publicador?")
        return

    t0 = peer_df["timestamp"].min()
    peer_df = peer_df.assign(t_rel=peer_df["timestamp"] - t0)

    topics = sorted(peer_df["topic"].unique())
    fig, axes = plt.subplots(len(topics), 1, figsize=(9, 3 * len(topics)), sharex=True)
    if len(topics) == 1:
        axes = [axes]

    for ax, topic in zip(axes, topics):
        sub = peer_df[peer_df["topic"] == topic]
        for node_id, g in sub.groupby("node_id"):
            ax.plot(g["t_rel"], g["objective_val"], marker="o", markersize=3, label=node_id)
        if fault_time is not None:
            ax.axvline(fault_time, color="red", linestyle="--", alpha=0.6, label="caída de peer")
        ax.set_title(f"Convergencia canal objetivo — {topic}")
        ax.set_ylabel("Valor objetivo")
        ax.legend(fontsize=8)

    axes[-1].set_xlabel("Tiempo relativo (s)")
    fig.tight_layout()
    out_path = out_dir / "convergencia_objetivo.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Guardado: {out_path}")


def plot_perception_gap(df: pd.DataFrame, out_dir: Path, fault_time: float | None = None):
    """Un subplot por tópico: brecha Pc(t) - Gc(t) reportada por cada peer en el tiempo."""
    peer_df = df[df["node_type"] == "peer"].dropna(subset=["perception_gap"])
    if peer_df.empty:
        print("No hay datos de perception_gap en peers.")
        return

    t0 = peer_df["timestamp"].min()
    peer_df = peer_df.assign(t_rel=peer_df["timestamp"] - t0)

    topics = sorted(peer_df["topic"].unique())
    fig, axes = plt.subplots(len(topics), 1, figsize=(9, 3 * len(topics)), sharex=True)
    if len(topics) == 1:
        axes = [axes]

    for ax, topic in zip(axes, topics):
        sub = peer_df[peer_df["topic"] == topic]
        for node_id, g in sub.groupby("node_id"):
            ax.plot(g["t_rel"], g["perception_gap"], marker="o", markersize=3, label=node_id)
        ax.axhline(0, color="gray", linewidth=1, linestyle=":")
        if fault_time is not None:
            ax.axvline(fault_time, color="red", linestyle="--", alpha=0.6, label="caída de peer")
        ax.set_title(f"Brecha percepción-realidad (Pc - Gc) — {topic}")
        ax.set_ylabel("Pc(t) - Gc(t)")
        ax.legend(fontsize=8)

    axes[-1].set_xlabel("Tiempo relativo (s)")
    fig.tight_layout()
    out_path = out_dir / "divergencia_percepcion.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Guardado: {out_path}")


def summarize(df: pd.DataFrame, out_dir: Path, run_id: str):
    """Tabla resumen por tópico: media/desv. estándar de la brecha y hops promedio.
    Útil para la sección 'Comparación cuantitativa entre dominios' del informe."""
    peer_df = df[df["node_type"] == "peer"].dropna(subset=["perception_gap"])
    if peer_df.empty:
        return
    summary = peer_df.groupby("topic").agg(
        gap_mean=("perception_gap", "mean"),
        gap_std=("perception_gap", "std"),
        objective_mean=("objective_val", "mean"),
        objective_std=("objective_val", "std"),
        avg_hops=("avg_hops_objective", "mean"),
        n_muestras=("perception_gap", "count"),
    ).round(4)
    out_csv = out_dir / f"resumen_{run_id}.csv"
    summary.to_csv(out_csv)
    print(f"Guardado: {out_csv}")
    print(summary)


def main():
    parser = argparse.ArgumentParser(description="Analiza métricas CivicMesh para el informe")
    parser.add_argument("--run-dir", help="Path directo a $CIVICMESH_RUNS/<run_id>")
    parser.add_argument("--run-id", help="run_id (se combina con $CIVICMESH_RUNS o --runs-base)")
    parser.add_argument("--runs-base", default=os.environ.get("CIVICMESH_RUNS", "./runs"))
    parser.add_argument("--fault-time", type=float, default=None,
                         help="Segundo relativo donde se mató un peer, para marcarlo en las gráficas")
    parser.add_argument("--out", default="./informe_assets")
    args = parser.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
        run_id = run_dir.name
    elif args.run_id:
        run_dir = Path(args.runs_base) / args.run_id
        run_id = args.run_id
    else:
        parser.error("Debes pasar --run-dir o --run-id")
        return

    metrics_dir = run_dir / "metrics"
    if not metrics_dir.exists():
        raise SystemExit(f"No existe {metrics_dir} — ¿corriste la malla con ese run_id?")

    out_dir = Path(args.out) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_peer_snapshots(metrics_dir)
    if df.empty:
        raise SystemExit("No se encontraron snapshots en los .jsonl — revisa que los peers hayan corrido lo suficiente.")

    plot_convergence(df, out_dir, fault_time=args.fault_time)
    plot_perception_gap(df, out_dir, fault_time=args.fault_time)
    summarize(df, out_dir, run_id)


if __name__ == "__main__":
    main()