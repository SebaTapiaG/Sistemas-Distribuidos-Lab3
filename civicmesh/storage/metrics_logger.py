"""
civicmesh.storage.metrics_logger
Gestión de persistencia de métricas y coordinación en Shared Filesystem.
Escribe en $CIVICMESH_RUNS/<run_id>/metrics/ y gestiona hostfile.txt.
"""

import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("civicmesh.storage")


class MetricsLogger:
    def __init__(
        self,
        run_id: Optional[str] = None,
        base_runs_dir: Optional[str] = None,
        node_id: str = "node",
    ):
        self.node_id = node_id
        
        # Determinar directorio raíz de corridas
        env_runs = os.environ.get("CIVICMESH_RUNS")
        if env_runs:
            self.runs_dir = Path(env_runs)
        elif base_runs_dir:
            self.runs_dir = Path(base_runs_dir)
        else:
            self.runs_dir = Path("runs")

        # Determinar ID de corrida
        if run_id:
            self.run_id = run_id
        else:
            slurm_job = os.environ.get("SLURM_JOB_ID")
            if slurm_job:
                self.run_id = slurm_job
            else:
                user = os.environ.get("USER", os.environ.get("USERNAME", "local"))
                self.run_id = f"local-{user}-{int(time.time())}"

        self.current_run_dir = self.runs_dir / self.run_id
        self.metrics_dir = self.current_run_dir / "metrics"
        self.logs_dir = self.current_run_dir / "logs"
        self.hostfile_path = self.current_run_dir / "hostfile.txt"

        self._init_dirs()

    def _init_dirs(self):
        try:
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
            self.logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"No se pudieron crear directorios de metricas: {e}")

    def register_in_hostfile(self, host: str, port: int):
        """Registra host:port en hostfile.txt en el shared filesystem."""
        try:
            self.current_run_dir.mkdir(parents=True, exist_ok=True)
            line = f"{self.node_id} {host}:{port}\n"
            with open(self.hostfile_path, "a", encoding="utf-8") as f:
                f.write(line)
            logger.info(f"Registrado {self.node_id} en {self.hostfile_path}")
        except Exception as e:
            logger.error(f"Error al escribir en hostfile: {e}")

    def read_hostfile(self) -> List[Dict[str, Any]]:
        """Lee todos los endpoints registrados en hostfile.txt."""
        peers = []
        if not self.hostfile_path.exists():
            return peers

        try:
            with open(self.hostfile_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split()
                    if len(parts) == 2:
                        nid, endpoint = parts[0], parts[1]
                    else:
                        nid, endpoint = "unknown", parts[0]
                    
                    if ":" in endpoint:
                        h, p = endpoint.split(":")
                        peers.append({"node_id": nid, "host": h, "port": int(p)})
        except Exception as e:
            logger.warning(f"Error leyendo hostfile: {e}")
        return peers

    def log_metric_snapshot(self, snapshot_data: Dict[str, Any]):
        """Escribe una línea JSONL con las métricas del nodo en metrics/<node_id>.jsonl."""
        file_path = self.metrics_dir / f"{self.node_id}.jsonl"
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(snapshot_data) + "\n")
        except Exception as e:
            logger.error(f"Error guardando métricas en {file_path}: {e}")
