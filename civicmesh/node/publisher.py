"""
civicmesh.node.publisher
Proceso Publicador de CivicMesh para inyección de eventos en la malla.
Soporta Dominio A (Delitos estocásticos) y Dominio B (Replay de calidad del aire con IDW).
"""

import argparse
import asyncio
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import yaml

from civicmesh.core.protocol import CivicMessage
from civicmesh.core.network import AsyncNetworkTransport
from civicmesh.domains.domain_a_delitos import DelitosDomain
from civicmesh.domains.domain_b_aire import CalidadAireDomain
from civicmesh.storage.metrics_logger import MetricsLogger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Publisher] %(message)s")
logger = logging.getLogger("civicmesh.publisher")


class PublisherNode:
    def __init__(
        self,
        publisher_id: str,
        domain_type: str, # "delitos" o "aire"
        config_path: str = "config/default_config.yaml",
        domain_config_path: Optional[str] = None,
        run_id: Optional[str] = None,
        interval_sec: float = 2.0,
        max_steps: Optional[int] = None,
    ):
        self.publisher_id = publisher_id
        self.domain_type = domain_type.lower()
        self.config_path = config_path
        self.domain_config_path = domain_config_path or (
            "config/domain_a_delitos.yaml" if self.domain_type in ["delitos", "a"] else "config/domain_b_aire.yaml"
        )
        self.run_id = run_id
        self.interval_sec = interval_sec
        self.max_steps = max_steps

        self.config = self._load_yaml(self.config_path)
        self.domain_config = self._load_yaml(self.domain_config_path)
        self.comunas_data = self._load_comunas_data()

        seed = int(self.domain_config.get("seed", self.config.get("global", {}).get("seed", 42)))

        # Instanciar el plugin de dominio correspondiente
        if self.domain_type in ["delitos", "a"]:
            self.domain_plugin = DelitosDomain(config=self.domain_config, seed=seed)
        else:
            self.domain_plugin = CalidadAireDomain(
                config=self.domain_config,
                comunas_data=self.comunas_data,
                seed=seed,
            )

        self.transport = AsyncNetworkTransport()
        self.metrics_logger = MetricsLogger(run_id=self.run_id, node_id=self.publisher_id)
        self._running = False

    def _load_yaml(self, path_str: str) -> Dict[str, Any]:
        p = Path(path_str)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _load_comunas_data(self) -> Dict[str, Any]:
        comunas_file = self.config.get("global", {}).get("comunas_file", "data/comunas_santiago.json")
        p = Path(comunas_file)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    async def start(self):
        """Inicia el publicador y el bucle de inyección de eventos a la malla."""
        self._running = True
        logger.info(f"Iniciando Publicador '{self.publisher_id}' para dominio '{self.domain_type}'...")

        step_count = 0
        sim_time = 0.0

        channels_cfg = self.config.get("pubsub", {}).get("channels", {})
        obj_ttl = channels_cfg.get("objective", {}).get("ttl", 4)
        sub_ttl = channels_cfg.get("subjective", {}).get("ttl", 3)

        while self._running:
            try:
                # 1. Leer endpoints activos de peers desde hostfile.txt
                peers = self.metrics_logger.read_hostfile()
                peer_endpoints = [(p["host"], p["port"]) for p in peers]

                # 2. Generar eventos del paso actual de simulación
                events = self.domain_plugin.step(t=sim_time, delta_t=self.interval_sec)

                published_count = 0
                for ev in events:
                    ch = ev["channel"]
                    topic = ev["topic"]
                    val = ev["value"]
                    payload = ev["payload"]
                    ttl = obj_ttl if ch == "objective" else sub_ttl
                    priority = 1 if ch == "objective" else 2

                    msg = CivicMessage.create_event(
                        sender_id=self.publisher_id,
                        sender_host=self.transport.host,
                        sender_port=self.transport.port,
                        channel=ch,
                        topic=topic,
                        payload=payload,
                        ttl=ttl,
                        priority=priority,
                    )

                    if peer_endpoints:
                        sent = await self.transport.broadcast_message(peer_endpoints, msg)
                        published_count += sent

                # 3. Registrar métrica del publicador
                snapshot = {
                    "publisher_id": self.publisher_id,
                    "domain": self.domain_type,
                    "sim_time": sim_time,
                    "step": step_count,
                    "active_peers_detected": len(peer_endpoints),
                    "events_generated": len(events),
                    "published_count": published_count,
                    "timestamp": time.time(),
                }
                self.metrics_logger.log_metric_snapshot(snapshot)

                logger.info(
                    f"[Paso {step_count} | t={sim_time:.1f}s] Generados {len(events)} eventos "
                    f"hacia {len(peer_endpoints)} peers (Envíos exitosos: {published_count})"
                )

                step_count += 1
                sim_time += self.interval_sec

                if self.max_steps and step_count >= self.max_steps:
                    logger.info(f"Alcanzado número máximo de pasos ({self.max_steps}). Finalizando...")
                    break

                await asyncio.sleep(self.interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en ciclo de publicación: {e}")
                await asyncio.sleep(1.0)

    async def stop(self):
        self._running = False
        await self.transport.stop()


def main():
    parser = argparse.ArgumentParser(description="CivicMesh Event Publisher")
    parser.add_argument("--id", default="publisher-1", help="ID único del publicador")
    parser.add_argument("--domain", choices=["delitos", "aire", "a", "b"], default="delitos", help="Dominio a simular")
    parser.add_argument("--config", default="config/default_config.yaml", help="Ruta config default")
    parser.add_argument("--domain-config", default=None, help="Ruta config de dominio")
    parser.add_argument("--run-id", default=None, help="ID de corrida")
    parser.add_argument("--interval", type=float, default=2.0, help="Intervalo de paso en segundos")
    parser.add_argument("--steps", type=int, default=None, help="Número de pasos a ejecutar (opcional)")

    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    pub = PublisherNode(
        publisher_id=args.id,
        domain_type=args.domain,
        config_path=args.config,
        domain_config_path=args.domain_config,
        run_id=args.run_id,
        interval_sec=args.interval,
        max_steps=args.steps,
    )

    try:
        loop.run_until_complete(pub.start())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        loop.run_until_complete(pub.stop())


if __name__ == "__main__":
    main()
