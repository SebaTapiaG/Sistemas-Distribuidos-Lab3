"""
civicmesh.node.peer
Proceso Peer principal de CivicMesh.
Integra membresía Gossip, Pub/Sub geográfico, estado agregado local y log de métricas.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml

from civicmesh.core.protocol import CivicMessage, MessageType, PeerInfo
from civicmesh.core.network import AsyncNetworkTransport
from civicmesh.core.gossip import GossipMembership
from civicmesh.core.pubsub import PubSubManager
from civicmesh.core.state import PeerLocalState
from civicmesh.storage.metrics_logger import MetricsLogger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("civicmesh.peer")


class PeerNode:
    def __init__(
        self,
        peer_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        topics: Optional[List[str]] = None,
        config_path: str = "config/default_config.yaml",
        run_id: Optional[str] = None,
        seeds: Optional[List[str]] = None,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.topics = topics or ["Santiago"]
        self.config_path = config_path
        self.run_id = run_id
        self.initial_seeds = seeds or []

        self.config = self._load_config()
        self.comunas_data = self._load_comunas_data()

        # Componentes del nodo
        self.transport = AsyncNetworkTransport(host=self.host, port=self.port)
        self.state = PeerLocalState(peer_id=self.peer_id)
        
        gossip_cfg = self.config.get("gossip", {})
        self.gossip = GossipMembership(
            peer_id=self.peer_id,
            host=self.host,
            port=self.port,
            topics=self.topics,
            transport=self.transport,
            fanout=gossip_cfg.get("fanout", 2),
            partial_view_size=gossip_cfg.get("partial_view_size", 6),
            heartbeat_interval=gossip_cfg.get("heartbeat_interval_sec", 1.0),
            fail_timeout=gossip_cfg.get("fail_timeout_sec", 3.5),
            policy=gossip_cfg.get("policy", "hybrid"),
            hybrid_random_ratio=gossip_cfg.get("hybrid_random_ratio", 0.5),
            comunas_data=self.comunas_data,
            seed=self.config.get("global", {}).get("seed", 42) + hash(self.peer_id) % 1000,
        )

        pubsub_cfg = self.config.get("pubsub", {})
        self.pubsub = PubSubManager(
            peer_id=self.peer_id,
            transport=self.transport,
            subscribed_topics=self.topics,
            comunas_data=self.comunas_data,
            dedup_cache_size=pubsub_cfg.get("dedup_cache_size", 2000),
            dedup_ttl_sec=pubsub_cfg.get("dedup_ttl_sec", 30.0),
            channel_configs=pubsub_cfg.get("channels", {}),
        )

        self.metrics_logger = MetricsLogger(run_id=self.run_id, node_id=self.peer_id)
        
        self._running = False
        self._metrics_task: Optional[asyncio.Task] = None
        self._hostfile_task: Optional[asyncio.Task] = None

    def _load_config(self) -> Dict[str, Any]:
        p = Path(self.config_path)
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

    async def handle_message(self, msg: CivicMessage):
        """Despachador central de mensajes entrantes."""
        msg_type = msg.header.msg_type

        # 1. Mensajes de Membresía / Gossip
        if msg_type == MessageType.GOSSIP_DIGEST:
            await self.gossip.handle_gossip_digest(msg)
            return

        elif msg_type == MessageType.PING:
            ack = CivicMessage.create_ack(
                sender_id=self.peer_id,
                sender_host=self.host,
                sender_port=self.port,
                target_msg_id=msg.header.msg_id,
            )
            await self.transport.send_message(msg.header.sender_host, msg.header.sender_port, ack)
            return

        elif msg_type == MessageType.ACK:
            # Reconocimiento de ping recibido
            return

        # 2. Mensajes de Datos (Pub/Sub)
        elif msg_type == MessageType.PUB:
            topic = msg.header.topic or ""
            channel = msg.header.channel or "objective"
            val = float(msg.payload.get("value", msg.payload.get("pm2_5", msg.payload.get("total_crimes", 0.0))))

            # Evaluar si el mensaje debe reenviarse
            alive_peers = self.gossip.get_alive_peers()
            must_forward = self.pubsub.should_forward(msg, topic, alive_peers)

            # Si el peer está suscrito al tópico o vecindad, actualiza su estado local
            if self.pubsub.is_subscribed_to(topic) or self.pubsub.is_topic_interested_or_neighbor(self.topics, topic):
                if channel == "objective":
                    self.state.record_objective_event(
                        topic=topic,
                        value=val,
                        payload=msg.payload,
                        hop_count=msg.header.hop_count,
                    )
                elif channel == "subjective":
                    self.state.record_subjective_rumor(
                        topic=topic,
                        rumor_val=val,
                        hop_count=msg.header.hop_count,
                    )
                    self.state.set_subjective_perception(
                    topic=topic,
                    perception_val=val,
                    ema_val=msg.payload.get("ema_memory", val),
                    )

            # Reenvío controlado (anti-flooding)
            if must_forward:
                await self.pubsub.forward_message(msg, topic, alive_peers)

    async def start(self):
        """Inicia el peer, registra en hostfile, conecta a seeds y arranca bucles."""
        self.host, self.port = await self.transport.start(self.handle_message)
        self.gossip.host = self.host
        self.gossip.port = self.port

        # Registrar en Shared FS hostfile.txt
        self.metrics_logger.register_in_hostfile(self.host, self.port)

        # Iniciar servicio Gossip
        await self.gossip.start()

        # Parsear e inicializar conexión con seeds
        seed_tuples = []
        for s in self.initial_seeds:
            if ":" in s:
                sh, sp = s.split(":")
                seed_tuples.append((sh, int(sp)))
        if seed_tuples:
            await self.gossip.join_network(seed_tuples)

        # Iniciar tareas periódicas
        self._running = True
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        self._hostfile_task = asyncio.create_task(self._hostfile_discovery_loop())

        logger.info(f"[{self.peer_id}] Peer operativo en {self.host}:{self.port}, tópicos={self.topics}")

    async def _metrics_loop(self):
        """Vuelca periódicamente el estado agregado a metrics/<peer_id>.jsonl."""
        interval = float(self.config.get("storage", {}).get("metrics_flush_interval_sec", 2.0))
        while self._running:
            try:
                await asyncio.sleep(interval)
                snapshot = self.state.get_snapshot()
                snapshot["active_peers"] = len(self.gossip.get_alive_peers())
                snapshot["pubsub_stats"] = self.pubsub.stats
                self.metrics_logger.log_metric_snapshot(snapshot)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.peer_id}] Error registrando métricas: {e}")

    async def _hostfile_discovery_loop(self):
        """Lee periódicamente hostfile.txt en el shared FS para descubrir nuevos nodos."""
        descubiertos = set() # NUEVO: Memoria de peers ya descubiertos por archivo
        
        while self._running:
            try:
                await asyncio.sleep(3.0)
                registered = self.metrics_logger.read_hostfile()
                for p_dict in registered:
                    if p_dict["host"] == self.host and p_dict["port"] == self.port:
                        continue
                    
                    peer_id = p_dict["node_id"]
                    
                    # NUEVO: Solo inyectar al Gossip si es la primera vez que lo leemos
                    if peer_id not in descubiertos:
                        descubiertos.add(peer_id)
                        p_info = PeerInfo(
                            peer_id=peer_id,
                            host=p_dict["host"],
                            port=p_dict["port"],
                        )
                        self.gossip.update_peer(p_info)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def stop(self):
        """Detención limpia del peer."""
        self._running = False
        if self._metrics_task:
            self._metrics_task.cancel()
        if self._hostfile_task:
            self._hostfile_task.cancel()
        await self.gossip.stop()
        await self.transport.stop()
        logger.info(f"[{self.peer_id}] Peer apagado correctamente.")


def main():
    parser = argparse.ArgumentParser(description="CivicMesh Peer Node")
    parser.add_argument("--id", required=True, help="ID único del peer")
    parser.add_argument("--host", default="127.0.0.1", help="Host/IP de escucha")
    parser.add_argument("--port", type=int, default=0, help="Puerto de escucha (0 para dinámico)")
    parser.add_argument("--topics", nargs="+", default=["Santiago"], help="Lista de comunas suscritas")
    parser.add_argument("--seeds", nargs="*", default=[], help="Endpoints semilla host:port")
    parser.add_argument("--config", default="config/default_config.yaml", help="Ruta al YAML de configuración")
    parser.add_argument("--run-id", default=None, help="ID de corrida para metrics")

    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    peer = PeerNode(
        peer_id=args.id,
        host=args.host,
        port=args.port,
        topics=args.topics,
        config_path=args.config,
        run_id=args.run_id,
        seeds=args.seeds,
    )

    try:
        loop.run_until_complete(peer.start())
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Recibida señal de detención...")
    finally:
        loop.run_until_complete(peer.stop())


if __name__ == "__main__":
    main()
