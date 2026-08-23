"""
civicmesh.core.gossip
Protocolo de membresía Gossip: descubrimiento de peers, mantenimiento de vista parcial,
detección de caídas por timeout y políticas de fanout (aleatorio, sesgado geográficamente e híbrido).
"""

import asyncio
import json
import logging
import math
import random
import time
from typing import Dict, List, Optional, Set, Tuple
from civicmesh.core.protocol import CivicMessage, PeerInfo, PeerStatus
from civicmesh.core.network import AsyncNetworkTransport

logger = logging.getLogger("civicmesh.gossip")


class GossipMembership:
    def __init__(
        self,
        peer_id: str,
        host: str,
        port: int,
        topics: List[str],
        transport: AsyncNetworkTransport,
        fanout: int = 2,
        partial_view_size: int = 6,
        heartbeat_interval: float = 1.0,
        fail_timeout: float = 3.5,
        policy: str = "hybrid",
        hybrid_random_ratio: float = 0.5,
        comunas_data: Optional[Dict] = None,
        seed: Optional[int] = None,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.topics = set(topics)
        self.transport = transport
        self.fanout = fanout
        self.partial_view_size = partial_view_size
        self.heartbeat_interval = heartbeat_interval
        self.fail_timeout = fail_timeout
        self.policy = policy.lower()
        self.hybrid_random_ratio = hybrid_random_ratio
        self.comunas_data = comunas_data or {}
        
        self.rng = random.Random(seed if seed is not None else None)
        
        # Vista parcial de peers: {peer_id: PeerInfo}
        self.partial_view: Dict[str, PeerInfo] = {}
        self._running = False
        self._gossip_task: Optional[asyncio.Task] = None

    @property
    def self_info(self) -> PeerInfo:
        return PeerInfo(
            peer_id=self.peer_id,
            host=self.host,
            port=self.port,
            topics=list(self.topics),
            last_seen=time.time(),
            status=PeerStatus.ALIVE,
        )

    def update_peer(self, peer: PeerInfo):
        """Actualiza o inserta un peer en la vista parcial."""
        if peer.peer_id == self.peer_id:
            return

        now = time.time()
        effective_last_seen = peer.last_seen if peer.last_seen > 0 else now

        if peer.peer_id in self.partial_view:
            existing = self.partial_view[peer.peer_id]
            existing.host = peer.host
            existing.port = peer.port
            existing.topics = list(set(existing.topics + peer.topics))
            existing.last_seen = max(existing.last_seen, effective_last_seen)
            existing.status = PeerStatus.ALIVE
        else:
            # Si la vista está llena, reemplazamos el peer más antiguo o inactivo
            if len(self.partial_view) >= self.partial_view_size:
                oldest_id = min(self.partial_view.keys(), key=lambda pid: self.partial_view[pid].last_seen)
                logger.debug(f"Vista llena en {self.peer_id}. Expulsando peer antiguo {oldest_id}")
                del self.partial_view[oldest_id]

            peer.last_seen = effective_last_seen
            peer.status = PeerStatus.ALIVE
            self.partial_view[peer.peer_id] = peer
            logger.info(f"[{self.peer_id}] Nuevo peer descubierto: {peer.peer_id} en {peer.endpoint}")

    def remove_peer(self, peer_id: str):
        """Remueve un peer de la vista parcial."""
        if peer_id in self.partial_view:
            del self.partial_view[peer_id]
            logger.warning(f"[{self.peer_id}] Peer eliminado por caída/timeout: {peer_id}")

    def get_alive_peers(self) -> List[PeerInfo]:
        """Retorna la lista de peers que no han superado el timeout de fallo."""
        now = time.time()
        alive = []
        for pid, peer in list(self.partial_view.items()):
            if now - peer.last_seen <= self.fail_timeout:
                alive.append(peer)
            else:
                peer.status = PeerStatus.DEAD
                self.remove_peer(pid)
        return alive

    def _calculate_geographic_proximity(self, peer_topics: List[str]) -> float:
        """
        Calcula una métrica de proximidad (score mayor = mayor cercanía) entre los tópicos locales
        y los del peer objetivo, basado en vecindad o distancia euclidiana en el grafo comunal.
        """
        if not self.topics or not peer_topics:
            return 0.0

        # Si comparten al menos una comuna exactamente
        common = self.topics.intersection(set(peer_topics))
        if common:
            return 10.0

        score = 0.0
        comunas = self.comunas_data.get("comunas", {})
        for my_topic in self.topics:
            my_meta = comunas.get(my_topic, {})
            my_vecinos = set(my_meta.get("vecinos", []))
            for target_topic in peer_topics:
                if target_topic in my_vecinos:
                    score += 5.0
                elif my_meta and target_topic in comunas:
                    target_meta = comunas[target_topic]
                    # Distancia euclidiana aproximada lat/lon
                    d = math.hypot(
                        my_meta.get("lat", 0) - target_meta.get("lat", 0),
                        my_meta.get("lon", 0) - target_meta.get("lon", 0),
                    )
                    score += max(0.0, 2.0 - d * 10)
        return score

    def select_gossip_targets(self, k: Optional[int] = None) -> List[PeerInfo]:
        """
        Selecciona k peers de la vista parcial según la política configurada:
        - random: Selección uniforme.
        - topic_biased: Ponderada por proximidad de tópicos/comunas.
        - hybrid: Mezcla aleatoria (exploración) y sesgada (explotación).
        """
        alive = self.get_alive_peers()
        if not alive:
            return []

        num_targets = min(k or self.fanout, len(alive))

        if self.policy == "random":
            return self.rng.sample(alive, num_targets)

        elif self.policy == "topic_biased":
            # Ordenar por proximidad
            scored = [(self._calculate_geographic_proximity(p.topics), p) for p in alive]
            scored.sort(key=lambda item: item[0], reverse=True)
            return [p for _, p in scored[:num_targets]]

        elif self.policy == "hybrid":
            n_rand = int(round(num_targets * self.hybrid_random_ratio))
            n_bias = num_targets - n_rand
            
            # Parte aleatoria
            random_pool = list(alive)
            selected_random = self.rng.sample(random_pool, min(n_rand, len(random_pool)))
            
            # Parte sesgada
            remaining = [p for p in alive if p not in selected_random]
            if remaining and n_bias > 0:
                scored = [(self._calculate_geographic_proximity(p.topics), p) for p in remaining]
                scored.sort(key=lambda item: item[0], reverse=True)
                selected_biased = [p for _, p in scored[:n_bias]]
            else:
                selected_biased = []

            return selected_random + selected_biased

        # Fallback por defecto
        return self.rng.sample(alive, num_targets)

    async def handle_gossip_digest(self, msg: CivicMessage):
        """Procesa un digest de membresía recibido de otro peer."""
        sender_peer = PeerInfo(
            peer_id=msg.header.sender_id,
            host=msg.header.sender_host,
            port=msg.header.sender_port,
            topics=msg.payload.get("topics", []),
            last_seen=time.time(),
            status=PeerStatus.ALIVE,
        )
        self.update_peer(sender_peer)

        # Procesar peers reportados en el payload
        peers_data = msg.payload.get("peers", [])
        for p_dict in peers_data:
            try:
                p_info = PeerInfo(**p_dict)
                self.update_peer(p_info)
            except Exception as e:
                logger.debug(f"Error procesando info de peer en digest: {e}")

    async def join_network(self, seed_endpoints: List[Tuple[str, int]]):
        """Envía mensaje de JOIN a los endpoints semilla (seeds) para integrarse a la malla."""
        for host, port in seed_endpoints:
            if host == self.host and port == self.port:
                continue
            logger.info(f"[{self.peer_id}] Contactando seed en {host}:{port}...")
            msg = CivicMessage.create_gossip_digest(
                sender_id=self.peer_id,
                sender_host=self.host,
                sender_port=self.port,
                known_peers=[self.self_info],
            )
            msg.payload["topics"] = list(self.topics)
            success = await self.transport.send_message(host, port, msg)
            if success:
                logger.info(f"[{self.peer_id}] Conectado exitosamente a seed {host}:{port}")

    async def start(self):
        """Inicia el bucle periódico de heartbeat / gossip."""
        self._running = True
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        logger.info(f"[{self.peer_id}] Servicio Gossip iniciado con política={self.policy}, fanout={self.fanout}")

    async def _gossip_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                alive = self.get_alive_peers()
                targets = self.select_gossip_targets()
                if targets:
                    digest_peers = [self.self_info] + alive[: self.partial_view_size - 1]
                    msg = CivicMessage.create_gossip_digest(
                        sender_id=self.peer_id,
                        sender_host=self.host,
                        sender_port=self.port,
                        known_peers=digest_peers,
                    )
                    msg.payload["topics"] = list(self.topics)
                    for target in targets:
                        await self.transport.send_message(target.host, target.port, msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.peer_id}] Error en ciclo gossip: {e}")

    async def stop(self):
        """Detiene el servicio Gossip."""
        self._running = False
        if self._gossip_task and not self._gossip_task.done():
            self._gossip_task.cancel()
        logger.info(f"[{self.peer_id}] Servicio Gossip detenido.")
