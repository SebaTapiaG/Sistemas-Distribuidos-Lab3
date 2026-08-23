"""
civicmesh.core.pubsub
Capa de Publish/Subscribe geográfico con enrutamiento explícito anti-flooding,
función should_forward(), deduplicación y gestión de TTL/prioridad por canal.
"""

import collections
import logging
import time
from typing import Dict, List, Optional, Set, Tuple
from civicmesh.core.protocol import CivicMessage, PeerInfo
from civicmesh.core.network import AsyncNetworkTransport

logger = logging.getLogger("civicmesh.pubsub")


class PubSubManager:
    def __init__(
        self,
        peer_id: str,
        transport: AsyncNetworkTransport,
        subscribed_topics: List[str],
        comunas_data: Optional[Dict] = None,
        dedup_cache_size: int = 2000,
        dedup_ttl_sec: float = 30.0,
        channel_configs: Optional[Dict] = None,
    ):
        self.peer_id = peer_id
        self.transport = transport
        self.subscribed_topics: Set[str] = set(subscribed_topics)
        self.comunas_data = comunas_data or {}
        self.dedup_cache_size = dedup_cache_size
        self.dedup_ttl_sec = dedup_ttl_sec

        # Configuraciones por canal (TTL por defecto, prioridad, fanout)
        self.channel_configs = channel_configs or {
            "objective": {"ttl": 4, "priority": 1, "fanout": 3},
            "subjective": {"ttl": 3, "priority": 2, "fanout": 2},
        }

        # Cache de deduplicación: {msg_id: timestamp_visto}
        self.seen_messages: collections.OrderedDict[str, float] = collections.OrderedDict()

        # Métricas de tráfico pub/sub
        self.stats = {
            "messages_received": 0,
            "messages_forwarded": 0,
            "messages_dropped_duplicate": 0,
            "messages_dropped_ttl": 0,
            "messages_dropped_no_interest": 0,
        }

    def is_subscribed_to(self, topic: str) -> bool:
        return topic in self.subscribed_topics

    def subscribe(self, topic: str):
        self.subscribed_topics.add(topic)

    def unsubscribe(self, topic: str):
        self.subscribed_topics.discard(topic)

    def is_duplicate(self, msg_id: str) -> bool:
        """Verifica y limpia la cache de mensajes duplicados."""
        now = time.time()
        if msg_id in self.seen_messages:
            return True

        # Limpiar elementos viejos si se supera el tamaño
        if len(self.seen_messages) >= self.dedup_cache_size:
            self.seen_messages.popitem(last=False)

        self.seen_messages[msg_id] = now
        return False

    def is_topic_interested_or_neighbor(self, peer_topics: List[str], target_topic: str) -> bool:
        """Determina si un peer remoto tiene interés en un tópico o en comunas adyacentes."""
        if not target_topic:
            return False
        if target_topic in peer_topics:
            return True

        # Verificar si el peer está suscrito a comunas vecinas en el grafo
        comunas = self.comunas_data.get("comunas", {})
        target_meta = comunas.get(target_topic, {})
        vecinos = set(target_meta.get("vecinos", []))
        return bool(vecinos.intersection(set(peer_topics)))

    def should_forward(self, msg: CivicMessage, topic: str, local_view: List[PeerInfo]) -> bool:
        """
        Función explícita de decisión de reenvío:
        1. TTL > 1 (debe quedar vida útil para que el próximo salto lo reciba).
        2. No haber visto el mensaje previamente (anti-bucles / deduplicación).
        3. Existencia de peers en local_view con interés geográfico o topológico relevante.
        4. Prioridad y reglas del canal.
        """
        # 1. Regla de TTL
        if msg.header.ttl <= 1:
            self.stats["messages_dropped_ttl"] += 1
            logger.debug(f"[{self.peer_id}] Drop msg {msg.header.msg_id}: TTL agotado ({msg.header.ttl})")
            return False

        # 2. Regla de duplicados
        if self.is_duplicate(msg.header.msg_id):
            self.stats["messages_dropped_duplicate"] += 1
            logger.debug(f"[{self.peer_id}] Drop msg {msg.header.msg_id}: Mensaje duplicado")
            return False

        # 3. Regla de vecinos y cobertura de interés
        if not local_view:
            self.stats["messages_dropped_no_interest"] += 1
            return False

        # Filtrar candidatos que no sean el emisor directo
        candidates = [p for p in local_view if p.peer_id != msg.header.sender_id]
        if not candidates:
            return False

        # Si el canal es de alta prioridad (objetivo ground-truth), permitimos reenvío para difusión amplia
        if msg.header.priority == 1:
            return True

        # Para prioridad normal/baja, exigimos que al menos un vecino tenga interés o vecindad
        has_interested_neighbor = any(self.is_topic_interested_or_neighbor(p.topics, topic) for p in candidates)
        if not has_interested_neighbor:
            self.stats["messages_dropped_no_interest"] += 1
            logger.debug(f"[{self.peer_id}] Drop msg {msg.header.msg_id}: Sin vecinos interesados en {topic}")
            return False

        return True

    def select_forward_targets(
        self,
        msg: CivicMessage,
        topic: str,
        local_view: List[PeerInfo],
    ) -> List[PeerInfo]:
        """Selecciona los peers de la vista local a los cuales reenviar el mensaje según el fanout del canal."""
        candidates = [p for p in local_view if p.peer_id != msg.header.sender_id]
        if not candidates:
            return []

        channel = msg.header.channel or "objective"
        channel_cfg = self.channel_configs.get(channel, {"fanout": 2})
        target_fanout = channel_cfg.get("fanout", 2)

        # Ordenar candidatos: primero los directamente suscritos al tópico, luego vecinos, luego resto
        comunas = self.comunas_data.get("comunas", {})
        vecinos = set(comunas.get(topic, {}).get("vecinos", []))

        def candidate_score(p: PeerInfo) -> int:
            if topic in p.topics:
                return 3
            if vecinos.intersection(set(p.topics)):
                return 2
            return 1

        candidates.sort(key=candidate_score, reverse=True)
        return candidates[:target_fanout]

    async def forward_message(
        self,
        msg: CivicMessage,
        topic: str,
        local_view: List[PeerInfo],
    ) -> int:
        """Reenvía el mensaje a los destinos seleccionados con TTL decrementado y hop_count incrementado."""
        targets = self.select_forward_targets(msg, topic, local_view)
        if not targets:
            return 0

        # Crear copia mutada del mensaje para el salto siguiente
        forward_msg = msg.model_copy(deep=True)
        forward_msg.header.ttl -= 1
        forward_msg.header.hop_count += 1
        # Actualizamos sender inmediato para evitar rebote inmediato al nodo actual
        forward_msg.header.sender_id = self.peer_id
        forward_msg.header.sender_host = self.transport.host
        forward_msg.header.sender_port = self.transport.port

        endpoints = [(t.host, t.port) for t in targets]
        sent_count = await self.transport.broadcast_message(endpoints, forward_msg)
        self.stats["messages_forwarded"] += sent_count
        return sent_count
