"""
civicmesh.core.protocol
Definición de tipos de mensaje, estructuras de cabecera y serialización para CivicMesh.
"""

from enum import Enum
import json
import time
import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class MessageType(str, Enum):
    # Mensajes de control de membresía (Gossip)
    JOIN = "JOIN"
    JOIN_ACK = "JOIN_ACK"
    PING = "PING"
    ACK = "ACK"
    LEAVE = "LEAVE"
    GOSSIP_DIGEST = "GOSSIP_DIGEST"

    # Mensajes de datos (Pub/Sub)
    PUB = "PUB"
    SUB = "SUB"
    UNSUB = "UNSUB"


class ChannelType(str, Enum):
    OBJECTIVE = "objective"
    SUBJECTIVE = "subjective"


class PeerStatus(str, Enum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


class PeerInfo(BaseModel):
    peer_id: str
    host: str
    port: int
    topics: List[str] = Field(default_factory=list)
    last_seen: float = Field(default_factory=time.time)
    status: PeerStatus = PeerStatus.ALIVE

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


class MessageHeader(BaseModel):
    msg_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    msg_type: MessageType
    sender_id: str
    sender_host: str
    sender_port: int
    channel: Optional[str] = None # "objective", "subjective", o None para control
    topic: Optional[str] = None   # Comuna o tópico geográfico
    ttl: int = 5                  # Hops restantes permitidos
    priority: int = 2             # 1=Alta, 2=Normal, 3=Baja
    hop_count: int = 0            # Número de saltos transitados
    timestamp: float = Field(default_factory=time.time)


class CivicMessage(BaseModel):
    header: MessageHeader
    payload: Dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        return self.model_dump_json()

    def to_bytes(self) -> bytes:
        # Delimitamos con newline '\n' para framing seguro sobre streams TCP
        return (self.to_json() + "\n").encode("utf-8")

    @classmethod
    def from_json(cls, json_str: str) -> "CivicMessage":
        return cls.model_validate_json(json_str)

    @classmethod
    def from_bytes(cls, data: bytes) -> "CivicMessage":
        return cls.from_json(data.decode("utf-8").strip())

    @classmethod
    def create_event(
        cls,
        sender_id: str,
        sender_host: str,
        sender_port: int,
        channel: str,
        topic: str,
        payload: Dict[str, Any],
        ttl: int = 4,
        priority: int = 2,
    ) -> "CivicMessage":
        """Crea un mensaje de evento de Pub/Sub (canal objetivo o subjetivo)."""
        header = MessageHeader(
            msg_type=MessageType.PUB,
            sender_id=sender_id,
            sender_host=sender_host,
            sender_port=sender_port,
            channel=channel,
            topic=topic,
            ttl=ttl,
            priority=priority,
            hop_count=0,
            timestamp=time.time(),
        )
        return cls(header=header, payload=payload)

    @classmethod
    def create_gossip_digest(
        cls,
        sender_id: str,
        sender_host: str,
        sender_port: int,
        known_peers: List[PeerInfo],
    ) -> "CivicMessage":
        """Crea un mensaje digest de membresía Gossip."""
        header = MessageHeader(
            msg_type=MessageType.GOSSIP_DIGEST,
            sender_id=sender_id,
            sender_host=sender_host,
            sender_port=sender_port,
            channel=None,
            topic=None,
            ttl=1, # Membresía directa
            priority=1,
            hop_count=0,
            timestamp=time.time(),
        )
        payload = {"peers": [p.model_dump() for p in known_peers]}
        return cls(header=header, payload=payload)

    @classmethod
    def create_ping(
        cls,
        sender_id: str,
        sender_host: str,
        sender_port: int,
    ) -> "CivicMessage":
        header = MessageHeader(
            msg_type=MessageType.PING,
            sender_id=sender_id,
            sender_host=sender_host,
            sender_port=sender_port,
            ttl=1,
            priority=1,
            hop_count=0,
            timestamp=time.time(),
        )
        return cls(header=header, payload={})

    @classmethod
    def create_ack(
        cls,
        sender_id: str,
        sender_host: str,
        sender_port: int,
        target_msg_id: str,
    ) -> "CivicMessage":
        header = MessageHeader(
            msg_type=MessageType.ACK,
            sender_id=sender_id,
            sender_host=sender_host,
            sender_port=sender_port,
            ttl=1,
            priority=1,
            hop_count=0,
            timestamp=time.time(),
        )
        return cls(header=header, payload={"target_msg_id": target_msg_id})
