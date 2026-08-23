"""
tests/unit/test_gossip.py
Pruebas unitarias para la capa de protocolo, serialización y membresía Gossip.
"""

import asyncio
import time
import pytest
from civicmesh.core.protocol import CivicMessage, MessageType, PeerInfo, PeerStatus
from civicmesh.core.network import AsyncNetworkTransport
from civicmesh.core.gossip import GossipMembership


def test_protocol_message_serialization():
    """Valida la serialización y deserialización binaria/JSON de CivicMessage."""
    event = CivicMessage.create_event(
        sender_id="peer-1",
        sender_host="127.0.0.1",
        sender_port=8001,
        channel="objective",
        topic="Santiago",
        payload={"crime_type": "hurto", "count": 3},
        ttl=4,
        priority=1,
    )
    data_bytes = event.to_bytes()
    assert isinstance(data_bytes, bytes)
    assert data_bytes.endswith(b"\n")

    decoded = CivicMessage.from_bytes(data_bytes)
    assert decoded.header.sender_id == "peer-1"
    assert decoded.header.channel == "objective"
    assert decoded.header.topic == "Santiago"
    assert decoded.header.ttl == 4
    assert decoded.payload["crime_type"] == "hurto"
    assert decoded.payload["count"] == 3


def test_gossip_partial_view_capacity():
    """Valida que la vista parcial respete el tamaño máximo expulsando el peer más antiguo."""
    transport = AsyncNetworkTransport()
    gossip = GossipMembership(
        peer_id="self-peer",
        host="127.0.0.1",
        port=8000,
        topics=["Santiago"],
        transport=transport,
        partial_view_size=3,
    )

    now = time.time()
    # Insertar 3 peers
    gossip.update_peer(PeerInfo(peer_id="p1", host="127.0.0.1", port=8001, topics=["Santiago"], last_seen=now - 10))
    gossip.update_peer(PeerInfo(peer_id="p2", host="127.0.0.1", port=8002, topics=["Providencia"], last_seen=now - 5))
    gossip.update_peer(PeerInfo(peer_id="p3", host="127.0.0.1", port=8003, topics=["Nunoa"], last_seen=now - 2))

    assert len(gossip.partial_view) == 3
    assert "p1" in gossip.partial_view

    # Insertar un 4to peer: debe expulsar a p1 (el más antiguo)
    gossip.update_peer(PeerInfo(peer_id="p4", host="127.0.0.1", port=8004, topics=["Las Condes"], last_seen=now))

    assert len(gossip.partial_view) == 3
    assert "p1" not in gossip.partial_view
    assert "p4" in gossip.partial_view


def test_gossip_failure_detection():
    """Valida la detección de caídas por timeout."""
    transport = AsyncNetworkTransport()
    gossip = GossipMembership(
        peer_id="self-peer",
        host="127.0.0.1",
        port=8000,
        topics=["Santiago"],
        transport=transport,
        fail_timeout=2.0,
    )

    now = time.time()
    gossip.update_peer(PeerInfo(peer_id="alive-peer", host="127.0.0.1", port=8001, topics=["Santiago"], last_seen=now))
    gossip.update_peer(PeerInfo(peer_id="dead-peer", host="127.0.0.1", port=8002, topics=["Providencia"], last_seen=now - 3.0))

    alive = gossip.get_alive_peers()
    assert len(alive) == 1
    assert alive[0].peer_id == "alive-peer"
    assert "dead-peer" not in gossip.partial_view


def test_gossip_policy_selection():
    """Valida la selección de targets bajo política sesgada por tópico y aleatoria."""
    transport = AsyncNetworkTransport()
    comunas_data = {
        "comunas": {
            "Santiago": {"vecinos": ["Providencia", "Nunoa"]},
            "Providencia": {"vecinos": ["Santiago"]},
            "Puente Alto": {"vecinos": []},
        }
    }
    gossip = GossipMembership(
        peer_id="peer-stgo",
        host="127.0.0.1",
        port=8000,
        topics=["Santiago"],
        transport=transport,
        policy="topic_biased",
        comunas_data=comunas_data,
        seed=123,
    )

    gossip.update_peer(PeerInfo(peer_id="p-prov", host="127.0.0.1", port=8001, topics=["Providencia"]))
    gossip.update_peer(PeerInfo(peer_id="p-pa", host="127.0.0.1", port=8002, topics=["Puente Alto"]))

    targets = gossip.select_gossip_targets(k=1)
    assert len(targets) == 1
    # Providencia es vecina de Santiago, por lo que debe tener prioridad en topic_biased
    assert targets[0].peer_id == "p-prov"


@pytest.mark.asyncio
async def test_async_network_communication():
    """Prueba la comunicación real TCP asíncrona entre dos instancias en localhost."""
    received = []

    async def server_handler(msg: CivicMessage):
        received.append(msg)

    server_transport = AsyncNetworkTransport(host="127.0.0.1", port=0)
    host, port = await server_transport.start(server_handler)

    client_transport = AsyncNetworkTransport()
    msg = CivicMessage.create_ping(sender_id="client-1", sender_host="127.0.0.1", sender_port=9999)

    success = await client_transport.send_message(host, port, msg)
    assert success is True

    await asyncio.sleep(0.1)
    assert len(received) == 1
    assert received[0].header.sender_id == "client-1"
    assert received[0].header.msg_type == MessageType.PING

    await server_transport.stop()
