"""
tests/unit/test_pubsub.py
Pruebas unitarias para la capa PubSub, should_forward, TTL, deduplicación y estado local.
"""

import pytest
from civicmesh.core.protocol import CivicMessage, PeerInfo
from civicmesh.core.network import AsyncNetworkTransport
from civicmesh.core.pubsub import PubSubManager
from civicmesh.core.state import PeerLocalState


def test_should_forward_ttl_and_deduplication():
    """Valida que should_forward descarte mensajes con TTL agotado o duplicados."""
    transport = AsyncNetworkTransport()
    pubsub = PubSubManager(peer_id="peer-test", transport=transport, subscribed_topics=["Santiago"])
    
    local_view = [PeerInfo(peer_id="p-remote", host="127.0.0.1", port=9000, topics=["Santiago"])]

    # Mensaje normal con TTL=3
    msg = CivicMessage.create_event(
        sender_id="p-sender",
        sender_host="127.0.0.1",
        sender_port=8000,
        channel="objective",
        topic="Santiago",
        payload={"value": 10},
        ttl=3,
    )

    # 1era vez: debe aceptar el reenvío
    assert pubsub.should_forward(msg, "Santiago", local_view) is True

    # 2da vez (mismo msg_id): debe descartar por duplicado
    assert pubsub.should_forward(msg, "Santiago", local_view) is False

    # Mensaje con TTL=1 (al restarle 1 quedaría en 0, no debe reenviarse)
    msg_low_ttl = CivicMessage.create_event(
        sender_id="p-sender-2",
        sender_host="127.0.0.1",
        sender_port=8000,
        channel="objective",
        topic="Santiago",
        payload={"value": 5},
        ttl=1,
    )
    assert pubsub.should_forward(msg_low_ttl, "Santiago", local_view) is False


def test_should_forward_interest_and_priority():
    """Valida el comportamiento de should_forward según prioridad e interés de vecinos."""
    transport = AsyncNetworkTransport()
    comunas_data = {
        "comunas": {
            "Santiago": {"vecinos": ["Providencia"]},
            "Providencia": {"vecinos": ["Santiago"]},
            "Puente Alto": {"vecinos": []},
        }
    }
    pubsub = PubSubManager(
        peer_id="peer-test",
        transport=transport,
        subscribed_topics=["Santiago"],
        comunas_data=comunas_data,
    )

    # Vecinos sin interés en Puente Alto
    local_view_no_interest = [
        PeerInfo(peer_id="p-prov", host="127.0.0.1", port=9001, topics=["Providencia"])
    ]

    # Mensaje de prioridad 2 (subjetivo) hacia Puente Alto
    msg_sub = CivicMessage.create_event(
        sender_id="p-sender",
        sender_host="127.0.0.1",
        sender_port=8000,
        channel="subjective",
        topic="Puente Alto",
        payload={"value": 0.8},
        ttl=3,
        priority=2,
    )
    assert pubsub.should_forward(msg_sub, "Puente Alto", local_view_no_interest) is False

    # Mensaje de prioridad 1 (objetivo / ground truth): debe propagarse ampliamente
    msg_obj = CivicMessage.create_event(
        sender_id="p-sender",
        sender_host="127.0.0.1",
        sender_port=8000,
        channel="objective",
        topic="Puente Alto",
        payload={"value": 2},
        ttl=3,
        priority=1,
    )
    assert pubsub.should_forward(msg_obj, "Puente Alto", local_view_no_interest) is True


def test_peer_local_state_aggregation():
    """Valida el cálculo de estado local, brecha percepción-realidad y snapshots."""
    state = PeerLocalState(peer_id="peer-1")

    # Registrar evento objetivo
    state.record_objective_event(topic="Santiago", value=12.0, payload={"crime": "hurto"}, hop_count=2)
    # Registrar percepción subjetiva
    state.set_subjective_perception(topic="Santiago", perception_val=0.75, ema_val=0.6)

    # Registrar rumores
    state.record_subjective_rumor(topic="Santiago", rumor_val=0.8, hop_count=1)
    state.record_subjective_rumor(topic="Santiago", rumor_val=0.9, hop_count=2)

    topic_st = state.get_or_create_topic("Santiago")
    assert topic_st.last_objective_val == 12.0
    assert topic_st.objective_events_count == 1
    assert topic_st.last_subjective_val == 0.75
    assert topic_st.rumor_aggregate == pytest.approx(0.85) # (0.8 + 0.9) / 2

    # Brecha: 0.75 - 12.0 = -11.25
    assert state.get_perception_gap("Santiago") == pytest.approx(-11.25)

    snapshot = state.get_snapshot()
    assert snapshot["peer_id"] == "peer-1"
    assert "Santiago" in snapshot["topics"]
    assert snapshot["topics"]["Santiago"]["avg_hops_objective"] == 2.0
