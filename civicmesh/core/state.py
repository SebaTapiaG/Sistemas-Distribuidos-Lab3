"""
civicmesh.core.state
Gestión del estado agregado local por tópico y canal para cada peer,
cálculo de brecha percepción-realidad y almacenamiento de métricas en memoria.
"""

import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TopicState(BaseModel):
    topic: str
    last_objective_val: float = 0.0
    last_objective_time: float = 0.0
    objective_events_count: int = 0
    objective_payload_history: List[Dict[str, Any]] = Field(default_factory=list)

    last_subjective_val: float = 0.0
    last_subjective_time: float = 0.0
    subjective_rumors_window: List[float] = Field(default_factory=list)
    rumor_aggregate: float = 0.0

    # Memoria interna si el peer actúa como publicador/evaluador
    ema_memory_m: float = 0.0

    # Métricas de propagación
    total_hops_objective: int = 0
    total_hops_subjective: int = 0


class PeerLocalState:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        # {topic_name: TopicState}
        self.topics_state: Dict[str, TopicState] = {}
        self.created_at = time.time()

    def get_or_create_topic(self, topic: str) -> TopicState:
        if topic not in self.topics_state:
            self.topics_state[topic] = TopicState(topic=topic)
        return self.topics_state[topic]

    def record_objective_event(self, topic: str, value: float, payload: Dict[str, Any], hop_count: int = 0):
        t_state = self.get_or_create_topic(topic)
        t_state.last_objective_val = value
        t_state.last_objective_time = time.time()
        t_state.objective_events_count += 1
        t_state.total_hops_objective += hop_count
        
        # Mantener historial corto para inspección
        t_state.objective_payload_history.append(payload)
        if len(t_state.objective_payload_history) > 20:
            t_state.objective_payload_history.pop(0)

    def record_subjective_rumor(self, topic: str, rumor_val: float, hop_count: int = 0):
        t_state = self.get_or_create_topic(topic)
        t_state.subjective_rumors_window.append(rumor_val)
        t_state.last_subjective_time = time.time()
        t_state.total_hops_subjective += hop_count
        
        # Mantener ventana deslizante de rumores
        if len(t_state.subjective_rumors_window) > 50:
            t_state.subjective_rumors_window.pop(0)

        # Actualizar promedio de rumores
        if t_state.subjective_rumors_window:
            t_state.rumor_aggregate = sum(t_state.subjective_rumors_window) / len(t_state.subjective_rumors_window)

    def set_subjective_perception(self, topic: str, perception_val: float, ema_val: float):
        t_state = self.get_or_create_topic(topic)
        t_state.last_subjective_val = perception_val
        t_state.ema_memory_m = ema_val
        t_state.last_subjective_time = time.time()

    def flush_rumors_window(self, topic: str) -> List[float]:
        """Extrae los rumores acumulados en el paso y limpia la ventana."""
        t_state = self.get_or_create_topic(topic)
        rumors = list(t_state.subjective_rumors_window)
        t_state.subjective_rumors_window.clear()
        return rumors

    def get_perception_gap(self, topic: str) -> float:
        """Calcula la brecha percepción - realidad: Pc(t) - Gc(t)."""
        t_state = self.get_or_create_topic(topic)
        return t_state.last_subjective_val - t_state.last_objective_val

    def get_snapshot(self) -> Dict[str, Any]:
        """Genera un snapshot serializable del estado del peer para métricas."""
        snapshot_topics = {}
        for t_name, t_state in self.topics_state.items():
            snapshot_topics[t_name] = {
                "objective_val": t_state.last_objective_val,
                "objective_events": t_state.objective_events_count,
                "subjective_val": t_state.last_subjective_val,
                "rumor_aggregate": t_state.rumor_aggregate,
                "ema_memory": t_state.ema_memory_m,
                "perception_gap": t_state.last_subjective_val - t_state.last_objective_val,
                "avg_hops_objective": (
                    t_state.total_hops_objective / t_state.objective_events_count
                    if t_state.objective_events_count > 0 else 0
                ),
            }

        return {
            "peer_id": self.peer_id,
            "timestamp": time.time(),
            "topics": snapshot_topics,
        }
