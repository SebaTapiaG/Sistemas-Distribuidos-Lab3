"""
civicmesh.domains.domain_a_delitos
Implementación del Dominio A: Delitos (Percepción vs Realidad).
Generación estocástica de Poisson y modelo no-lineal de sensación de inseguridad.
"""

import math
import random
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from civicmesh.domains.base import DomainPlugin


class DelitosDomain(DomainPlugin):
    def __init__(
        self,
        config: Dict[str, Any],
        seed: int = 42,
    ):
        self.config = config
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        self.crime_types = config.get("crime_types", ["robo_con_violencia", "hurto", "homicidio"])
        self.comuna_lambdas = config.get("comuna_lambdas", {})

        # Parámetros del modelo subjetivo
        sub_cfg = config.get("subjective", {})
        self.alpha = float(sub_cfg.get("alpha", 0.8))
        self.beta_0 = float(sub_cfg.get("beta_0", -1.0))
        self.beta_1 = float(sub_cfg.get("beta_1", 0.4))
        self.beta_2 = float(sub_cfg.get("beta_2", 0.8))
        self.sigma_eps = float(sub_cfg.get("sigma_eps", 0.1))
        self.rumor_summary = sub_cfg.get("rumor_summary", "mean")

        # Estado de memoria EMA por comuna Mc(t)
        self.memory_m: Dict[str, float] = {c: 0.0 for c in self.comuna_lambdas.keys()}

    def get_topics(self) -> List[str]:
        return list(self.comuna_lambdas.keys())

    @staticmethod
    def sigmoid(z: float) -> float:
        """Función logística standard."""
        # Evitar overflow
        z = max(min(z, 50.0), -50.0)
        return 1.0 / (1.0 + math.exp(-z))

    def generate_objective_crimes(self, comuna: str, delta_t: float) -> Tuple[int, Dict[str, int]]:
        """
        Genera eventos de delitos según Poisson(lambda_{c,k} * delta_t) por cada tipo de delito.
        Retorna (total_delitos Rc, desglose_por_tipo).
        """
        lambdas = self.comuna_lambdas.get(comuna, {})
        counts_by_type = {}
        total_crimes = 0

        for c_type in self.crime_types:
            lam = lambdas.get(c_type, 0.5)
            # Muestra Poisson
            count = int(self.rng.poisson(lam * delta_t))
            counts_by_type[c_type] = count
            total_crimes += count

        return total_crimes, counts_by_type

    def calculate_subjective_insecurity(
        self,
        comuna: str,
        rc_t: float,
        gossip_rumors: List[float],
    ) -> Tuple[float, float, float]:
        """
        Calcula el índice de inseguridad Pc(t) in [0, 1] según las ecuaciones (1)-(3):
        (1) Mc(t) = alpha * Mc(t - dt) + (1 - alpha) * Rc(t)
        (2) Zc(t) = beta_0 + beta_1 * Mc(t) + beta_2 * P_hat_gossip(t) + eps_c(t)
        (3) Pc(t) = sigma(Zc(t))
        Retorna (Pc, Mc, Zc).
        """
        prev_m = self.memory_m.get(comuna, 0.0)
        # 1. Actualización de memoria EMA
        m_t = self.alpha * prev_m + (1.0 - self.alpha) * rc_t
        self.memory_m[comuna] = m_t

        # 2. Resumen de rumores recibidos
        if not gossip_rumors:
            p_hat_gossip = 0.0
        elif self.rumor_summary == "max":
            p_hat_gossip = max(gossip_rumors)
        else:
            p_hat_gossip = float(np.mean(gossip_rumors))

        # Ruido gaussiano
        eps = float(self.rng.normal(0.0, self.sigma_eps)) if self.sigma_eps > 0 else 0.0

        # Ecuación (2)
        z_t = self.beta_0 + (self.beta_1 * m_t) + (self.beta_2 * p_hat_gossip) + eps

        # Ecuación (3)
        p_t = self.sigmoid(z_t)

        return p_t, m_t, z_t

    def step(
        self,
        t: float,
        delta_t: float,
        incoming_rumors_by_topic: Optional[Dict[str, List[float]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta un ciclo de simulación para todas las comunas configuradas.
        Genera los mensajes objetivo y subjetivo correspondientes.
        """
        events = []
        incoming_rumors = incoming_rumors_by_topic or {}

        for comuna in self.get_topics():
            # 1. Canal Objetivo
            total_rc, breakdown = self.generate_objective_crimes(comuna, delta_t)
            events.append({
                "channel": "objective",
                "topic": comuna,
                "value": float(total_rc),
                "payload": {
                    "timestamp": t,
                    "comuna": comuna,
                    "total_crimes": total_rc,
                    "breakdown": breakdown,
                },
            })

            # 2. Canal Subjetivo
            rumors = incoming_rumors.get(comuna, [])
            pc, mc, zc = self.calculate_subjective_insecurity(comuna, total_rc, rumors)
            events.append({
                "channel": "subjective",
                "topic": comuna,
                "value": pc,
                "payload": {
                    "timestamp": t,
                    "comuna": comuna,
                    "insecurity_index": pc,
                    "ema_memory": mc,
                    "z_score": zc,
                    "rumors_count": len(rumors),
                },
            })

        return events
