"""
civicmesh.domains.base
Interfaz base para los dominios de aplicación en CivicMesh.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple


class DomainPlugin(ABC):
    @abstractmethod
    def step(self, t: float, delta_t: float) -> List[Dict[str, Any]]:
        """
        Ejecuta un paso de simulación en el instante t con intervalo delta_t.
        Retorna una lista de eventos generados listos para publicar:
        [
            {
                "channel": "objective"|"subjective",
                "topic": "comuna_name",
                "value": float,
                "payload": {...}
            }, ...
        ]
        """
        pass

    @abstractmethod
    def get_topics(self) -> List[str]:
        """Retorna la lista de tópicos asociados a este dominio."""
        pass
