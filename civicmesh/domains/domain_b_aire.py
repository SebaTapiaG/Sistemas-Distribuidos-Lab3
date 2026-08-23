"""
civicmesh.domains.domain_b_aire
Implementación del Dominio B: Calidad del Aire (Medición vs Percepción).
Replay de series reales continuas, interpolación espacial (IDW / vecinos)
y modelo de percepción ciudadana con retención de picos (ecuaciones 4 y 5).
"""

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from civicmesh.domains.base import DomainPlugin


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica en kilómetros entre dos coordenadas usando Haversine."""
    R = 6371.0 # Radio medio de la Tierra en km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return max(0.001, R * c)


class CalidadAireDomain(DomainPlugin):
    def __init__(
        self,
        config: Dict[str, Any],
        comunas_data: Dict[str, Any],
        seed: int = 42,
    ):
        self.config = config
        self.comunas_data = comunas_data
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        self.estaciones_ancla = config.get("estaciones_ancla", ["Santiago", "Las Condes", "La Florida"])
        self.extrapolacion_metodo = config.get("extrapolacion", {}).get("metodo", "idw")
        self.idw_p = float(config.get("extrapolacion", {}).get("power_p", 2.0))
        
        # Parámetros del modelo subjetivo
        sub_cfg = config.get("subjective", {})
        self.alpha = float(sub_cfg.get("alpha", 0.85))
        self.gamma = float(sub_cfg.get("gamma", 0.6))
        self.delta = float(sub_cfg.get("delta", 0.3))
        self.sigma_eps = float(sub_cfg.get("sigma_eps", 2.0))
        self.rumor_summary = sub_cfg.get("rumor_summary", "mean")
        self.clip_min = float(sub_cfg.get("clip_min", 0.0))
        self.clip_max = float(sub_cfg.get("clip_max", 500.0))

        # Dataset de series reales cacheadas: {comuna: [{"time": ..., "pm2_5": float, "pm10": float}, ...]}
        self.series_data: Dict[str, List[Dict[str, Any]]] = {}
        self.current_index: int = 0

        # Memoria interna EMA Mc(t)
        all_comunas = list(comunas_data.get("comunas", {}).keys())
        self.memory_m: Dict[str, float] = {c: 0.0 for c in all_comunas}

        self._load_cache_data()

    def get_topics(self) -> List[str]:
        return list(self.comunas_data.get("comunas", {}).keys())

    def _load_cache_data(self):
        """Carga el dataset de series reales desde el archivo de cache o genera un baseline realista."""
        cache_path_str = self.config.get("cache_file", "data/air_quality_cache.json")
        cache_file = Path(cache_path_str)

        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    self.series_data = json.load(f)
                return
            except Exception:
                pass

        # Generador de respaldo con patrón diurno y estacional realista para Santiago si el archivo no existe
        self._generate_synthetic_baseline_cache()

    def _generate_synthetic_baseline_cache(self, hours: int = 240):
        """Genera un dataset base con correlación espacial para pruebas offline."""
        data = {}
        for est in self.estaciones_ancla:
            records = []
            base_pm25 = 22.0 if est == "Santiago" else (14.0 if est == "Las Condes" else 18.0)
            for h in range(hours):
                # Ciclo diario (picos a las 8am y 21pm)
                hour_of_day = h % 24
                diurnal = 8.0 * math.sin((hour_of_day - 6) * math.pi / 12.0)
                noise = self.rng.normal(0.0, 2.5)
                val_pm25 = max(3.0, base_pm25 + diurnal + noise)
                val_pm10 = val_pm25 * 1.8 + max(0.0, self.rng.normal(0.0, 3.0))
                records.append({
                    "step": h,
                    "pm2_5": round(val_pm25, 2),
                    "pm10": round(val_pm10, 2),
                })
            data[est] = records
        self.series_data = data

    def get_objective_value(self, comuna: str, step_idx: int) -> float:
        """
        Obtiene el valor objetivo vc(t) en PM2.5. Si la comuna posee estación ancla,
        retorna el dato de la serie; si no, aplica interpolación espacial IDW.
        """
        if comuna in self.series_data and self.series_data[comuna]:
            records = self.series_data[comuna]
            rec = records[step_idx % len(records)]
            return float(rec.get("pm2_5", 15.0))

        # Extrapolación espacial para comunas sin estación propia
        return self._extrapolate_spatial_value(comuna, step_idx)

    def _extrapolate_spatial_value(self, comuna: str, step_idx: int) -> float:
        """Aplica IDW (Inverse Distance Weighting) sobre las estaciones ancla disponibles."""
        comunas_dict = self.comunas_data.get("comunas", {})
        target_meta = comunas_dict.get(comuna, {})
        lat_c, lon_c = target_meta.get("lat", -33.45), target_meta.get("lon", -70.66)

        anchors_with_data = [s for s in self.estaciones_ancla if s in self.series_data and self.series_data[s]]
        if not anchors_with_data:
            return 15.0 # Fallback general

        if self.extrapolacion_metodo == "nearest":
            # Vecino más cercano
            best_s = min(
                anchors_with_data,
                key=lambda s: haversine_distance_km(
                    lat_c, lon_c,
                    comunas_dict.get(s, {}).get("lat", lat_c),
                    comunas_dict.get(s, {}).get("lon", lon_c),
                )
            )
            records = self.series_data[best_s]
            return float(records[step_idx % len(records)].get("pm2_5", 15.0))

        # IDW standard
        sum_weights = 0.0
        sum_weighted_vals = 0.0

        for s in anchors_with_data:
            s_meta = comunas_dict.get(s, {})
            lat_s, lon_s = s_meta.get("lat", lat_c), s_meta.get("lon", lon_c)
            dist_km = haversine_distance_km(lat_c, lon_c, lat_s, lon_s)

            if dist_km < 0.01: # Mismo punto exacto
                records = self.series_data[s]
                return float(records[step_idx % len(records)].get("pm2_5", 15.0))

            w = 1.0 / (dist_km ** self.idw_p)
            records = self.series_data[s]
            val_s = float(records[step_idx % len(records)].get("pm2_5", 15.0))

            sum_weights += w
            sum_weighted_vals += w * val_s

        if sum_weights == 0:
            return 15.0
        return sum_weighted_vals / sum_weights

    def calculate_subjective_perception(
        self,
        comuna: str,
        vc_t: float,
        gossip_rumors: List[float],
    ) -> Tuple[float, float, float]:
        """
        Calcula la percepción ciudadana Pc(t) en ug/m3 según las ecuaciones (4)-(5):
        uc(t) = max(vc(t), Mc(t - dt))
        (4) Mc(t) = alpha * Mc(t - dt) + (1 - alpha) * uc(t)
        (5) Pc(t) = vc(t) + gamma * (Mc(t) - vc(t)) + delta * P_hat_gossip(t) + eps_c(t)
        Retorna (Pc, Mc, uc).
        """
        prev_m = self.memory_m.get(comuna, 0.0)

        # Estímulo con memoria de pico
        uc_t = max(vc_t, prev_m)

        # Ecuación (4): Memoria EMA de picos
        mc_t = self.alpha * prev_m + (1.0 - self.alpha) * uc_t
        self.memory_m[comuna] = mc_t

        # Resumen de rumores de gossip
        if not gossip_rumors:
            p_hat_gossip = 0.0
        elif self.rumor_summary == "max":
            p_hat_gossip = max(gossip_rumors)
        else:
            p_hat_gossip = float(np.mean(gossip_rumors))

        # Ruido gaussiano
        eps = float(self.rng.normal(0.0, self.sigma_eps)) if self.sigma_eps > 0 else 0.0

        # Ecuación (5)
        pc_raw = vc_t + self.gamma * (mc_t - vc_t) + (self.delta * p_hat_gossip) + eps

        # Aplicar clip a rango físico
        pc_clipped = float(np.clip(pc_raw, self.clip_min, self.clip_max))

        return pc_clipped, mc_t, uc_t

    def get_who_category(self, pm2_5_val: float) -> str:
        """Clasifica el nivel de PM2.5 según guías OMS 2021."""
        if pm2_5_val <= 5.0:
            return "Buena (<= 5 ug/m3)"
        if pm2_5_val <= 15.0:
            return "Moderada (<= 15 ug/m3)"
        if pm2_5_val <= 25.0:
            return "Dañina grupos sensibles (<= 25 ug/m3)"
        if pm2_5_val <= 50.0:
            return "Dañina (<= 50 ug/m3)"
        if pm2_5_val <= 150.0:
            return "Muy Dañina (<= 150 ug/m3)"
        return "Peligrosa (> 150 ug/m3)"

    def step(
        self,
        t: float,
        delta_t: float,
        incoming_rumors_by_topic: Optional[Dict[str, List[float]]] = None,
    ) -> List[Dict[str, Any]]:
        events = []
        incoming_rumors = incoming_rumors_by_topic or {}

        for comuna in self.get_topics():
            # 1. Canal Objetivo (Replay real o IDW)
            vc = self.get_objective_value(comuna, self.current_index)
            is_anchor = comuna in self.estaciones_ancla
            who_cat = self.get_who_category(vc)

            events.append({
                "channel": "objective",
                "topic": comuna,
                "value": round(vc, 2),
                "payload": {
                    "timestamp": t,
                    "comuna": comuna,
                    "pm2_5": round(vc, 2),
                    "is_station_anchor": is_anchor,
                    "who_2021_category": who_cat,
                },
            })

            # 2. Canal Subjetivo
            rumors = incoming_rumors.get(comuna, [])
            pc, mc, uc = self.calculate_subjective_perception(comuna, vc, rumors)
            events.append({
                "channel": "subjective",
                "topic": comuna,
                "value": round(pc, 2),
                "payload": {
                    "timestamp": t,
                    "comuna": comuna,
                    "perceived_pm2_5": round(pc, 2),
                    "peak_stimulus": round(uc, 2),
                    "ema_memory": round(mc, 2),
                    "rumors_count": len(rumors),
                },
            })

        self.current_index += 1
        return events
