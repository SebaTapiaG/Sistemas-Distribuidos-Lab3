"""
tests/unit/test_domain_b.py
Pruebas unitarias para el Dominio B (Calidad del Aire, Replay, IDW y Percepción).
Valida reproducibilidad, interpolación espacial y exactitud de las fórmulas (4)-(5).
"""

import pytest
from civicmesh.domains.domain_b_aire import CalidadAireDomain, haversine_distance_km


def test_haversine_distance():
    """Valida el cálculo de distancia geográfica entre dos puntos conocidos."""
    # Distancia aproximada Santiago Centro (-33.4489, -70.6693) a Las Condes (-33.4117, -70.5800) ~ 9.2 km
    dist = haversine_distance_km(-33.4489, -70.6693, -33.4117, -70.5800)
    assert 8.0 < dist < 11.0


def test_domain_b_mathematical_formulas():
    """Valida el cálculo de memoria de picos y percepción en fórmulas (4) y (5)."""
    comunas_data = {
        "comunas": {
            "Santiago": {"lat": -33.4489, "lon": -70.6693},
        }
    }
    config = {
        "estaciones_ancla": ["Santiago"],
        "subjective": {
            "alpha": 0.8,
            "gamma": 0.5,
            "delta": 0.2,
            "sigma_eps": 0.0,
            "rumor_summary": "mean",
            "clip_min": 0.0,
            "clip_max": 500.0,
        },
    }
    dom = CalidadAireDomain(config=config, comunas_data=comunas_data, seed=42)

    # Paso 1: Mc(0) = 0. Supongamos vc(1) = 30.0, rumores Q = [40.0]
    # uc(1) = max(30.0, 0.0) = 30.0
    # Mc(1) = 0.8 * 0 + 0.2 * 30.0 = 6.0
    # Pc(1) = 30.0 + 0.5 * (6.0 - 30.0) + 0.2 * 40.0 + 0.0 = 30.0 - 12.0 + 8.0 = 26.0
    p_1, m_1, u_1 = dom.calculate_subjective_perception("Santiago", vc_t=30.0, gossip_rumors=[40.0])

    assert u_1 == pytest.approx(30.0)
    assert m_1 == pytest.approx(6.0)
    assert p_1 == pytest.approx(26.0)

    # Paso 2: La calidad real baja súbitamente a vc(2) = 5.0 (aire limpio), pero queda memoria del pico anterior!
    # uc(2) = max(5.0, Mc(1)) = max(5.0, 6.0) = 6.0
    # Mc(2) = 0.8 * 6.0 + 0.2 * 6.0 = 6.0
    # Pc(2) = 5.0 + 0.5 * (6.0 - 5.0) + 0.2 * 0.0 = 5.0 + 0.5 = 5.5
    p_2, m_2, u_2 = dom.calculate_subjective_perception("Santiago", vc_t=5.0, gossip_rumors=[])

    assert u_2 == pytest.approx(6.0)
    assert m_2 == pytest.approx(6.0)
    assert p_2 == pytest.approx(5.5)


def test_spatial_idw_extrapolation():
    """Valida que una comuna intermedia obtenga un valor interpolado coherente por IDW."""
    comunas_data = {
        "comunas": {
            "Santiago": {"lat": -33.4489, "lon": -70.6693}, # Estación A
            "Las Condes": {"lat": -33.4117, "lon": -70.5800}, # Estación B
            "Providencia": {"lat": -33.4314, "lon": -70.6093}, # Comuna intermedia sin estación
        }
    }
    config = {
        "estaciones_ancla": ["Santiago", "Las Condes"],
        "extrapolacion": {"metodo": "idw", "power_p": 2.0},
    }
    dom = CalidadAireDomain(config=config, comunas_data=comunas_data, seed=42)
    # Sobrescribir datos de estaciones
    dom.series_data = {
        "Santiago": [{"pm2_5": 50.0}],
        "Las Condes": [{"pm2_5": 10.0}],
    }

    val_prov = dom.get_objective_value("Providencia", step_idx=0)
    # El valor interpolado debe estar estrictamente entre 10.0 y 50.0
    assert 10.0 < val_prov < 50.0
