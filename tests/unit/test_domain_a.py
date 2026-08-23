"""
tests/unit/test_domain_a.py
Pruebas unitarias para el Dominio A (Delitos y Percepción de Inseguridad).
Valida reproducibilidad con --seed y exactitud numérica de las ecuaciones (1)-(3).
"""

import pytest
import math
from civicmesh.domains.domain_a_delitos import DelitosDomain


def test_domain_a_seed_reproducibility():
    """Valida que dos instancias con la misma semilla generen exactamente la misma secuencia."""
    config = {
        "crime_types": ["hurto", "robo_con_violencia"],
        "comuna_lambdas": {
            "Santiago": {"hurto": 2.0, "robo_con_violencia": 1.0},
        },
        "subjective": {"alpha": 0.8, "beta_0": -1.0, "beta_1": 0.4, "beta_2": 0.8, "sigma_eps": 0.0},
    }

    dom1 = DelitosDomain(config=config, seed=999)
    dom2 = DelitosDomain(config=config, seed=999)

    events1 = dom1.step(t=1.0, delta_t=1.0)
    events2 = dom2.step(t=1.0, delta_t=1.0)

    assert len(events1) == len(events2)
    for e1, e2 in zip(events1, events2):
        assert e1["topic"] == e2["topic"]
        assert e1["channel"] == e2["channel"]
        assert e1["value"] == pytest.approx(e2["value"])


def test_domain_a_mathematical_formulas():
    """Valida el cálculo numérico exacto de Mc(t), Zc(t) y Pc(t) sin ruido (sigma_eps=0)."""
    config = {
        "crime_types": ["hurto"],
        "comuna_lambdas": {"Santiago": {"hurto": 1.0}},
        "subjective": {
            "alpha": 0.8,
            "beta_0": -1.0,
            "beta_1": 0.5,
            "beta_2": 0.4,
            "sigma_eps": 0.0,
            "rumor_summary": "mean",
        },
    }
    dom = DelitosDomain(config=config, seed=42)

    # Paso 1: Mc(0) = 0. Supongamos Rc(1) = 4, rumores Q = [0.5, 0.7] (promedio = 0.6)
    # Mc(1) = 0.8 * 0 + 0.2 * 4 = 0.8
    # Zc(1) = -1.0 + 0.5 * 0.8 + 0.4 * 0.6 + 0.0 = -1.0 + 0.4 + 0.24 = -0.36
    # Pc(1) = 1 / (1 + exp(0.36)) = 1 / (1 + 1.433329) = 0.410959
    p_1, m_1, z_1 = dom.calculate_subjective_insecurity("Santiago", rc_t=4.0, gossip_rumors=[0.5, 0.7])

    assert m_1 == pytest.approx(0.8, abs=1e-5)
    assert z_1 == pytest.approx(-0.36, abs=1e-5)
    expected_p1 = 1.0 / (1.0 + math.exp(0.36))
    assert p_1 == pytest.approx(expected_p1, abs=1e-5)

    # Paso 2: Mc(1) = 0.8. Supongamos Rc(2) = 10, sin rumores (Q = [])
    # Mc(2) = 0.8 * 0.8 + 0.2 * 10 = 0.64 + 2.0 = 2.64
    # Zc(2) = -1.0 + 0.5 * 2.64 + 0.4 * 0.0 = -1.0 + 1.32 = 0.32
    # Pc(2) = 1 / (1 + exp(-0.32)) = 1 / (1 + 0.726149) = 0.579324
    p_2, m_2, z_2 = dom.calculate_subjective_insecurity("Santiago", rc_t=10.0, gossip_rumors=[])

    assert m_2 == pytest.approx(2.64, abs=1e-5)
    assert z_2 == pytest.approx(0.32, abs=1e-5)
    expected_p2 = 1.0 / (1.0 + math.exp(-0.32))
    assert p_2 == pytest.approx(expected_p2, abs=1e-5)
