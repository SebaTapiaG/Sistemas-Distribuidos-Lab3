"""
tests/integration/test_peer_failure.py
Test de integración para tolerancia a fallos y caída de peers en la malla.
"""

import asyncio
import os
import shutil
import tempfile
import pytest
from civicmesh.node.peer import PeerNode


@pytest.mark.asyncio
async def test_peer_failure_detection_and_resilience():
    temp_dir = tempfile.mkdtemp(prefix="civicmesh_fail_test_")
    run_id = "test_run_failure"
    os.environ["CIVICMESH_RUNS"] = temp_dir

    try:
        # Configurar timeout corto para acelerar el test
        p1 = PeerNode(peer_id="p1", host="127.0.0.1", port=0, topics=["Santiago"], run_id=run_id)
        p2 = PeerNode(peer_id="p2", host="127.0.0.1", port=0, topics=["Providencia"], run_id=run_id)
        
        # Ajustar fail_timeout
        p1.gossip.fail_timeout = 1.0
        p2.gossip.fail_timeout = 1.0

        await p1.start()
        await p2.start()

        # Conectar p2 con p1
        await p2.gossip.join_network([(p1.host, p1.port)])
        await asyncio.sleep(0.5)

        # Verificar que p1 conoce a p2
        alive_p1 = [p.peer_id for p in p1.gossip.get_alive_peers()]
        assert "p2" in alive_p1

        # Simular caída abrupta de p2
        await p2.stop()

        # Esperar a que se supere el fail_timeout
        await asyncio.sleep(1.5)

        # p1 debe haber detectado la caída y expulsado a p2
        alive_after_fail = [p.peer_id for p in p1.gossip.get_alive_peers()]
        assert "p2" not in alive_after_fail

        await p1.stop()

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
