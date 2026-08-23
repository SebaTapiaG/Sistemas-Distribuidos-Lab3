"""
tests/integration/test_mesh_convergence.py
Test de integración multi-nodo: 3 peers + 1 publicador.
Verifica la propagación de eventos a través de la malla y convergencia del estado local.
"""

import asyncio
import os
import shutil
import tempfile
import pytest
from civicmesh.node.peer import PeerNode
from civicmesh.node.publisher import PublisherNode


@pytest.mark.asyncio
async def test_multi_peer_mesh_convergence():
    temp_dir = tempfile.mkdtemp(prefix="civicmesh_test_")
    run_id = "test_run_convergence"
    os.environ["CIVICMESH_RUNS"] = temp_dir

    try:
        # 1. Crear 3 peers
        p1 = PeerNode(peer_id="peer-1", host="127.0.0.1", port=0, topics=["Santiago"], run_id=run_id)
        p2 = PeerNode(peer_id="peer-2", host="127.0.0.1", port=0, topics=["Providencia", "Santiago"], run_id=run_id)
        p3 = PeerNode(peer_id="peer-3", host="127.0.0.1", port=0, topics=["Las Condes"], run_id=run_id)

        # Iniciar peers
        await p1.start()
        await p2.start()
        await p3.start()

        # Conectar en malla
        await p2.gossip.join_network([(p1.host, p1.port)])
        await p3.gossip.join_network([(p2.host, p2.port)])

        # Esperar descubrimiento inicial
        await asyncio.sleep(0.5)

        # 2. Crear Publicador para Dominio A (Delitos)
        pub = PublisherNode(
            publisher_id="pub-delitos-1",
            domain_type="delitos",
            run_id=run_id,
            interval_sec=0.2,
            max_steps=3,
        )

        # Ejecutar publicador
        pub_task = asyncio.create_task(pub.start())
        await pub_task

        # Permitir tiempo para reenvío y convergencia
        await asyncio.sleep(1.0)

        # 3. Verificaciones de convergencia
        # Peer 1 debe tener datos registrados para Santiago
        st_p1 = p1.state.get_or_create_topic("Santiago")
        assert st_p1.objective_events_count > 0

        # Peer 2 está suscrito a Santiago, debe haber recibido los eventos también
        st_p2 = p2.state.get_or_create_topic("Santiago")
        assert st_p2.objective_events_count > 0

        # Verificar que el publicador registró métricas en el shared FS
        metrics_file_p1 = p1.metrics_logger.metrics_dir / "peer-1.jsonl"
        assert metrics_file_p1.exists()

        # Detener nodos
        await p1.stop()
        await p2.stop()
        await p3.stop()
        await pub.stop()

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
