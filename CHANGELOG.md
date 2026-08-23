# Changelog

Todos los cambios notables en el proyecto **CivicMesh** serán documentados en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/),
y este proyecto adhiere a [Semantic Versioning](https://semver.org/lang/es/).

## [v1.0.0-lab3] - 2026-08-23

### Added
- **Core Network & Protocol**: Capa de serialización de mensajes asíncronos (`CivicMessage`) con delimitación newline y framing TCP en `civicmesh.core.protocol` y `civicmesh.core.network`.
- **Gossip Membership**: Protocolo de membresía con vista parcial, heartbeats, detección de fallos por timeout y políticas de fanout configurables (`random`, `topic_biased`, `hybrid`) en `civicmesh.core.gossip`.
- **Geographic Pub/Sub**: Reenvío explícito determinista anti-flooding `should_forward()` considerando deduplicación, TTL decreciente, prioridad de canal e interés geográfico en `civicmesh.core.pubsub`.
- **Local State Aggregation**: Almacenamiento de series temporales y métricas de brecha percepción-realidad en `civicmesh.core.state`.
- **Dominio A (Delitos)**: Generador de eventos discretos Poisson $X_{c,k}(t) \sim \text{Poisson}(\lambda_{c,k} \Delta t)$ con semilla `--seed` reproducible y modelo no-lineal de sensación de inseguridad con memoria EMA, amplificación por rumores y función sigmoide (ecuaciones 1 a 3).
- **Dominio B (Calidad del Aire)**: Replay de series reales continuas de PM2.5/PM10 (Open-Meteo), interpolación espacial IDW ($p=2$) para comunas sin estación, clasificación según umbrales OMS 2021 y modelo de percepción ciudadana con retención de picos (ecuaciones 4 y 5).
- **Shared Filesystem Logger**: Persistencia de métricas en `$CIVICMESH_RUNS/<run_id>/metrics/` en formato JSONL y descubrimiento mediante `hostfile.txt`.
- **Frontend Dashboard**: Aplicación interactiva en Streamlit (`frontend/app.py`) con visualización de estado por tópico $\times$ canal, serie temporal de brecha percepción-realidad y análisis de convergencia entre réplicas.
- **Docker Compose**: Entorno contenedorizado con 3 peers, 1 publicador y frontend en red aislada.
- **Slurm Deployment**: Scripts `sbatch` para clúster DIINF mapeando 2 nodos CPU (malla) y 2 nodos GPU (publicador y frontend usando solo CPU del host).
- **CI/CD & IA Agents**: Pipeline en GitHub Actions con tests unitarios e integración, más 3 agentes de IA (Revisor de Bugs, Documentador, Revisor de PRs con Ollama local `qwen2.5-coder:7b`).
- **Test Suite**: Cobertura exhaustiva en `tests/unit/` y `tests/integration/` con `pytest`.
