# CivicMesh: Framework P2P de Publish/Subscribe para Monitoreo Ciudadano Distribuido

**Asignatura:** Sistemas Distribuidos y Paralelos (USACH)  
**Semestre:** 1-2026  
**Profesor:** Miguel Cárcamo  
**Versión:** `v1.0.0-lab3`

---

## 1. Integrantes del Equipo y Roles

| Nombre | Rol Principal | Responsabilidades |
| :--- | :--- | :--- |
| **Sebastián Tapia** | Líder de Capa de Red / Gossip | Diseño del protocolo de membresía, mantención de la vista parcial, detección de fallos (timeouts) y políticas de fanout. |
| **Rodrigo González** | Líder de Capa Pub/Sub | Configuración de tópicos comunales, suscripciones, función `should_forward` (anti-flooding), y gestión de TTL/prioridad. |
| **Juan Loyola** | Líder de Datos | Ingesta de series de calidad del aire (Open-Meteo/SINCA), replay, interpolación IDW y generadores estocásticos (Poisson/Percepción). |
| **Vicente Aninat** | Líder de Analítica y Estadística | Análisis de métricas (convergencia/divergencia), experimentos de partición de red y desarrollo del frontend (Streamlit). |
| **Ignacio Celis Castro** | Líder de CI/CD, Git y Agentes | Orquestación en GitHub Actions, Docker Compose, scripts Slurm (`sbatch`), sistema de archivos compartido y agentes IA. |

---

## 2. Arquitectura de CivicMesh

CivicMesh implementa una infraestructura P2P desacoplada sobre Python asíncrono (`asyncio` + TCP):

```
┌─────────────────────────────────────────────────────────────┐
│             Frontend de Estadísticas (Streamlit)            │
├─────────────────────────────────────────────────────────────┤
│         Estado Agregado Local + Shared FS ($CIVICMESH_RUNS) │
├─────────────────────────────────────────────────────────────┤
│  Pub/Sub Geográfico: should_forward (TTL, Prioridad, Fanout)│
├─────────────────────────────────────────────────────────────┤
│      Membresía / Gossip (Vista Parcial, Heartbeats, Fallos) │
├──────────────────────────────┬──────────────────────────────┤
│      Dominio A (Delitos)     │ Dominio B (Calidad del Aire) │
│  Eventos discretos (Poisson) │ Series Reales (Open-Meteo)   │
│  vs Percepción Inseguridad   │ vs Percepción con Retención  │
└──────────────────────────────┴──────────────────────────────┘
```

### 2.1 Membresía Gossip
- Cada nodo mantiene una **vista parcial** de tamaño acotado (`partial_view_size = 6`).
- Emite latidos periódicos cada `heartbeat_interval_sec = 1.0s` hacia $f$ peers seleccionados.
- Si un peer no responde en `fail_timeout_sec = 3.5s`, se declara `DEAD` y se expulsa.
- **Políticas de Fanout:**
  - `random`: Muestreo uniforme aleatorio.
  - `topic_biased`: Favorece peers suscritos a comunas idénticas o vecinas en el grafo comunal.
  - `hybrid`: Compromiso con 50% de exploración aleatoria y 50% de explotación geográfica.

### 2.2 Capa Pub/Sub y Reenvío Anti-Flooding
- Cada peer evalúa la función explícita:
  $$\text{should\_forward}(\text{msg}, \text{topic}, \text{local\_view}) \to \text{bool}$$
- **Criterios de descarte:**
  1. $\text{TTL} \le 1$: Se agota el tiempo de vida (evita bucles infinitos).
  2. $\text{Deduplicación}$: Mensajes con `msg_id` ya visto en la ventana temporal son descartados.
  3. $\text{Interés geográfico}$: Para prioridad normal, solo reenvía si existen vecinos interesados o adyacentes.

---

## 3. Dominios de Aplicación y Modelos Matemáticos

### Dominio A — Delitos (Percepción vs. Realidad)
**Canal Objetivo:** Generación de eventos discretos por comuna $c$ y tipo de delito $k$ usando un proceso de Poisson:
$$X_{c,k}(t)\sim Poisson(\lambda_{c,k}\Delta t)$$
**Canal Subjetivo (Índice de Inseguridad):** Combina el ground truth local ($R_{c}(t)$), una memoria EMA ($M_{c}(t)$) y los rumores de la red ($\hat{P}_{c}^{gossip}(t)$):
$$M_{c}(t)=\alpha M_{c}(t-\Delta t)+(1-\alpha)R_{c}(t)$$
$$Z_{c}(t)=\beta_{0}+\beta_{1}M_{c}(t)+\beta_{2}\hat{P}_{c}^{gossip}(t)+\epsilon_{c}(t)$$
$$P_{c}(t)=\sigma(Z_{c}(t))$$

### Dominio B — Calidad del Aire (Medición vs. Percepción)
**Canal Objetivo:** Replay de series reales continuas. Para comunas sin estación, se extrapola usando IDW (Inverse Distance Weighting) con potencia $p$:
$$v_{c}(t)=\frac{\sum_{s\in S}w_{s}v_{s}(t)}{\sum_{s\in S}w_{s}}$$
$$w_{s}=\frac{1}{d(c,s)^{p}}$$
**Canal Subjetivo (Retención de picos):**
$$u_{c}(t)=\max(v_{c}(t),M_{c}(t-\Delta t))$$
$$M_{c}(t)=\alpha M_{c}(t-\Delta t)+(1-\alpha)u_{c}(t)$$
$$P_{c}(t)=v_{c}(t)+\gamma(M_{c}(t)-v_{c}(t))+\delta\hat{P}_{c}^{gossip}(t)+\epsilon_{c}(t)$$
*Umbrales OMS 2021 (PM2.5):* Buena <= 5, Moderada <= 15, Dañina <= 50.
---

## 4. Guía de Instalación y Ejecución

### 4.1 Requisitos Previos
- Python 3.10 o superior.
- Docker y Docker Compose (opcional para ejecución en contenedores).

### 4.2 Instalación Local
```bash
# Crear y activar entorno virtual
python -m venv venv
# Linux/macOS:
source venv/bin/activate
# Windows:
.\venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 4.3 Configuración de Entorno y Ejecución Local

# Exportar variable de entorno para la corrida local
export CIVICMESH_RUNS="./runs/local-run"
mkdir -p $CIVICMESH_RUNS/metrics $CIVICMESH_RUNS/logs

# 1. Levantar Peer Seed
python -m civicmesh.node.peer --id peer-1 --host localhost --port 8001 --topics Santiago Providencia --run-id local-run &

# 2. Levantar Peer secundario (conectado a peer-1)
python -m civicmesh.node.peer --id peer-2 --host localhost --port 8002 --topics Providencia Las_Condes --seeds localhost:8001 --run-id local-run &

# 3. Levantar Publicador
python -m civicmesh.node.publisher --id pub-1 --domain delitos --interval 2.0 --run-id local-run &

# 4. Levantar Dashboard Frontend
streamlit run frontend/app.py --server.port=8501

### 4.4 Ejecución de Pruebas Automatizadas
```bash
# Ejecutar suite completa (unitarias + integración)
pytest -v

# O mediante Makefile
make test
```

### 4.4 Despliegue con Docker Compose
Para levantar automáticamente 3 peers + 1 publicador + Frontend de métricas:
```bash
docker compose up --build
```
Una vez levantado, abre tu navegador en **http://localhost:8501** para ver el dashboard interactivo.

---

## 5. Despliegue en Clúster DIINF (Slurm)

En el clúster del DIINF, el despliegue separa estrictamente los roles sin utilizar CUDA (usando únicamente la CPU del host):

```
       [ 2 Hosts CPU ]                        [ 2 Hosts GPU ]
   (Peers Gossip + Pub/Sub)              (Publicador + Replay + UI)
              │                                      │
              └──────────────► RED (TCP) ◄───────────┘
                                  │
                                  ▼
           [ Shared Filesystem: $CIVICMESH_RUNS/<run_id>/ ]
           ├── hostfile.txt (descubrimiento de endpoints)
           ├── config.yaml  (parámetros y semillas)
           ├── metrics/     (JSONL de convergencia y brecha)
           └── logs/        (stdout/stderr)
```

### 5.1 Convención del Shared Filesystem

En el clúster Slurm, la variable $CIVICMESH_RUNS se resuelve dinámicamente en el almacenamiento compartido NFS/Shared FS para permitir la coordinación entre nodos CPU y GPU

```bash
export CIVICMESH_RUNS="$HOME/proyecto/$SLURM_JOB_ID"
```

### 5.2 Lanzamiento del Job Slurm
```bash
sbatch scripts/slurm/run_cluster.sbatch
```

### 5.3 Acceso al Frontend mediante Túnel SSH
Si el frontend se inicia en el nodo GPU asignado (por ejemplo `gpu02`):
```bash
ssh -L 8501:gpu02:8501 <usuario>@<login.diinf.usach.cl>
```
Luego navega a http://localhost:8501 en tu máquina local.
---

## 🤖 6. Agentes de IA Integrados (Laboratorio 2 adaptados)

El repositorio incluye tres agentes autónomos que ejecutan análisis de código mediante **Ollama local** (`qwen2.5-coder:7b`):
1. **Agente Revisor de Bugs (`.github/workflows/agente-revisor-bugs.yml`):** Analiza manejo asíncrono, anti-flooding, semillas deterministas y divisiones por cero.
2. **Agente Documentador (`.github/workflows/agente-documentador.yml`):** Audita completitud del README, CHANGELOG y consistencia de fórmulas.
3. **Agente Revisor de Pull Requests (`.github/workflows/agente-revisor-pr.yml`):** Clasifica cambios en `MECANICO` vs `REQUIERE_REVISION_HUMANA` y comenta los PRs.
