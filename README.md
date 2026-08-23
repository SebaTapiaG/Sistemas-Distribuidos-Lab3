# CivicMesh: Framework P2P de Publish/Subscribe para Monitoreo Ciudadano Distribuido

**Asignatura:** Sistemas Distribuidos y Paralelos (USACH)  
**Semestre:** 1-2026  
**Profesor:** Miguel Cárcamo  
**Versión:** `v1.0.0-lab3`

---

## 1. Integrantes del Equipo y Roles

| Nombre | Rol Principal | Responsabilidades Evaluadas |
| :--- | :--- | :--- |
| **Sebastián Tapia** | Líder de Capa de Red / Gossip | Diseño de protocolo de membresía gossip, mantenimiento de vista parcial, detección de caídas por timeout y políticas de fanout (random, topic-biased, hybrid). |
| **Rodrigo González** | Líder de Capa Pub/Sub | Tópicos comunales, suscripciones, reenvío explícito determinista anti-flooding (`should_forward`), gestión de TTL decreciente y prioridades por canal. |
| **Juan Loyola** | Líder de Datos | Ingesta y cache de series reales continuas de PM2.5/PM10 (Open-Meteo), interpolación espacial IDW, generador estocástico Poisson para delitos y modelos de percepción (fórmulas 1 a 5). |
| **Vicente Aninat** | Líder de Analítica y Estadística | Métricas de convergencia y dispersión, experimentos de tolerancia a fallos/partición, y desarrollo del frontend de monitoreo en Streamlit. |
| **Ignacio Celis Castro** | Líder de CI/CD, Git y Agentes | Pipeline de CI/CD en GitHub Actions, contenedores Docker / Docker Compose, scripts sbatch/srun para Slurm en clúster DIINF, coordinación en Shared FS, y los 3 agentes de IA (Ollama `qwen2.5-coder:7b`). |

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
- **Canal Objetivo:** Generación estocástica por comuna $c$ y tipo de delito $k$:
  $$X_{c,k}(t) \sim \text{Poisson}(\lambda_{c,k} \cdot \Delta t), \quad R_c(t) = \sum_k X_{c,k}(t)$$
- **Canal Subjetivo (Índice de Inseguridad $P_c(t) \in [0, 1]$):**
  $$M_c(t) = \alpha M_c(t - \Delta t) + (1 - \alpha) R_c(t) \quad \text{con } M_c(0) = 0$$
  $$Z_c(t) = \beta_0 + \beta_1 M_c(t) + \beta_2 \hat{P}_c^{\text{gossip}}(t) + \varepsilon_c(t), \quad \varepsilon_c(t) \sim \mathcal{N}(0, \sigma_\varepsilon^2)$$
  $$P_c(t) = \sigma(Z_c(t)) = \frac{1}{1 + e^{-Z_c(t)}}$$
  *Parámetros base:* $\alpha = 0,8$, $\beta_0 = -1,0$, $\beta_1 = 0,4$, $\beta_2 = 0,8$, $\sigma_\varepsilon = 0,1$.

### Dominio B — Calidad del Aire (Medición vs. Percepción)
- **Canal Objetivo:** Replay determinista de series reales de PM2.5 ($\mu\text{g}/\text{m}^3$) de estaciones oficiales (Open-Meteo). Para comunas intermedias sin estación se aplica interpolación espacial **IDW (Inverse Distance Weighting)** con potencia $p = 2$:
  $$v_c(t) = \frac{\sum_{s \in S} w_s v_s(t)}{\sum_{s \in S} w_s}, \quad w_s = \frac{1}{d(c, s)^p}$$
- **Canal Subjetivo (Percepción Ciudadana $P_c(t)$ en $\mu\text{g}/\text{m}^3$ con retención de picos):**
  $$u_c(t) = \max(v_c(t), M_c(t - \Delta t))$$
  $$M_c(t) = \alpha M_c(t - \Delta t) + (1 - \alpha) u_c(t) \quad \text{con } M_c(0) = 0$$
  $$P_c(t) = v_c(t) + \gamma (M_c(t) - v_c(t)) + \delta \hat{P}_c^{\text{gossip}}(t) + \varepsilon_c(t)$$
  *Parámetros base:* $\alpha = 0,85$, $\gamma = 0,6$ (sesgo por pico retenido), $\delta = 0,3$ (arrastre por rumores), $\sigma_\varepsilon = 2,0$, clip en $[0; 500]$.
- **Contexto Regulatorio OMS 2021 (PM2.5):**
  - Buena: $\le 5\,\mu\text{g}/\text{m}^3$
  - Moderada: $\le 15\,\mu\text{g}/\text{m}^3$
  - Dañina grupos sensibles: $\le 25\,\mu\text{g}/\text{m}^3$
  - Dañina: $\le 50\,\mu\text{g}/\text{m}^3$
  - Muy Dañina: $\le 150\,\mu\text{g}/\text{m}^3$
  - Peligrosa: $> 150\,\mu\text{g}/\text{m}^3$

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

### 4.3 Ejecución de Pruebas Automatizadas
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

### 5.1 Lanzamiento del Job Slurm
```bash
sbatch scripts/slurm/run_cluster.sbatch
```

### 5.2 Acceso al Frontend mediante Túnel SSH
Si el frontend se inicia en el nodo GPU asignado (por ejemplo `gpu02`):
```bash
ssh -L 8501:gpu02:8501 <usuario>@<login.diinf.usach.cl>
```

---

## 🤖 6. Agentes de IA Integrados (Laboratorio 2 adaptados)

El repositorio incluye tres agentes autónomos que ejecutan análisis de código mediante **Ollama local** (`qwen2.5-coder:7b`):
1. **Agente Revisor de Bugs (`.github/workflows/agente-revisor-bugs.yml`):** Analiza manejo asíncrono, anti-flooding, semillas deterministas y divisiones por cero.
2. **Agente Documentador (`.github/workflows/agente-documentador.yml`):** Audita completitud del README, CHANGELOG y consistencia de fórmulas.
3. **Agente Revisor de Pull Requests (`.github/workflows/agente-revisor-pr.yml`):** Clasifica cambios en `MECANICO` vs `REQUIERE_REVISION_HUMANA` y comenta los PRs.
