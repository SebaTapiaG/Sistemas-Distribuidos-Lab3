#!/bin/bash
# ==============================================================================
# CivicMesh: Despliegue Distribuido en Clúster DIINF (Slurm)
# Laboratorio 3 - Sistemas Distribuidos y Paralelos (USACH)
# ==============================================================================
#SBATCH --job-name=civicmesh_cluster
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm_%j.out
#SBATCH --error=logs/slurm_%j.err

# Configuración de entorno y directorios
export RUN_ID=${SLURM_JOB_ID:-"diinf_$(date +%s)"}
export CIVICMESH_RUNS="${CIVICMESH_RUNS:-$PWD/runs}"
RUN_DIR="$CIVICMESH_RUNS/$RUN_ID"

mkdir -p "$RUN_DIR/metrics"
mkdir -p "$RUN_DIR/logs"
mkdir -p "logs"

LOG_FILE="$RUN_DIR/logs/cluster_execution.log"

echo "======================================================================" | tee $LOG_FILE
echo "             CIVICMESH: REGISTRO DE EJECUCIÓN EN CLÚSTER DIINF        " | tee -a $LOG_FILE
echo "======================================================================" | tee -a $LOG_FILE
echo "FECHA / HORA INICIO : $(date '+%Y-%m-%d %H:%M:%S')" | tee -a $LOG_FILE
echo "JOB ID              : $SLURM_JOB_ID" | tee -a $LOG_FILE
echo "DIRECTORIO CORRIDA  : $RUN_DIR" | tee -a $LOG_FILE
echo "SHARED FS METRICS   : $RUN_DIR/metrics" | tee -a $LOG_FILE
echo "----------------------------------------------------------------------" | tee -a $LOG_FILE

# Obtener lista de nodos asignados por Slurm
NODELIST=($(scontrol show hostnames $SLURM_NODELIST))
echo "Nodos asignados por Slurm: ${NODELIST[@]}" | tee -a $LOG_FILE

NODE_CPU1=${NODELIST[0]}
NODE_CPU2=${NODELIST[1]:-$NODE_CPU1}
NODE_GPU1=${NODELIST[2]:-$NODE_CPU1}
NODE_GPU2=${NODELIST[3]:-$NODE_CPU2}

echo "Mapeo de Roles Slurm (Sin uso de CUDA, solo CPU del host en nodos GPU):" | tee -a $LOG_FILE
echo " [Nodo CPU 1] Malla Peers (Santiago, Providencia, Las Condes) -> $NODE_CPU1" | tee -a $LOG_FILE
echo " [Nodo CPU 2] Malla Peers (Nunoa, La Florida, Puente Alto)     -> $NODE_CPU2" | tee -a $LOG_FILE
echo " [Nodo GPU 1] Publicador de Dominio (Poisson / Replay)        -> $NODE_GPU1" | tee -a $LOG_FILE
echo " [Nodo GPU 2] Frontend Streamlit (Dashboard)                  -> $NODE_GPU2" | tee -a $LOG_FILE
echo "======================================================================" | tee -a $LOG_FILE

# Copiar dataset de aire cacheado al directorio de ejecución
cp data/comunas_santiago.json "$RUN_DIR/" 2>/dev/null || true
cp config/*.yaml "$RUN_DIR/" 2>/dev/null || true

# 1. Iniciar Peer 1 (Semilla) en Nodo CPU 1
echo ">>> [1/4] Levantando Peer Semilla 1 en $NODE_CPU1:8001..." | tee -a $LOG_FILE
srun --nodes=1 --nodelist=$NODE_CPU1 --ntasks=1 \
    python -m civicmesh.node.peer \
    --id peer-cpu-1 \
    --host $NODE_CPU1 \
    --port 8001 \
    --topics Santiago Providencia Las_Condes \
    --run-id $RUN_ID > "$RUN_DIR/logs/peer1.log" 2>&1 &

sleep 2

# 2. Iniciar Peer 2 en Nodo CPU 2
echo ">>> [2/4] Levantando Peer 2 en $NODE_CPU2:8002 conectado a Peer 1..." | tee -a $LOG_FILE
srun --nodes=1 --nodelist=$NODE_CPU2 --ntasks=1 \
    python -m civicmesh.node.peer \
    --id peer-cpu-2 \
    --host $NODE_CPU2 \
    --port 8002 \
    --topics Nunoa La_Florida Puente_Alto \
    --seeds "$NODE_CPU1:8001" \
    --run-id $RUN_ID > "$RUN_DIR/logs/peer2.log" 2>&1 &

sleep 2

# 3. Iniciar Publicador de Dominio en Nodo GPU 1 (usando CPU del host)
echo ">>> [3/4] Levantando Publicador en $NODE_GPU1..." | tee -a $LOG_FILE
srun --nodes=1 --nodelist=$NODE_GPU1 --ntasks=1 \
    python -m civicmesh.node.publisher \
    --id pub-gpu-1 \
    --domain delitos \
    --interval 2.0 \
    --run-id $RUN_ID > "$RUN_DIR/logs/publisher.log" 2>&1 &

# 4. Iniciar Frontend de Monitoreo en Nodo GPU 2
echo ">>> [4/4] Levantando Frontend Streamlit en $NODE_GPU2:8501..." | tee -a $LOG_FILE
srun --nodes=1 --nodelist=$NODE_GPU2 --ntasks=1 \
    streamlit run frontend/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 > "$RUN_DIR/logs/frontend.log" 2>&1 &

echo "" | tee -a $LOG_FILE
echo "======================================================================" | tee -a $LOG_FILE
echo "CivicMesh ejecutándose activamente en el Clúster." | tee -a $LOG_FILE
echo "Túnel SSH para visualizar dashboard:" | tee -a $LOG_FILE
echo "  ssh -L 8501:$NODE_GPU2:8501 <usuario>@login.diinf.usach.cl" | tee -a $LOG_FILE
echo "======================================================================" | tee -a $LOG_FILE

# Esperar finalización de subprocesos
wait
