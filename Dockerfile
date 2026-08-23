FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente y datos
COPY civicmesh/ /app/civicmesh/
COPY config/ /app/config/
COPY data/ /app/data/
COPY frontend/ /app/frontend/
COPY scripts/ /app/scripts/

ENV PYTHONPATH=/app
ENV CIVICMESH_RUNS=/app/runs

# Puerto por defecto para frontend
EXPOSE 8501

CMD ["python", "-m", "civicmesh.node.peer", "--id", "peer-1", "--host", "0.0.0.0", "--port", "8001"]
