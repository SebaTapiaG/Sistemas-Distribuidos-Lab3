.PHONY: help install test test-unit test-int lint run-compose run-frontend download-data clean

PYTHON ?= python

help:
	@echo "CivicMesh - Comandos disponibles:"
	@echo "  make install         Instala dependencias en el entorno actual"
	@echo "  make test            Ejecuta suite completa de pruebas (unitarias e integracion)"
	@echo "  make test-unit       Ejecuta solo pruebas unitarias"
	@echo "  make test-int        Ejecuta solo pruebas de integracion"
	@echo "  make lint            Revisa estilo y sintaxis con flake8"
	@echo "  make download-data   Descarga o actualiza el cache de calidad del aire desde Open-Meteo"
	@echo "  make run-frontend    Inicia el dashboard Streamlit"
	@echo "  make run-compose     Construye y levanta la malla multi-nodo con Docker Compose"
	@echo "  make clean           Limpia archivos temporales y caches de pytest/pycache"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest -v

test-unit:
	$(PYTHON) -m pytest tests/unit/ -v

test-int:
	$(PYTHON) -m pytest tests/integration/ -v

lint:
	flake8 civicmesh tests --count --select=E9,F63,F7,F82 --show-source --statistics

download-data:
	$(PYTHON) scripts/download_air_data.py

run-frontend:
	streamlit run frontend/app.py

run-compose:
	docker compose up --build

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf .coverage
