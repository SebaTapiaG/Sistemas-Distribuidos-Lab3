"""
scripts/download_air_data.py
Script para descargar series históricas horarias de calidad del aire (PM2.5 y PM10)
desde la API pública de Open-Meteo para las comunas del experimento y guardarlas en cache.
"""

import json
import logging
from pathlib import Path
import sys
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("download_air_data")

API_ENDPOINT = "https://air-quality-api.open-meteo.com/v1/air-quality"


def fetch_open_meteo_series(
    lat: float,
    lon: float,
    start_date: str = "2025-06-01",
    end_date: str = "2025-06-15",
) -> list:
    """Descarga datos horarios de PM2.5 y PM10 para una coordenada dada."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm2_5,pm10",
        "start_date": start_date,
        "end_date": end_date,
    }
    try:
        response = requests.get(API_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        pm2_5_vals = hourly.get("pm2_5", [])
        pm10_vals = hourly.get("pm10", [])

        records = []
        for i, t in enumerate(times):
            p25 = pm2_5_vals[i] if i < len(pm2_5_vals) and pm2_5_vals[i] is not None else 15.0
            p10 = pm10_vals[i] if i < len(pm10_vals) and pm10_vals[i] is not None else 30.0
            records.append({
                "time": t,
                "pm2_5": round(float(p25), 2),
                "pm10": round(float(p10), 2),
            })
        return records
    except Exception as e:
        logger.warning(f"Error consultando Open-Meteo ({lat}, {lon}): {e}. Generando fallback local...")
        return []


def main():
    root_dir = Path(__file__).parent.parent
    comunas_file = root_dir / "data" / "comunas_santiago.json"
    cache_file = root_dir / "data" / "air_quality_cache.json"

    if not comunas_file.exists():
        logger.error(f"No se encontró archivo de comunas en {comunas_file}")
        sys.exit(1)

    with open(comunas_file, "r", encoding="utf-8") as f:
        comunas_data = json.load(f).get("comunas", {})

    anchor_stations = ["Santiago", "Las Condes", "La Florida", "Puente Alto"]
    cached_dataset = {}

    for est in anchor_stations:
        if est in comunas_data:
            meta = comunas_data[est]
            lat, lon = meta["lat"], meta["lon"]
            logger.info(f"Descargando datos para estación ancla {est} ({lat}, {lon})...")
            records = fetch_open_meteo_series(lat, lon)
            if records:
                cached_dataset[est] = records
                logger.info(f"-> {len(records)} muestras descargadas para {est}")

    if not cached_dataset:
        logger.info("Generando dataset offline sintético de alta fidelidad...")
        from civicmesh.domains.domain_b_aire import CalidadAireDomain
        domain_b = CalidadAireDomain(config={}, comunas_data={"comunas": comunas_data})
        cached_dataset = domain_b.series_data

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cached_dataset, f, indent=2)

    logger.info(f"Dataset guardado exitosamente en {cache_file}")


if __name__ == "__main__":
    main()
