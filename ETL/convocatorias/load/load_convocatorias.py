# load_convocatorias.py
# Carga todas las convocatorias de los JSONs mensuales en la base de datos y actualiza el CSV de control.

import csv
import logging
import time
from pathlib import Path
from ETL.convocatorias.load_convocatorias_from_json import load_convocatorias_from_json
from ETL.etl_utils import get_or_create_dir

logger = logging.getLogger("load_convocatorias")
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )

def get_control_csv_path(year):
    return get_or_create_dir("ETL", "control") / f"convocatoria_{year}.csv"

def get_jsons_mensuales(year, tipo):
    base_dir = get_or_create_dir("ETL", "json", "convocatorias")
    return sorted(base_dir.glob(f"convocatorias_{tipo}_{year}_??.json"))

def load_convocatorias(year):
    csv_path = get_control_csv_path(year)
    if not csv_path.exists():
        logger.error(f"No existe el CSV de control: {csv_path}")
        return

    start = time.time()
    total_completadas = 0
    total_pendientes = 0

    for tipo in ["C", "A", "L", "O"]:
        jsons_mes = get_jsons_mensuales(year, tipo)
        for json_mes in jsons_mes:
            logger.info(f"Procesando {json_mes.name} ...")
            completadas, pendientes = load_convocatorias_from_json(json_mes, csv_path)
            total_completadas += completadas
            total_pendientes += pendientes

    elapsed = time.time() - start
    logger.info(f"Convocatorias completadas: {total_completadas}")
    logger.info(f"Convocatorias pendientes: {total_pendientes}")
    logger.info(f"Tiempo total: {elapsed:.1f} segundos")

    print(f"Convocatorias completadas: {total_completadas}")
    print(f"Convocatorias pendientes: {total_pendientes}")
    print(f"Tiempo total: {elapsed:.1f} segundos")
  
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carga convocatorias desde JSONs mensuales y marca el CSV de control.")
    parser.add_argument("--year", "-y", type=int, required=True)
    args = parser.parse_args()
    load_convocatorias(args.year)
