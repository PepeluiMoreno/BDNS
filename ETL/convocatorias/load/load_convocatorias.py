# load_convocatorias.py
# Carga todas las convocatorias de los JSONs mensuales en la base de datos y actualiza el CSV de control.

import csv
import logging
import re
import time
from pathlib import Path
from ETL.convocatorias.load.load_convocatorias_from_json import load_convocatorias_from_json
from ETL.etl_utils import get_or_create_dir

logger = logging.getLogger("load_convocatorias")
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )

RUTA_CONTROL = get_or_create_dir("control")
RUTA_TRANSFORMED = get_or_create_dir("json", "convocatorias", "transformed")


def get_control_csv_path(year):
    return RUTA_CONTROL / f"convocatoria_{year}.csv"


def get_jsons_mensuales(year, tipo):
    return sorted(RUTA_TRANSFORMED.glob(f"convocatorias_{tipo}_{year}_??.json"))

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
  
def extraer_year_de_filename(filepath: str) -> int | None:
    """Extrae el año del nombre de archivo: convocatorias_X_YYYY_MM.json -> YYYY"""
    match = re.search(r'convocatorias_[A-Z]_(\d{4})_\d{2}\.json', Path(filepath).name)
    if match:
        return int(match.group(1))
    return None


def cargar_archivo_unico(filepath: str) -> int:
    """
    Carga un archivo transformado específico (llamado desde orchestrator).

    Args:
        filepath: Ruta completa al archivo transformado

    Returns:
        Código de salida (0=ok, 1=error)
    """
    json_path = Path(filepath)
    if not json_path.exists():
        logger.error(f"Archivo no encontrado: {filepath}")
        return 1

    # Extraer año del filename para encontrar el CSV de control
    year = extraer_year_de_filename(filepath)
    if not year:
        logger.error(f"No se pudo extraer el año del filename: {filepath}")
        return 1

    csv_path = get_control_csv_path(year)
    if not csv_path.exists():
        logger.error(f"No existe el CSV de control: {csv_path}")
        return 1

    logger.info(f"Cargando archivo único: {json_path.name}")
    completadas, pendientes = load_convocatorias_from_json(json_path, csv_path)

    if pendientes > 0:
        logger.warning(f"Completado con {pendientes} pendientes")

    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carga convocatorias desde JSONs mensuales y marca el CSV de control.")
    parser.add_argument("--year", "-y", type=int, help="Año a procesar")
    parser.add_argument("--file", type=str, help="Cargar un archivo transformado específico")
    args = parser.parse_args()

    # Modo archivo único (usado por orchestrator)
    if args.file:
        exit(cargar_archivo_unico(args.file))

    # Modo batch
    if not args.year:
        parser.error("Se requiere --year o --file")

    load_convocatorias(args.year)
