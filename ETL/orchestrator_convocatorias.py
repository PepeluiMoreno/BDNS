#!/usr/bin/env python3
"""
Orquestador paralelo ETL convocatorias con watchdog.

Ejecuta Extract, Transform, Load en paralelo:
- Extract genera raw_convocatorias_*.json
- Watcher detecta raw files -> lanza Transform
- Watcher detecta transformed files -> lanza Load

Uso:
    python -m ETL.orchestrator_convocatorias --year 2024 --workers 10
"""

import threading
import subprocess
import logging
import time
import sys
from pathlib import Path
from datetime import datetime

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    print("ERROR: Instala watchdog con: pip install watchdog")
    sys.exit(1)

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent
RUTA_RAW = BASE_DIR / "ETL" / "json" / "convocatorias" / "raw"
RUTA_TRANSFORMED = BASE_DIR / "ETL" / "json" / "convocatorias" / "transformed"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("orchestrator")


class RawFileHandler(FileSystemEventHandler):
    """Detecta raw_*.json creados y lanza transform."""

    def __init__(self, year: int, csv_path: Path):
        self.year = year
        self.csv_path = csv_path
        self.processing = set()
        self.lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith('.json'):
            return

        filename = Path(event.src_path).name

        # Verificar que es del año correcto
        if f"_{self.year}_" not in filename:
            return

        with self.lock:
            if filename in self.processing:
                return
            self.processing.add(filename)

        logger.info(f"[RAW] Detectado: {filename}")

        # Lanzar transform en thread separado
        thread = threading.Thread(
            target=self._run_transform,
            args=(event.src_path, filename),
            daemon=True
        )
        thread.start()

    def _run_transform(self, filepath: str, filename: str):
        """Ejecuta transform para un archivo específico."""
        try:
            logger.info(f"[TRANSFORM] Iniciando: {filename}")
            result = subprocess.run(
                [
                    sys.executable, "-m",
                    "ETL.convocatorias.transform.transform_convocatorias",
                    "--file", filepath
                ],
                capture_output=True,
                text=True,
                cwd=str(BASE_DIR)
            )
            if result.returncode == 0:
                logger.info(f"[TRANSFORM] Completado: {filename}")
            else:
                logger.error(f"[TRANSFORM] Error en {filename}: {result.stderr}")
        except Exception as e:
            logger.error(f"[TRANSFORM] Excepción en {filename}: {e}")


class TransformedFileHandler(FileSystemEventHandler):
    """Detecta convocatorias_*.json transformados y lanza load."""

    def __init__(self, year: int, csv_path: Path):
        self.year = year
        self.csv_path = csv_path
        self.processing = set()
        self.lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith('.json'):
            return

        filename = Path(event.src_path).name

        # Verificar que es del año correcto y no es raw
        if f"_{self.year}_" not in filename:
            return
        if filename.startswith("raw_"):
            return

        with self.lock:
            if filename in self.processing:
                return
            self.processing.add(filename)

        logger.info(f"[TRANSFORMED] Detectado: {filename}")

        # Lanzar load en thread separado
        thread = threading.Thread(
            target=self._run_load,
            args=(event.src_path, filename),
            daemon=True
        )
        thread.start()

    def _run_load(self, filepath: str, filename: str):
        """Ejecuta load para un archivo específico."""
        try:
            logger.info(f"[LOAD] Iniciando: {filename}")
            result = subprocess.run(
                [
                    sys.executable, "-m",
                    "ETL.convocatorias.load.load_convocatorias",
                    "--file", filepath
                ],
                capture_output=True,
                text=True,
                cwd=str(BASE_DIR)
            )
            if result.returncode == 0:
                logger.info(f"[LOAD] Completado: {filename}")
            else:
                logger.error(f"[LOAD] Error en {filename}: {result.stderr}")
        except Exception as e:
            logger.error(f"[LOAD] Excepción en {filename}: {e}")


def run_extract(year: int, workers: int):
    """Ejecuta el script de extracción."""
    logger.info(f"[EXTRACT] Iniciando extracción año {year} con {workers} workers...")
    result = subprocess.run(
        [
            sys.executable, "-m",
            "ETL.convocatorias.extract.extract_convocatorias",
            "--year", str(year),
            "--workers", str(workers)
        ],
        cwd=str(BASE_DIR)
    )
    logger.info(f"[EXTRACT] Finalizado con código: {result.returncode}")
    return result.returncode


def main(year: int, workers: int = 10, wait_after: int = 60):
    """
    Orquesta el pipeline paralelo E-T-L.

    Args:
        year: Año a procesar
        workers: Workers paralelos para extract
        wait_after: Segundos a esperar después de extract para transform/load pendientes
    """
    csv_path = BASE_DIR / "ETL" / "control" / f"convocatoria_{year}.csv"

    logger.info("=" * 60)
    logger.info(f"PIPELINE PARALELO CONVOCATORIAS {year}")
    logger.info("=" * 60)
    logger.info(f"CSV control: {csv_path}")
    logger.info(f"Ruta raw: {RUTA_RAW}")
    logger.info(f"Ruta transformed: {RUTA_TRANSFORMED}")

    # Crear directorios
    RUTA_RAW.mkdir(parents=True, exist_ok=True)
    RUTA_TRANSFORMED.mkdir(parents=True, exist_ok=True)

    # Verificar CSV de control
    if not csv_path.exists():
        logger.error(f"No existe CSV de control: {csv_path}")
        logger.info("Ejecuta primero: python -m ETL.convocatorias.extract_control_csv --year " + str(year))
        return 1

    # Configurar watchers
    logger.info("Iniciando watchers...")

    observer_raw = Observer()
    observer_raw.schedule(
        RawFileHandler(year, csv_path),
        str(RUTA_RAW),
        recursive=False
    )
    observer_raw.start()
    logger.info(f"[WATCHER] Monitoreando raw: {RUTA_RAW}")

    observer_trans = Observer()
    observer_trans.schedule(
        TransformedFileHandler(year, csv_path),
        str(RUTA_TRANSFORMED),
        recursive=False
    )
    observer_trans.start()
    logger.info(f"[WATCHER] Monitoreando transformed: {RUTA_TRANSFORMED}")

    # Ejecutar extract (bloquea hasta terminar)
    start_time = datetime.now()
    extract_code = run_extract(year, workers)

    # Esperar a que terminen transform/load pendientes
    logger.info(f"Extract terminado. Esperando {wait_after}s para transform/load pendientes...")
    time.sleep(wait_after)

    # Detener watchers
    observer_raw.stop()
    observer_trans.stop()
    observer_raw.join()
    observer_trans.join()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETADO en {elapsed:.1f}s ({elapsed/60:.1f} min)")
    logger.info("=" * 60)

    return extract_code


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Orquestador paralelo ETL convocatorias"
    )
    parser.add_argument("--year", type=int, required=True,
                        help="Año a procesar")
    parser.add_argument("--workers", type=int, default=10,
                        help="Workers paralelos para extract (default: 10)")
    parser.add_argument("--wait", type=int, default=60,
                        help="Segundos a esperar tras extract (default: 60)")
    args = parser.parse_args()

    sys.exit(main(args.year, args.workers, args.wait))
