# ETL/convocatorias/extract/extract_convocatorias.py
"""
EXTRACT: Extrae convocatorias desde API BDNS y guarda JSON raw.

Este script SOLO hace extraccion:
- Lee el CSV de control para saber que convocatorias procesar
- Llama a la API de BDNS para cada convocatoria pendiente
- Guarda los JSONs raw (sin transformar) por mes
- Actualiza el CSV de control con el estado

El paso de transformacion (resolver FKs) se hace en transform_convocatorias.py
"""

import csv
import json
import logging
import requests
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from ETL.etl_utils import get_or_create_dir

MODULO = "extract_convocatorias"
TIPOS = ["C", "A", "L", "O"]  # Central, Autonomica, Local, Otros

# Rutas
RUTA_CONTROL = get_or_create_dir("control")
RUTA_RAW = get_or_create_dir("json", "convocatorias", "raw")
RUTA_LOGS = get_or_create_dir("logs")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)


def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")


def extraer_convocatoria(codigo_bdns: str, timeout: int = 60) -> dict | None:
    """Llama a la API de BDNS y devuelve el JSON raw."""
    url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatorias?numConv={codigo_bdns}"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log(f"Error extrayendo {codigo_bdns}: {e}", "ERROR")
        return None


def procesar_mes(year: int, mes: int, tipo: str, csv_path: Path, max_workers: int = 10):
    """Procesa un mes para un tipo, extrae solo las pendientes."""
    json_out_path = RUTA_RAW / f"raw_convocatorias_{tipo}_{year}_{mes:02d}.json"
    extracted = []
    pendientes = 0
    errores = 0

    # Leer CSV de control
    rows = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Filtrar pendientes del mes/tipo
    to_extract = []
    for i, row in enumerate(rows):
        if row["tipo_administracion"] != tipo or row["status"] != "pending":
            continue
        fecha_rec = row.get("fecha_recepcion", "")[:7]
        if fecha_rec != f"{year}-{mes:02d}":
            continue
        to_extract.append((i, row["codigo_bdns"]))

    if not to_extract:
        log(f"[{tipo}-{year}-{mes:02d}] Sin convocatorias pendientes")
        return 0, 0, 0

    log(f"[{tipo}-{year}-{mes:02d}] Extrayendo {len(to_extract)} convocatorias...")

    # Extraccion paralela
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(extraer_convocatoria, codigo): (idx, codigo)
            for idx, codigo in to_extract
        }
        for future in as_completed(future_to_idx):
            idx, codigo = future_to_idx[future]
            try:
                data = future.result()
                results[idx] = (codigo, data)
            except Exception as e:
                results[idx] = (codigo, None)
                log(f"Error en {codigo}: {e}", "ERROR")

    # Actualizar CSV y recoger JSONs
    changed = False
    for idx, (codigo, data) in results.items():
        row = rows[idx]
        if data:
            extracted.append(data)
            row["status"] = "extracted"
            row["last_error"] = ""
            changed = True
        else:
            row["retries"] = str(int(row.get("retries", 0)) + 1)
            row["last_error"] = "extraction_failed"
            pendientes += 1
            errores += 1
            changed = True
        row["last_attempt"] = datetime.now().isoformat()

    # Guardar JSON raw
    if extracted:
        # Cargar existentes si hay
        existing = []
        if json_out_path.exists():
            with open(json_out_path, encoding="utf-8") as f:
                existing = json.load(f)
        existing.extend(extracted)
        with open(json_out_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        log(f"[{tipo}-{year}-{mes:02d}] Guardadas {len(extracted)} convocatorias raw en {json_out_path}")

    # Guardar CSV actualizado
    if changed:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return len(extracted), pendientes, errores


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Extrae convocatorias desde API BDNS.")
    parser.add_argument("--year", type=int, required=True, help="Ejercicio a procesar")
    parser.add_argument("--workers", type=int, default=10, help="Workers paralelos (default: 10)")
    parser.add_argument("--tipo", type=str, default=None, help="Tipo administracion (C/A/L/O)")
    args = parser.parse_args()

    year = args.year
    tipos = [args.tipo] if args.tipo else TIPOS

    log(f"Extrayendo convocatorias del ejercicio {year}")

    csv_path = RUTA_CONTROL / f"convocatoria_{year}.csv"
    if not csv_path.exists():
        log(f"ERROR: No existe CSV de control: {csv_path}", "ERROR")
        return 1

    total_extracted = 0
    total_pending = 0
    total_errors = 0

    for tipo in tipos:
        for mes in range(1, 13):
            extracted, pending, errors = procesar_mes(
                year, mes, tipo, csv_path, max_workers=args.workers
            )
            total_extracted += extracted
            total_pending += pending
            total_errors += errors

    log(f"Proceso completado: {total_extracted} extraidas, {total_pending} pendientes, {total_errors} errores")
    return 0


if __name__ == "__main__":
    exit(main())
