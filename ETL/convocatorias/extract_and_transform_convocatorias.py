# extract_and_transform_convocatorias.py

import os
import json
import logging
import csv
import time
import requests
from pathlib import Path
from multiprocessing import Process, Queue
from datetime import datetime
from app.db.session import SessionLocal
from app.db.utils import normalizar, buscar_organo_id
from app.db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto, Region,
    Finalidad, Objetivo, Reglamento, Fondo
)
from ETL.etl_utils import get_or_create_dir

MODULO = "extract_and_transform_convocatorias"

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")

# Rutas
RUTA_CONTROL = get_or_create_dir("control")
RUTA_JSONS = get_or_create_dir("json", "convocatorias")
RUTA_LOGS = get_or_create_dir("logs")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)

TIPOS = ["C", "A", "L", "O"]

def enriquecer_convocatoria(session, entrada):
    """Enriquece la convocatoria con los IDs FK correspondientes a catálogos y órgano."""
    CATALOGOS = {
        "instrumentos": Instrumento,
        "tiposBeneficiarios": TipoBeneficiario,
        "sectores": SectorActividad,
        "sectoresProducto": SectorProducto,
        "regiones": Region,
        "finalidades": Finalidad,
        "objetivos": Objetivo,
        "reglamentos": Reglamento,
        "fondos": Fondo,
    }
    resultado = entrada.copy()
    # Procesar órgano (meter id dentro del dict "organo")
    organo = entrada.get("organo")
    if organo:
        nivel1 = organo.get("nivel1")
        nivel2 = organo.get("nivel2")
        nivel3 = organo.get("nivel3")
        organo_id = buscar_organo_id(session, nivel1, nivel2, nivel3)
        if organo_id:
            resultado["organo"] = dict(organo)  # Clonar para no modificar el original
            resultado["organo"]["id"] = organo_id
        else:
            resultado["organo"] = organo  # Sin id
    # Procesar otros catálogos (añadir solo los ids, sin info de control)
    for campo, modelo in CATALOGOS.items():
        valores = entrada.get(campo)
        if not valores:
            continue
        enriched = []
        for item in valores:
            norm = normalizar(item.get("descripcion")) if "descripcion" in item else None
            obj = None
            if norm:
                obj = session.query(modelo).filter_by(descripcion_norm=norm).first()
            elif item.get("id"):
                obj = session.query(modelo).get(item.get("id"))
            enriched.append({"id": obj.id} if obj else item)
        resultado[campo] = enriched
    return resultado

def procesar_mes(year, mes, tipo, csv_path):
    """Procesa un mes para un tipo, solo trata las que están en 'pending' y las guarda en un JSON mensual."""
    session = SessionLocal()
    json_out_path = RUTA_JSONS / f"convocatorias_{tipo}_{year}_{mes:02d}.json"
    processed = []
    pendientes = 0

    # Leer y actualizar CSV
    rows = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    changed = False

    for row in rows:
        # Filtrar por mes/tipo y estado pending
        if row["tipo_administracion"] != tipo or row["status"] != "pending":
            continue
        fecha_rec = row.get("fecha_recepcion", "")[:7]  # "YYYY-MM"
        if fecha_rec != f"{year}-{mes:02d}":
            continue
        # Recuperar el detalle
        codigo_bdns = row["codigo_bdns"]
        url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatorias?numConv={codigo_bdns}"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            detalle = resp.json()
            enriched = enriquecer_convocatoria(session, detalle)
            processed.append(enriched)
            row["status"] = "extracted"
            row["last_error"] = ""
            row["last_attempt"] = datetime.now().isoformat()
            changed = True
        except Exception as e:
            row["status"] = "pending"
            row["last_error"] = str(e)
            row["last_attempt"] = datetime.now().isoformat()
            row["retries"] = str(int(row.get("retries", 0)) + 1)
            pendientes += 1
            changed = True

    # Guardar JSON mensual (solo campos de datos, sin campos de control)
    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(processed, f, ensure_ascii=False, indent=2)
    log(f"[{tipo}-{year}-{mes:02d}] Guardadas {len(processed)} convocatorias en {json_out_path}")

    # Guardar de vuelta el CSV actualizado si hubo cambios
    if changed:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        log(f"[{tipo}-{year}-{mes:02d}] CSV de control actualizado: {csv_path}")

    session.close()
    return len(processed), pendientes

def main():
    import argparse
    import requests
    parser = argparse.ArgumentParser(description=f"{MODULO}: Extrae y enriquece convocatorias por año.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year
    log(f"Transformando convocatorias del ejercicio {year}")

    # Control CSV debe estar en control/convocatoria_{year}.csv
    csv_path = RUTA_CONTROL / f"convocatoria_{year}.csv"
    if not csv_path.exists():
        log(f"ERROR: No existe el CSV de control: {csv_path}", "ERROR")
        return

    # Procesar por mes/tipo (puedes hacer multiproceso aquí si quieres)
    for tipo in TIPOS:
        for mes in range(1, 13):
            procesar_mes(year, mes, tipo, csv_path)

    log(f"Proceso completado.")

if __name__ == "__main__":
    main()






