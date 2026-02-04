# ETL/beneficiarios/transform/transform_beneficiarios.py
"""
TRANSFORM: Extrae y transforma beneficiarios desde CSVs de concesiones.

Este script SOLO hace transformacion:
- Lee los CSVs de concesiones extraidos
- Extrae NIF y nombre de cada beneficiario
- Deduce forma juridica desde el NIF
- Normaliza nombres y detecta pseudonimos
- Guarda JSON listo para cargar

La carga a BD se hace en load_beneficiarios.py
"""

import csv
import json
import re
import logging
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from app.db.utils import normalizar
from ETL.etl_utils import get_or_create_dir

MODULO = "transform_beneficiarios"

# Rutas
RUTA_CONTROL = get_or_create_dir("control")
RUTA_TRANSFORMED = get_or_create_dir("json", "beneficiarios", "transformed")
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


def extraer_nif_y_nombre(descripcion: str) -> tuple[str, str]:
    """Extrae NIF y nombre desde el campo beneficiario del CSV."""
    if not descripcion:
        return "", ""
    partes = descripcion.strip().split(maxsplit=1)
    if not partes:
        return "", ""
    if len(partes) == 1:
        return partes[0].strip(":-"), ""
    return partes[0].strip(":-"), partes[1].strip()


def deducir_forma_juridica(nif: str) -> str | None:
    """
    Deduce la forma juridica desde la primera letra del NIF.

    - PF: Persona fisica (NIF con asteriscos)
    - A-Z: Primera letra del CIF/NIF
    """
    if not nif:
        return None
    if "*" in nif:
        return "PF"
    match = re.match(r"([A-Z])", nif, re.I)
    if match:
        return match.group(1).upper()
    return None


def limpiar_nombre(nombre: str) -> str:
    """Limpia caracteres no alfabeticos del nombre."""
    nombre = re.sub(r'^[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+', '', nombre)
    nombre = re.sub(r'[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+$', '', nombre)
    nombre = re.sub(r'[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ .-]', '', nombre)
    return nombre.strip()


def procesar_csv_concesiones(archivo_csv: Path, beneficiarios: dict):
    """
    Procesa un CSV de concesiones y extrae beneficiarios.

    Args:
        archivo_csv: Path al archivo CSV
        beneficiarios: Dict donde acumular beneficiarios por id_persona
    """
    with open(archivo_csv, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_persona = row.get("idPersona")
            beneficiario_raw = row.get("beneficiario") or ""

            if not id_persona or not beneficiario_raw.strip():
                continue

            nif, nombre = extraer_nif_y_nombre(beneficiario_raw)
            nombre_limpio = limpiar_nombre(nombre)

            if not nombre_limpio:
                continue

            nombre_norm = normalizar(nombre_limpio)
            forma_juridica = deducir_forma_juridica(nif)

            beneficiarios[id_persona].append({
                "nif": nif,
                "nombre_original": nombre,
                "nombre": nombre_limpio,
                "nombre_norm": nombre_norm,
                "forma_juridica": forma_juridica,
            })


def consolidar_beneficiarios(beneficiarios: dict) -> tuple[list, list]:
    """
    Consolida beneficiarios y detecta pseudonimos.

    Para cada id_persona, el nombre principal es el mas largo/normalizado.
    Los demas nombres se convierten en pseudonimos.

    Returns:
        (lista_beneficiarios, lista_pseudonimos)
    """
    beneficiarios_out = []
    pseudonimos_out = []

    for id_persona, variantes in beneficiarios.items():
        # Ordenar por longitud de nombre_norm (mayor primero)
        variantes = sorted(variantes, key=lambda x: (-len(x["nombre_norm"]), x["nombre_norm"]))

        principal = variantes[0]

        beneficiarios_out.append({
            "id": int(id_persona),
            "nif": principal["nif"],
            "nombre": principal["nombre"],
            "nombre_norm": principal["nombre_norm"],
            "forma_juridica": principal["forma_juridica"],
        })

        # Detectar pseudonimos (nombres alternativos)
        vistos = {principal["nombre_norm"]}
        for v in variantes[1:]:
            if v["nombre_norm"] and v["nombre_norm"] not in vistos:
                pseudonimos_out.append({
                    "beneficiario_id": int(id_persona),
                    "pseudonimo": v["nombre"],
                    "pseudonimo_norm": v["nombre_norm"],
                })
                vistos.add(v["nombre_norm"])

    return beneficiarios_out, pseudonimos_out


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Transforma beneficiarios desde CSVs.")
    parser.add_argument("--year", type=int, required=True, help="Ejercicio a procesar")
    args = parser.parse_args()

    year = args.year
    log(f"Transformando beneficiarios del ejercicio {year}")

    # Buscar CSVs de concesiones del año
    pattern = f"concesiones*{year}*.csv"
    archivos = list(RUTA_CONTROL.glob(pattern))

    if not archivos:
        log(f"No se encontraron archivos con patron {pattern} en {RUTA_CONTROL}")
        return 1

    log(f"Encontrados {len(archivos)} archivos de concesiones")

    # Acumular beneficiarios de todos los archivos
    beneficiarios = defaultdict(list)
    for archivo in archivos:
        log(f"Procesando {archivo.name}")
        procesar_csv_concesiones(archivo, beneficiarios)

    log(f"Extraidos {len(beneficiarios)} beneficiarios unicos")

    # Consolidar y detectar pseudonimos
    beneficiarios_out, pseudonimos_out = consolidar_beneficiarios(beneficiarios)

    # Guardar JSON transformado
    out_path = RUTA_TRANSFORMED / f"beneficiarios_{year}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "beneficiarios": beneficiarios_out,
            "pseudonimos": pseudonimos_out,
            "meta": {
                "year": year,
                "total_beneficiarios": len(beneficiarios_out),
                "total_pseudonimos": len(pseudonimos_out),
                "archivos_procesados": [str(a.name) for a in archivos],
                "fecha_transformacion": datetime.now().isoformat(),
            }
        }, f, ensure_ascii=False, indent=2)

    log(f"Guardado: {len(beneficiarios_out)} beneficiarios, {len(pseudonimos_out)} pseudonimos -> {out_path}")
    return 0


if __name__ == "__main__":
    exit(main())
