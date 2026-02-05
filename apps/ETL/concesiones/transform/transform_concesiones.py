# ETL/concesiones/transform/transform_concesiones.py
"""
TRANSFORM: Valida y prepara concesiones para carga.

Este script SOLO hace transformacion:
- Lee los CSVs de concesiones extraidos
- Valida que existan las FKs necesarias (beneficiario, convocatoria)
- Genera JSON/CSV preparado para carga directa

Dependencias:
- extract_concesiones.py debe haber extraido los CSVs
- load_beneficiarios.py debe haber cargado los beneficiarios
- load_convocatorias.py debe haber cargado las convocatorias
"""

import csv
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from bdns_core.db.session import get_session
from bdns_core.db.models import Beneficiario, Convocatoria, Instrumento
from ETL.etl_utils import get_or_create_dir

MODULO = "transform_concesiones"

# Rutas
RUTA_CONTROL = get_or_create_dir("control")
RUTA_TRANSFORMED = get_or_create_dir("json", "concesiones", "transformed")
RUTA_LOGS = get_or_create_dir("logs")
RUTA_INCIDENCIAS = get_or_create_dir("incidencias")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)


def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")


def cargar_ids_existentes(session) -> tuple[set, set, set]:
    """Carga los IDs existentes de beneficiarios, convocatorias e instrumentos."""
    beneficiarios = set(row[0] for row in session.query(Beneficiario.id).all())
    convocatorias = set(row[0] for row in session.query(Convocatoria.id).all())
    instrumentos = set(row[0] for row in session.query(Instrumento.id).all())
    return beneficiarios, convocatorias, instrumentos


def transformar_concesion(row: dict, beneficiarios: set, convocatorias: set,
                          instrumentos: set) -> tuple[dict | None, str | None]:
    """
    Transforma y valida una fila de concesion.

    Returns:
        (concesion_transformada, error_si_hay)
    """
    # Campos requeridos
    id_concesion = row.get("idConcesion")
    codigo_bdns = row.get("codigoBDNS")
    id_beneficiario = row.get("idPersona")

    if not id_concesion:
        return None, "sin_id_concesion"

    # Validar FK beneficiario
    try:
        id_beneficiario = int(id_beneficiario) if id_beneficiario else None
    except (ValueError, TypeError):
        return None, f"id_beneficiario_invalido:{id_beneficiario}"

    if id_beneficiario and id_beneficiario not in beneficiarios:
        return None, f"beneficiario_no_existe:{id_beneficiario}"

    # Validar FK convocatoria
    try:
        codigo_bdns = int(codigo_bdns) if codigo_bdns else None
    except (ValueError, TypeError):
        return None, f"codigo_bdns_invalido:{codigo_bdns}"

    if codigo_bdns and codigo_bdns not in convocatorias:
        return None, f"convocatoria_no_existe:{codigo_bdns}"

    # Validar FK instrumento (opcional)
    id_instrumento = row.get("instrumento") or row.get("idInstrumento")
    try:
        id_instrumento = int(id_instrumento) if id_instrumento else None
    except (ValueError, TypeError):
        id_instrumento = None

    if id_instrumento and id_instrumento not in instrumentos:
        id_instrumento = None  # Ignorar instrumento invalido

    # Parsear importe
    importe = row.get("importe")
    try:
        importe = float(importe) if importe else None
    except (ValueError, TypeError):
        importe = None

    # Parsear ayuda_equivalente
    ayuda_eq = row.get("ayudaEquivalente")
    try:
        ayuda_eq = float(ayuda_eq) if ayuda_eq else None
    except (ValueError, TypeError):
        ayuda_eq = None

    # Parsear fecha
    fecha = row.get("fechaConcesion")
    if fecha and len(fecha) >= 10:
        fecha = fecha[:10]  # Solo YYYY-MM-DD
    else:
        fecha = None

    # Parsear booleano tiene_proyecto
    tiene_proyecto = row.get("tieneProyecto", "").lower() in ("true", "1", "s", "si")

    return {
        "id": id_concesion,
        "codigo_bdns": codigo_bdns,
        "id_beneficiario": id_beneficiario,
        "id_instrumento": id_instrumento,
        "fecha_concesion": fecha,
        "importe": importe,
        "ayuda_equivalente": ayuda_eq,
        "url_br": row.get("urlBR"),
        "tiene_proyecto": tiene_proyecto,
    }, None


def procesar_csv(archivo: Path, beneficiarios: set, convocatorias: set,
                 instrumentos: set) -> tuple[list, list]:
    """Procesa un archivo CSV de concesiones."""
    concesiones = []
    incidencias = []

    with open(archivo, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            concesion, error = transformar_concesion(
                row, beneficiarios, convocatorias, instrumentos
            )
            if concesion:
                concesiones.append(concesion)
            elif error:
                incidencias.append({
                    "archivo": archivo.name,
                    "id_concesion": row.get("idConcesion"),
                    "error": error,
                    "row": row,
                })

    return concesiones, incidencias


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Transforma concesiones.")
    parser.add_argument("--year", type=int, required=True, help="Ejercicio a procesar")
    args = parser.parse_args()

    year = args.year
    log(f"Transformando concesiones del ejercicio {year}")

    # Cargar IDs existentes
    log("Cargando IDs de BD...")
    with get_session() as session:
        beneficiarios, convocatorias, instrumentos = cargar_ids_existentes(session)

    log(f"Cargados: {len(beneficiarios)} beneficiarios, {len(convocatorias)} convocatorias, {len(instrumentos)} instrumentos")

    # Buscar CSVs de concesiones
    pattern = f"concesiones*{year}*.csv"
    archivos = list(RUTA_CONTROL.glob(pattern))

    if not archivos:
        log(f"No se encontraron archivos con patron {pattern}")
        return 1

    log(f"Encontrados {len(archivos)} archivos")

    todas_concesiones = []
    todas_incidencias = []

    for archivo in archivos:
        log(f"Procesando {archivo.name}")
        concesiones, incidencias = procesar_csv(
            archivo, beneficiarios, convocatorias, instrumentos
        )
        todas_concesiones.extend(concesiones)
        todas_incidencias.extend(incidencias)
        log(f"  -> {len(concesiones)} validas, {len(incidencias)} incidencias")

    # Guardar JSON transformado
    out_path = RUTA_TRANSFORMED / f"concesiones_{year}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "concesiones": todas_concesiones,
            "meta": {
                "year": year,
                "total": len(todas_concesiones),
                "archivos_procesados": [str(a.name) for a in archivos],
                "fecha_transformacion": datetime.now().isoformat(),
            }
        }, f, ensure_ascii=False, indent=2)

    log(f"Guardadas {len(todas_concesiones)} concesiones -> {out_path}")

    # Guardar incidencias si hay
    if todas_incidencias:
        inc_path = RUTA_INCIDENCIAS / f"concesiones_{year}_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(inc_path, "w", encoding="utf-8") as f:
            json.dump(todas_incidencias, f, ensure_ascii=False, indent=2)
        log(f"Guardadas {len(todas_incidencias)} incidencias -> {inc_path}")

    return 0


if __name__ == "__main__":
    exit(main())
