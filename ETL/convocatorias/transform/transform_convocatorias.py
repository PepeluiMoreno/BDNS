# ETL/convocatorias/transform/transform_convocatorias.py
"""
TRANSFORM: Enriquece convocatorias raw con IDs de FK.

Este script SOLO hace transformacion:
- Lee los JSONs raw extraidos por extract_convocatorias.py
- Resuelve las FKs de catalogos (organo, instrumentos, tipos_beneficiarios, etc.)
- Guarda JSONs transformados listos para cargar

Usa la funcion enriquecer_convocatoria() para resolver FKs contra la BD.
"""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from app.db.session import get_session
from app.db.utils import normalizar, buscar_organo_id
from app.db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto,
    Region, Finalidad, Objetivo, Reglamento, Fondo
)
from ETL.etl_utils import get_or_create_dir

MODULO = "transform_convocatorias"
TIPOS = ["C", "A", "L", "O"]

# Rutas
RUTA_RAW = get_or_create_dir("json", "convocatorias", "raw")
RUTA_TRANSFORMED = get_or_create_dir("json", "convocatorias", "transformed")
RUTA_LOGS = get_or_create_dir("logs")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)

# Mapeo de campos JSON a modelos de catalogo
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


def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")


def enriquecer_convocatoria(session, entrada: dict) -> dict:
    """
    Enriquece una convocatoria con los IDs FK correspondientes a catalogos y organo.

    Args:
        session: Sesion de BD
        entrada: Dict con datos raw de la convocatoria

    Returns:
        Dict enriquecido con IDs de FK
    """
    resultado = entrada.copy()

    # Procesar organo: buscar ID en BD y agregarlo al dict
    organo = entrada.get("organo")
    if organo:
        nivel1 = organo.get("nivel1")
        nivel2 = organo.get("nivel2")
        nivel3 = organo.get("nivel3")
        organo_id = buscar_organo_id(session, nivel1, nivel2, nivel3)
        if organo_id:
            resultado["organo"] = dict(organo)
            resultado["organo"]["id"] = organo_id
        else:
            resultado["organo"] = organo  # Sin id, se manejara en load

    # Procesar otros catalogos: resolver IDs por descripcion_norm
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

            if obj:
                enriched.append({"id": obj.id})
            else:
                # Mantener item original si no se encuentra
                enriched.append(item)

        resultado[campo] = enriched

    return resultado


def procesar_archivo(raw_path: Path, out_path: Path) -> tuple[int, int]:
    """
    Transforma un archivo JSON raw específico.

    Args:
        raw_path: Ruta al archivo raw de entrada
        out_path: Ruta al archivo transformado de salida

    Returns:
        Tupla (transformadas, errores)
    """
    if not raw_path.exists():
        log(f"Archivo no encontrado: {raw_path}", "WARNING")
        return 0, 0

    with open(raw_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    if not raw_data:
        return 0, 0

    transformed = []
    errors = 0

    with get_session() as session:
        for conv in raw_data:
            try:
                enriched = enriquecer_convocatoria(session, conv)
                transformed.append(enriched)
            except Exception as e:
                log(f"Error transformando convocatoria {conv.get('codigoBDNS', '?')}: {e}", "ERROR")
                errors += 1

    # Guardar transformado
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)

    log(f"Transformadas {len(transformed)} convocatorias -> {out_path.name}")
    return len(transformed), errors


def procesar_mes(year: int, mes: int, tipo: str) -> tuple[int, int]:
    """Transforma un archivo JSON raw de un mes/tipo."""
    raw_path = RUTA_RAW / f"raw_convocatorias_{tipo}_{year}_{mes:02d}.json"
    out_path = RUTA_TRANSFORMED / f"convocatorias_{tipo}_{year}_{mes:02d}.json"
    return procesar_archivo(raw_path, out_path)


def procesar_archivo_unico(filepath: str) -> int:
    """
    Procesa un archivo raw específico (llamado desde orchestrator).

    Args:
        filepath: Ruta completa al archivo raw

    Returns:
        Código de salida (0=ok, 1=error)
    """
    raw_path = Path(filepath)
    if not raw_path.exists():
        log(f"ERROR: Archivo no encontrado: {filepath}", "ERROR")
        return 1

    # Generar nombre de salida: raw_convocatorias_X_YYYY_MM.json -> convocatorias_X_YYYY_MM.json
    filename = raw_path.name
    if filename.startswith("raw_"):
        out_filename = filename[4:]  # Quitar "raw_"
    else:
        out_filename = filename

    out_path = RUTA_TRANSFORMED / out_filename

    transformed, errors = procesar_archivo(raw_path, out_path)

    if errors > 0:
        log(f"Completado con {errors} errores")
        return 1

    return 0


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Transforma convocatorias raw.")
    parser.add_argument("--year", type=int, help="Ejercicio a procesar")
    parser.add_argument("--tipo", type=str, default=None, help="Tipo administracion (C/A/L/O)")
    parser.add_argument("--file", type=str, help="Procesar un archivo raw específico")
    args = parser.parse_args()

    # Modo archivo único (usado por orchestrator)
    if args.file:
        return procesar_archivo_unico(args.file)

    # Modo batch (todos los archivos de un año)
    if not args.year:
        parser.error("Se requiere --year o --file")

    year = args.year
    tipos = [args.tipo] if args.tipo else TIPOS

    log(f"Transformando convocatorias del ejercicio {year}")

    total_transformed = 0
    total_errors = 0

    for tipo in tipos:
        for mes in range(1, 13):
            transformed, errors = procesar_mes(year, mes, tipo)
            total_transformed += transformed
            total_errors += errors

    log(f"Proceso completado: {total_transformed} transformadas, {total_errors} errores")
    return 0


if __name__ == "__main__":
    exit(main())
